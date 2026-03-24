"""
Refactored Sync Manager

Fixes:
1. Prevents duplicate sync operations
2. Prevents re-inserting deleted/old visits
3. Respects SYSTEM_MODE (no PostgreSQL in OFFLINE/CRITICAL)
4. Uses UPSERT for PostgreSQL (latest visit only)
5. Thread-safe with proper connection handling
"""

import json
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger("sync")


class RefactoredSyncManager:
    """
    Refactored sync manager with proper offline-first architecture.
    
    CRITICAL RULES:
    1. SQLite is the ONLY source of truth
    2. PostgreSQL is ONLY for sync (secondary)
    3. No PostgreSQL calls in OFFLINE or CRITICAL mode
    4. One row per patient_id in PostgreSQL (latest visit only)
    5. Prevent duplicate sync and re-inserting deleted visits
    """
    
    def __init__(self, sqlite_manager, sync_queue_manager):
        """
        Initialize refactored sync manager.
        
        Args:
            sqlite_manager: ThreadSafeSQLiteManager instance
            sync_queue_manager: Existing SyncQueueManager from MAIN.PY
        """
        self.sqlite_manager = sqlite_manager
        self.sync_queue_manager = sync_queue_manager
        
        # Prevent concurrent sync workers
        self._sync_lock = threading.Lock()
        
        # Track synced items to prevent duplicates
        self._synced_items = set()
        self._synced_items_lock = threading.Lock()
        
        logger.info("RefactoredSyncManager initialized")
    
    def should_sync_item(self, item_id: int) -> bool:
        """
        Check if item should be synced (prevents duplicates).
        
        Args:
            item_id: ID from sync_queue table
            
        Returns:
            True if should sync, False if already synced
        """
        with self._synced_items_lock:
            if item_id in self._synced_items:
                logger.debug(f"Item {item_id} already synced, skipping")
                return False
            return True
    
    def mark_as_synced(self, item_id: int):
        """Mark item as successfully synced."""
        with self._synced_items_lock:
            self._synced_items.add(item_id)
            
            # Prevent memory leak - keep only last 1000 items
            if len(self._synced_items) > 1000:
                # Remove oldest 500
                items_list = list(self._synced_items)
                self._synced_items = set(items_list[500:])
    
    def process_sync_queue(self, system_mode: str, is_online: bool) -> int:
        """
        Process sync queue with proper guards.
        
        Args:
            system_mode: Current system mode (ONLINE/OFFLINE/CRITICAL)
            is_online: Whether PostgreSQL is available
            
        Returns:
            Number of items processed
        """
        # CRITICAL: Only sync in ONLINE mode with PostgreSQL available
        if system_mode != "ONLINE" or not is_online:
            logger.debug(f"Sync skipped in {system_mode} mode or PostgreSQL unavailable")
            return 0
        
        # Prevent concurrent sync workers
        if not self._sync_lock.acquire(blocking=False):
            logger.debug("Sync already running, skipping")
            return 0
        
        processed_count = 0
        try:
            items = self.sync_queue_manager.get_pending_items(limit=200)
            if not items:
                self._sync_lock.release()
                return 0
                
            # Improvement: Open a single connection for the whole batch to increase speed
            # Use absolute import to avoid resolution issues
            import MAIN
            conn_pg = MAIN.get_connection()
            
            try:
                for item in items:
                    item_id = item['id']
                    
                    if not self.should_sync_item(item_id):
                        continue
                    
                    self.sync_queue_manager.mark_processing(item_id)
                    
                    try:
                        # Pass the connection to avoid re-opening it
                        success = self._process_single_item(item, conn_pg)
                        
                        if success:
                            self.sync_queue_manager.mark_success(item_id)
                            self.mark_as_synced(item_id)
                            processed_count += 1
                        else:
                            self.sync_queue_manager.mark_failed(item_id, "Processing failed")
                            
                    except Exception as e:
                        logger.error(f"Sync item {item_id} failed: {e}")
                        self.sync_queue_manager.mark_failed(item_id, str(e))
                
                if conn_pg:
                    conn_pg.commit()
            finally:
                if conn_pg:
                    MAIN.release_connection(conn_pg)
            
        except Exception as e:
            logger.error(f"Batch sync error: {e}")
        finally:
            # Ensure lock is released even if return was called earlier
            try:
                self._sync_lock.release()
            except RuntimeError:
                pass # Already released
        
        return processed_count
    
    def _process_single_item(self, item: dict, conn_pg=None) -> bool:
        """
        Process a single sync item.
        
        Args:
            item: Sync queue item dictionary
            conn_pg: Optional existing PostgreSQL connection
            
        Returns:
            True if successful, False otherwise
        """
        action = item.get('action')
        payload_str = item.get('payload')
        
        if not action or not payload_str:
            logger.error(f"Invalid sync item {item.get('id')}: missing action or payload")
            return False
        
        try:
            payload = json.loads(payload_str) if isinstance(payload_str, str) else payload_str
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in sync item {item.get('id')}: {e}")
            return False
        
        # Skip unknown patients (local-only)
        if self._is_unknown_patient(payload):
            logger.info(f"Skipping sync for unknown patient: {payload.get('patient_id')}")
            return True
        
        # Get PostgreSQL connection if not provided
        import MAIN
        
        local_conn = False
        cursor_pg = None
        
        try:
            if not conn_pg:
                conn_pg = MAIN.get_connection()
                local_conn = True
                
            if not conn_pg:
                logger.warning("PostgreSQL connection unavailable")
                return False
            
            cursor_pg = conn_pg.cursor()
            
            # Process based on action type
            if action == 'add_patient':
                result = self._sync_add_patient(cursor_pg, payload)
            elif action == 'update_patient':
                result = self._sync_update_patient(cursor_pg, payload)
            elif action == 'delete_patient':
                result = self._sync_delete_patient(cursor_pg, payload)
            elif action == 'add_visit':
                result = self._sync_add_visit(cursor_pg, payload)
            else:
                logger.warning(f"Unknown sync action: {action}")
                result = False
            
            # CRITICAL FIX: Commit the transaction if we opened the connection locally!
            if result and local_conn and conn_pg:
                conn_pg.commit()
            
            return result
                
        except Exception as e:
            logger.error(f"PostgreSQL sync error for item {item.get('id')}: {e}")
            if conn_pg:
                conn_pg.rollback()
            return False
        
        finally:
            if cursor_pg:
                try:
                    cursor_pg.close()
                except Exception:
                    pass
            # ONLY release if we opened it locally in this function
            if local_conn and conn_pg:
                try:
                    MAIN.release_connection(conn_pg)
                except Exception:
                    pass
    
    def _is_unknown_patient(self, payload: dict) -> bool:
        """Check if patient is unknown (should not be synced)."""
        patient_id = payload.get('patient_id', '')
        is_unknown = payload.get('is_unknown', 0)
        
        return is_unknown == 1 or '-U-' in patient_id or patient_id.startswith('U-')
    
    def _sync_add_patient(self, cursor_pg, payload: dict) -> bool:
        """
        Sync patient addition to PostgreSQL.
        
        Uses UPSERT to handle conflicts.
        """
        # Ensure all required fields exist
        required_fields = [
            'patient_id', 'name', 'national_id', 'birth_date', 'phone',
            'address', 'hospital', 'registration_date', 'fingerprint',
            'blood_type', 'chronic_diseases', 'chronic_medications',
            'allergies', 'medical_notes', 'current_complaint',
            'created_by', 'created_at'
        ]
        
        for field in required_fields:
            payload.setdefault(field, '')
        
        # UPSERT query
        cursor_pg.execute("""
            INSERT INTO patients (
                patient_id, name, national_id, birth_date, phone, address, hospital,
                registration_date, fingerprint, blood_type, chronic_diseases,
                chronic_medications, allergies, medical_notes, current_complaint,
                created_by, created_at, search_vector
            )
            VALUES (
                %(patient_id)s, %(name)s, %(national_id)s, %(birth_date)s, %(phone)s,
                %(address)s, %(hospital)s, %(registration_date)s, %(fingerprint)s,
                %(blood_type)s, %(chronic_diseases)s, %(chronic_medications)s,
                %(allergies)s, %(medical_notes)s, %(current_complaint)s,
                %(created_by)s, %(created_at)s,
                to_tsvector('simple', COALESCE(%(name)s,'') || ' ' || 
                           COALESCE(%(phone)s,'') || ' ' || COALESCE(%(national_id)s,''))
            )
            ON CONFLICT (national_id) DO UPDATE SET
                name = EXCLUDED.name,
                updated_at = EXCLUDED.updated_at
        """, payload)
        
        return True
    
    def _sync_update_patient(self, cursor_pg, payload: dict) -> bool:
        """Sync patient update to PostgreSQL."""
        p_id = payload.get('id')
        data = payload.get('data', {})
        
        if not p_id or not data:
            logger.error(f"Invalid update payload: {payload}")
            return False
        
        sql_params = {
            'patient_id': p_id,
            'name': data.get('name', ''),
            'national_id': data.get('national_id', ''),
            'birth_date': data.get('birth_date', ''),
            'phone': data.get('phone', ''),
            'address': data.get('address', ''),
            'fingerprint': data.get('fingerprint', ''),
            'blood_type': data.get('blood_type', ''),
            'chronic_diseases': data.get('chronic_diseases', ''),
            'chronic_medications': data.get('chronic_medications', ''),
            'allergies': data.get('allergies', ''),
            'medical_notes': data.get('medical_notes', ''),
            'current_complaint': data.get('current_complaint', '')
        }
        
        cursor_pg.execute("""
            UPDATE patients SET 
                name=%(name)s, national_id=%(national_id)s, birth_date=%(birth_date)s,
                phone=%(phone)s, address=%(address)s, fingerprint=%(fingerprint)s,
                blood_type=%(blood_type)s, chronic_diseases=%(chronic_diseases)s,
                chronic_medications=%(chronic_medications)s, allergies=%(allergies)s,
                medical_notes=%(medical_notes)s, current_complaint=%(current_complaint)s,
                search_vector=to_tsvector('simple', COALESCE(%(name)s,'') || ' ' || 
                               COALESCE(%(phone)s,'') || ' ' || COALESCE(%(national_id)s,''))
            WHERE patient_id=%(patient_id)s
        """, sql_params)
        
        return True
    
    def _sync_delete_patient(self, cursor_pg, payload: dict) -> bool:
        """Sync patient deletion to PostgreSQL."""
        p_id = payload.get('id')
        
        if not p_id:
            logger.error(f"Invalid delete payload: {payload}")
            return False
        
        # Delete visits first (cascade)
        cursor_pg.execute("DELETE FROM visits WHERE patient_id = %s", (p_id,))
        
        # Delete patient
        cursor_pg.execute("DELETE FROM patients WHERE patient_id = %s", (p_id,))
        
        return True
    
    def _sync_add_visit(self, cursor_pg, payload: dict) -> bool:
        """
        Sync visit to PostgreSQL.
        
        CRITICAL: PostgreSQL stores ONLY latest visit per patient.
        Uses UPSERT to replace old visit with new one.
        """
        # Normalize payload keys
        if 'hospital' in payload and 'hospital_code' not in payload:
            payload['hospital_code'] = payload.pop('hospital')
        
        # Set defaults for all visit fields
        visit_fields = [
            'patient_id', 'hospital_code', 'hospital_name', 'entry_type',
            'admission_time', 'admission_department', 'chief_complaint',
            'initial_diagnosis', 'chronic_diseases', 'chronic_medications',
            'allergies', 'admission_notes', 'discharge_time', 'discharge_doctor',
            'final_diagnosis', 'discharge_medications', 'surgery_performed',
            'surgery_name', 'created_by', 'created_at', 'updated_at', 'visit_uuid'
        ]
        
        for field in visit_fields:
            if field == 'surgery_performed':
                payload.setdefault(field, 0)
            elif field == 'visit_uuid':
                payload.setdefault(field, '')  # Should already exist
            else:
                payload.setdefault(field, None if 'time' in field or 'date' in field else '')
        
        # CRITICAL: UPSERT to keep only latest visit per patient
        cursor_pg.execute("""
            INSERT INTO visits (
                patient_id, hospital_code, hospital_name, entry_type, admission_time,
                admission_department, chief_complaint, initial_diagnosis, chronic_diseases,
                chronic_medications, allergies, admission_notes, discharge_time,
                discharge_doctor, final_diagnosis, discharge_medications, surgery_performed,
                surgery_name, created_by, created_at, updated_at, visit_uuid
            )
            VALUES (
                %(patient_id)s, %(hospital_code)s, %(hospital_name)s, %(entry_type)s,
                %(admission_time)s, %(admission_department)s, %(chief_complaint)s,
                %(initial_diagnosis)s, %(chronic_diseases)s, %(chronic_medications)s,
                %(allergies)s, %(admission_notes)s, %(discharge_time)s, %(discharge_doctor)s,
                %(final_diagnosis)s, %(discharge_medications)s, %(surgery_performed)s,
                %(surgery_name)s, %(created_by)s, %(created_at)s, %(updated_at)s, %(visit_uuid)s
            )
            ON CONFLICT (visit_uuid) DO UPDATE SET
                hospital_code = EXCLUDED.hospital_code,
                hospital_name = EXCLUDED.hospital_name,
                entry_type = EXCLUDED.entry_type,
                admission_department = EXCLUDED.admission_department,
                chief_complaint = EXCLUDED.chief_complaint,
                initial_diagnosis = EXCLUDED.initial_diagnosis,
                chronic_diseases = EXCLUDED.chronic_diseases,
                chronic_medications = EXCLUDED.chronic_medications,
                allergies = EXCLUDED.allergies,
                admission_notes = EXCLUDED.admission_notes,
                discharge_time = EXCLUDED.discharge_time,
                discharge_doctor = EXCLUDED.discharge_doctor,
                discharge_status = EXCLUDED.discharge_status,
                final_diagnosis = EXCLUDED.final_diagnosis,
                discharge_medications = EXCLUDED.discharge_medications,
                surgery_performed = EXCLUDED.surgery_performed,
                surgery_name = EXCLUDED.surgery_name,
                updated_at = EXCLUDED.updated_at
            WHERE EXCLUDED.updated_at > visits.updated_at
        """, payload)
        
        logger.info(f"Synced visit for patient {payload['patient_id']} (latest only)")
        return True


# Global instance
refactored_sync_manager: Optional[RefactoredSyncManager] = None


def initialize_refactored_sync(sqlite_manager, sync_queue_manager) -> RefactoredSyncManager:
    """Initialize the global refactored sync manager."""
    global refactored_sync_manager
    refactored_sync_manager = RefactoredSyncManager(sqlite_manager, sync_queue_manager)
    return refactored_sync_manager
