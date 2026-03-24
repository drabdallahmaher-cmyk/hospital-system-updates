"""
Centralized Write Queue Manager

ALL database write operations (INSERT/UPDATE/DELETE) MUST go through this queue.
This ensures:
1. Single write path - no scattered writes
2. Guaranteed ordering and atomicity
3. Thread-safe operations
4. No race conditions or database locked errors
"""

import queue
import threading
import time
import json
import logging
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional, Callable, List, Union

logger = logging.getLogger("system")


class WriteOperation(Enum):
    """Types of write operations."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    UPSERT = "upsert"


class WriteRequest:
    """Represents a single write request in the queue."""
    
    def __init__(self, operation: WriteOperation, table: str, data: Dict[str, Any], 
                 where_clause: Optional[str] = None, where_params: Optional[tuple] = None,
                 priority: int = 0, callback: Optional[Callable] = None):
        """
        Create a write request.
        
        Args:
            operation: Type of operation (INSERT/UPDATE/DELETE/UPSERT)
            table: Target table name
            data: Dictionary of column names to values
            where_clause: WHERE clause for UPDATE/DELETE (SQL with ? placeholders)
            where_params: Parameters for WHERE clause
            priority: Higher priority = processed first (default 0)
            callback: Optional callback function(result) when complete
        """
        self.operation = operation
        self.table = table
        self.data = data
        self.where_clause = where_clause
        self.where_params = where_params or ()
        self.priority = priority
        self.callback = callback
        self.timestamp = datetime.now()
        self.retries = 0 # Track retries
        self.id = f"{threading.current_thread().name}-{time.time()}"
    
    def __repr__(self):
        return f"WriteRequest({self.operation.value}, {self.table}, priority={self.priority}, retries={getattr(self, 'retries', 0)})"

    def __lt__(self, other):
        """Tie-breaker for PriorityQueue based on timestamp."""
        if not isinstance(other, WriteRequest):
            return NotImplemented
        return self.timestamp < other.timestamp


class WriteQueueManager:
    """
    Centralized manager for all database write operations.
    
    CRITICAL RULES:
    1. ALL writes MUST go through this queue - NO direct writes allowed
    2. Queue processes one write at a time (serialized)
    3. Higher priority items are processed first
    4. Supports callbacks for async completion notification
    """
    
    def __init__(self, sqlite_manager):
        """
        Initialize write queue manager.
        
        Args:
            sqlite_manager: ThreadSafeSQLiteManager instance
        """
        self.sqlite_manager = sqlite_manager
        
        # Priority queue for write requests
        self._queue = queue.PriorityQueue()
        
        # Worker thread
        self._worker_thread: Optional[threading.Thread] = None
        self._shutdown_flag = threading.Event()
        
        # Lock to prevent concurrent processing
        self._processing_lock = threading.Lock()
        
        # Statistics
        self._processed_count = 0
        self._error_count = 0
        self._retry_count = 0
        
        # Reliability Settings
        self.MAX_RETRIES = 3
        self.RETRY_DELAY = 1.0 # Base delay for backoff
        
        # Link back to manager for redirect support
        self.sqlite_manager.write_queue = self
        
        logger.info("WriteQueueManager initialized with Production Hardening")
    
    def start_worker(self):
        """Start the background worker thread."""
        thread = self._worker_thread
        if thread is None or not thread.is_alive():
            new_thread = threading.Thread(
                target=self._worker_loop,
                name="WriteQueueWorker",
                daemon=False
            )
            self._worker_thread = new_thread
            new_thread.start()
            logger.info("WriteQueueManager worker started")

    def is_running(self) -> bool:
        """Check if worker thread is alive."""
        thread = self._worker_thread
        return thread is not None and thread.is_alive()
    
    def stop_worker(self, timeout: float = 5.0):
        """
        Stop the worker thread gracefully.
        
        Args:
            timeout: Maximum time to wait for queue to drain
        """
        logger.info("Stopping WriteQueueManager worker...")
        self._shutdown_flag.set()
        
        thread = self._worker_thread
        if thread and thread.is_alive():
            thread.join(timeout=timeout)
            if thread.is_alive():
                logger.warning("WriteQueueManager worker did not shut down gracefully")
        
        self._worker_thread = None
        logger.info(f"WriteQueueManager stopped. Processed: {self._processed_count}, Errors: {self._error_count}")
    
    def enqueue_write(self, request: WriteRequest) -> bool:
        """
        Add a write request to the queue.
        
        Args:
            request: WriteRequest object
            
        Returns:
            True if successfully queued, False otherwise
        """
        try:
            # Use negative priority so higher priority = processed first
            self._queue.put((-request.priority, request))
            logger.debug(f"Enqueued: {request}")
            return True
        except Exception as e:
            logger.error(f"Failed to enqueue write request: {e}")
            return False
    
    def _worker_loop(self):
        """Main worker loop - processes write requests with retry logic."""
        while not self._shutdown_flag.is_set():
            try:
                try:
                    priority, request = self._queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                
                # Process the request with retry handling
                with self._processing_lock:
                    if not hasattr(request, 'retries'):
                        request.retries = 0
                        
                    try:
                        result = self._execute_write(request)
                        
                        if request.callback:
                            cb = request.callback
                            if callable(cb):
                                try:
                                    cb(result)
                                except Exception as e:
                                    logger.error(f"Callback error: {e}")
                        
                        self._processed_count += 1
                        
                    except Exception as e:
                        logger.error(f"Write execution error (Attempt {request.retries + 1}): {e}")
                        
                        if request.retries < self.MAX_RETRIES:
                            request.retries += 1
                            self._retry_count += 1
                            # Re-enqueue with exponential backoff (simulated by sleep here for simplicity, 
                            # or could use a delayed queue if available)
                            time.sleep(self.RETRY_DELAY * (2 ** (request.retries - 1)))
                            self._queue.put((priority, request))
                            logger.info(f"Re-queued request for retry: {request}")
                        else:
                            # Move to Dead Letter Queue
                            self._move_to_dead_letter(request, str(e))
                            self._error_count += 1
                            
                            if request.callback:
                                cb = request.callback
                                if callable(cb):
                                    try:
                                        cb(None, e)
                                    except Exception:
                                        pass
                
                self._queue.task_done()
                
            except Exception as e:
                logger.error(f"Worker loop fatal error: {e}")
                time.sleep(1.0)

    def _move_to_dead_letter(self, request: WriteRequest, error_msg: str):
        """Store failed operation in the dead_letter_queue table."""
        try:
            logger.critical(f"Write failed permanently. Moving to Dead Letter Queue: {request} | Error: {error_msg}")
            
            payload = {
                'data': request.data,
                'where': request.where_clause,
                'params': list(request.where_params) if request.where_params else []
            }
            
            # Using internal cursor bypass since this is a recovery action
            with self.sqlite_manager.cursor(commit=True, from_write_queue=True) as (cursor, conn):
                cursor.execute("""
                    INSERT INTO dead_letter_queue (timestamp, operation, table_name, payload, error, retries)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    datetime.now().isoformat(),
                    request.operation.value,
                    request.table,
                    json.dumps(payload),
                    error_msg,
                    request.retries
                ))
        except Exception as dle:
            logger.error(f"Failed to record dead letter: {dle}")
    
    def _execute_write(self, request: WriteRequest) -> Optional[Any]:
        """
        Execute a single write operation.
        """
        logger.debug(f"Executing write (authorized): {request}")
        
        # ALWAYS set from_write_queue=True and authorize the connection
        with self.sqlite_manager.cursor(commit=True, from_write_queue=True) as (cursor, conn):
            # NEW: Explicit authorization for commit
            if hasattr(conn, 'authorize'):
                conn.authorize()
                
            if request.operation == WriteOperation.INSERT:
                return self._execute_insert(cursor, request)
            
            elif request.operation == WriteOperation.UPDATE:
                return self._execute_update(cursor, request)
            
            elif request.operation == WriteOperation.DELETE:
                return self._execute_delete(cursor, request)
            
            elif request.operation == WriteOperation.UPSERT:
                return self._execute_upsert(cursor, request)
            
            else:
                raise ValueError(f"Unknown operation: {request.operation}")
    
    def _execute_insert(self, cursor, request: WriteRequest) -> Optional[int]:
        """Execute INSERT operation."""
        columns = ', '.join(request.data.keys())
        placeholders = ', '.join(['?' for _ in request.data])
        values = tuple(request.data.values())
        
        query = f"INSERT INTO {request.table} ({columns}) VALUES ({placeholders})"
        cursor.execute(query, values)
        return cursor.lastrowid
    
    def _execute_update(self, cursor, request: WriteRequest) -> int:
        """Execute UPDATE operation."""
        set_clause = ', '.join([f"{col} = ?" for col in request.data.keys()])
        values = list(request.data.values())
        
        query = f"UPDATE {request.table} SET {set_clause}"
        
        if request.where_clause:
            query += f" WHERE {request.where_clause}"
            values.extend(request.where_params)
        
        cursor.execute(query, tuple(values))
        return cursor.rowcount
    
    def _execute_delete(self, cursor, request: WriteRequest) -> int:
        """Execute DELETE operation."""
        query = f"DELETE FROM {request.table}"
        params = []
        
        if request.where_clause:
            query += f" WHERE {request.where_clause}"
            params.extend(request.where_params)
        
        cursor.execute(query, tuple(params))
        return cursor.rowcount
    
    def _execute_upsert(self, cursor, request: WriteRequest) -> Optional[int]:
        """
        Execute UPSERT operation (INSERT OR REPLACE).
        
        For SQLite: Uses INSERT OR REPLACE
        """
        # Build conflict resolution based on primary key or unique constraint
        columns = ', '.join(request.data.keys())
        placeholders = ', '.join(['?' for _ in request.data])
        values = tuple(request.data.values())
        
        # Determine unique constraint (usually first column or patient_id)
        unique_col = 'patient_id' if 'patient_id' in request.data else list(request.data.keys())[0]
        
        query = f"""
            INSERT OR REPLACE INTO {request.table} ({columns}) 
            VALUES ({placeholders})
        """
        cursor.execute(query, values)
        return cursor.lastrowid
    
    def flush_queue(self, timeout: float = 10.0) -> bool:
        """
        Wait for all queued writes to complete.
        
        Args:
            timeout: Maximum time to wait
            
        Returns:
            True if queue was flushed successfully
        """
        try:
            self._queue.join()
            return True
        except Exception as e:
            logger.error(f"Error flushing queue: {e}")
            return False
    
    def get_stats(self) -> Dict[str, int]:
        """Get queue statistics."""
        return {
            'queue_size': self._queue.qsize(),
            'processed': self._processed_count,
            'errors': self._error_count
        }


# Convenience functions for common operations

def enqueue_patient_add(sqlite_manager, write_queue: WriteQueueManager, patient_data: Dict[str, Any], 
                       callback: Optional[Callable] = None):
    """Add a patient through the write queue."""
    request = WriteRequest(
        operation=WriteOperation.UPSERT,
        table='patients',
        data=patient_data,
        priority=10,  # High priority for patient creation
        callback=callback
    )
    return write_queue.enqueue_write(request)


def enqueue_patient_update(sqlite_manager, write_queue: WriteQueueManager, patient_id: str,
                          update_data: Dict[str, Any], callback: Optional[Callable] = None):
    """Update a patient through the write queue."""
    request = WriteRequest(
        operation=WriteOperation.UPDATE,
        table='patients',
        data=update_data,
        where_clause='patient_id = ?',
        where_params=(patient_id,),
        priority=5,  # Medium priority for updates
        callback=callback
    )
    return write_queue.enqueue_write(request)


def enqueue_visit_add(sqlite_manager, write_queue: WriteQueueManager, visit_data: Dict[str, Any],
                     callback: Optional[Callable] = None):
    """Add a visit through the write queue."""
    request = WriteRequest(
        operation=WriteOperation.INSERT,
        table='visits',
        data=visit_data,
        priority=8,  # High priority for visits
        callback=callback
    )
    return write_queue.enqueue_write(request)


# Global instance
write_queue_manager: Optional[WriteQueueManager] = None


def initialize_write_queue(sqlite_manager) -> WriteQueueManager:
    """Initialize the global write queue manager."""
    global write_queue_manager
    write_queue_manager = WriteQueueManager(sqlite_manager)
    write_queue_manager.start_worker()
    return write_queue_manager
