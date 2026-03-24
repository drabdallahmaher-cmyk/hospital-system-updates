import sys
import os
import psycopg2
import psycopg2.pool
import psycopg2.extras
import sqlite3
import threading
import csv
import json
import logging
import time
import socket
import uuid
from datetime import datetime, timedelta, date
from PySide6.QtWidgets import *
from PySide6.QtCore import *
from PySide6.QtGui import *
from PySide6.QtWidgets import QCompleter, QMessageBox
from PySide6.QtCore import QStringListModel
import hashlib
import bcrypt
from collections import OrderedDict

# ==========================================
# Enhanced Features Import
# ==========================================
from ENHANCED_FEATURES_COMPLETE import (
    SettingsDialog,
    NationalIDParser,
    EnhancedStatusBar,
    ConflictResolver
)

# ==========================================
# Refactored Database Modules Import
# ==========================================
from database_manager import (
    initialize_sqlite_manager,
    get_thread_connection,
    return_thread_connection,
    ThreadSafeSQLiteManager
)

from write_queue import (
    initialize_write_queue,
    WriteQueueManager,
    WriteRequest,
    WriteOperation,
    enqueue_patient_add,
    enqueue_patient_update,
    enqueue_visit_add
)

from error_handler import (
    DatabaseErrorHandler,
    critical_section
)

from sync_manager_refactored import (
    initialize_refactored_sync,
    RefactoredSyncManager
)

# ==========================================
# Production Mode Configuration
# ==========================================
PRODUCTION = True  # Set to False for development/debug mode
DEBUG_PRINTS = not PRODUCTION


# FIXED: HARDENING PATCH - Add safe wrapper for database reads
def db_read(operation_func, *args):
    """
    Safe wrapper for database read operations.
    Enforces read-only pattern and prevents accidental writes.
    
    Usage:
        def get_patient(cursor, conn, patient_id):
            cursor.execute("SELECT * FROM patients WHERE patient_id=?", (patient_id,))
            return cursor.fetchone()
        
        result = db_read(get_patient, patient_id)
    """
    return DatabaseErrorHandler.safe_execute_db_operation(
        operation_func, "sqlite", *args
    )


# Custom JSON Encoder to handle datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        return super().default(obj)


def safe_json_dumps(data, **kwargs):
    """Safely convert data to JSON string, handling datetime objects"""
    return json.dumps(data, cls=DateTimeEncoder, **kwargs)

# ==========================================
# System State Management
# ==========================================
class SystemState:
    """
    Centralized state management for the application.
    Enforces consistency across threads and simplifies state tracking.
    """
    def __init__(self):
        self.mode = "OFFLINE"
        self.health = {
            "db_online": False,
            "sqlite_ok": False,
            "sync_ok": False,
            "last_check": None
        }
        self.running = True
        self.db_status = "INITIALIZING"
        self.environment = "production"
        self.system_ready = False
        self.active_threads = []
        self.pg_keepalive_running = False

    def is_online(self) -> bool:
        return self.mode == "ONLINE"

    def shutdown(self):
        self.running = False
        self.db_status = "SHUTTING_DOWN"

system_state = SystemState()

# System State Management
# (ALLOW_DIRECT_DB_WRITE removed - enforced by SafeCursor)

# ==========================================
# Paths and Logging Configuration
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")
BACKUP_DIR = os.path.join(BASE_DIR, "backup")

for d in [DATA_DIR, LOG_DIR, BACKUP_DIR]:
    if not os.path.exists(d):
        os.makedirs(d)

# Configure logging to split into separate files
def setup_logging():
    # إعداد نظام التسجيل المتقدم مع loggers منفصلة
    system_logger = logging.getLogger("system")
    sync_logger = logging.getLogger("sync")
    error_logger = logging.getLogger("error")

    if not system_logger.handlers:
        # FIX #7: Use RotatingFileHandler for automatic log rotation
        from logging.handlers import RotatingFileHandler
        
        # 10MB max per file, keep 5 backup files (50MB total per log type)
        system_handler = RotatingFileHandler(
            os.path.join(LOG_DIR, "system.log"), 
            encoding="utf-8",
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        sync_handler = RotatingFileHandler(
            os.path.join(LOG_DIR, "sync.log"),
            encoding="utf-8", 
            maxBytes=10*1024*1024,
            backupCount=5
        )
        error_handler = RotatingFileHandler(
            os.path.join(LOG_DIR, "errors.log"),
            encoding="utf-8",
            maxBytes=10*1024*1024,
            backupCount=5
        )

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        system_handler.setFormatter(formatter)
        sync_handler.setFormatter(formatter)
        error_handler.setFormatter(formatter)

        system_logger.addHandler(system_handler)
        system_logger.addHandler(error_handler)
        sync_logger.addHandler(sync_handler)
        error_logger.addHandler(error_handler)

    return system_logger, sync_logger, error_logger

system_logger, sync_logger, error_logger = setup_logging()

# note: do not override the logging module; use named loggers directly

# ==========================================
# PostgreSQL & SQLite Configuration
# ==========================================
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

def create_default_config():
    """إنشاء ملف config.json إفتراضي مع قيم placeholder للمستخدم لملأها"""
    default_config = {
        "db_host": "localhost",
        "db_name": "hospital_db", 
        "user": "postgres",
        "password": "your_password_here",
        "port": "5432",
        "sslmode": "prefer",
        "hospital_code": "MCH",
        "device_id": "DEVICE_001"
    }
    
    try:
        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=4, ensure_ascii=False)
        system_logger.info("Created default config.json with placeholder values")
        return default_config
    except Exception as e:
        error_logger.error(f"Failed to create default config.json: {e}")
        return None

def load_config():
    """تحميل إعدادات قاعدة البيانات من ملف خارجي مع إمكانية إنشاء ملف إفتراضي"""
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # التأكد من وجود الحقول الأساسية
                required = ["db_host", "db_name", "user", "password", "port", "sslmode"]
                if all(k in config for k in required):
                    # FIX #3: Validate password is not placeholder
                    if config.get('password') == 'your_password_here':
                        raise RuntimeError("Invalid PostgreSQL configuration: placeholder password. Please update config.json.")
                    
                    return config
                else:
                    error_logger.critical("Missing required fields in config.json")
                    return None
        except Exception as e:
            error_logger.error(f"Error loading config.json: {e}")
            return None
    
    # في حالة عدم وجود الملف، إنشاء ملف إفتراضي وإظهار رسالة للمستخدم
    system_logger.info("Config.json not found, creating default configuration")
    default_config = create_default_config()
    if default_config:
        # إظهار رسالة GUI للمستخدم بدل إغلاق البرنامج
        if QApplication.instance():
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Information)
            msg.setWindowTitle("إعداد الإتصال")
            msg.setText("🔧 تم إنشاء ملف الإعدادات الإفتراضي")
            msg.setInformativeText("برجاء ملء بيانات اتصال قاعدة البيانات في ملف config.json ثم إعادة تشغيل البرنامج")
            msg.setDetailedText("الموقع: config.json\n\nالمعلومات المطلوبة:\n- db_host: عنوان خادم قاعدة البيانات\n- db_name: اسم قاعدة البيانات\n- user: اسم المستخدم\n- password: كلمة المرور\n- port: المنفذ (عادة 5432)\n- sslmode: وضع SSL")
            msg.exec()
        else:
            system_logger.info("Config.json created with default values. Please configure database connection.")
    else:
        if QApplication.instance():
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Critical)
            msg.setWindowTitle("خطأ في الإعدادات")
            msg.setText("❌ فشل في إنشاء ملف الإعدادات")
            msg.exec()
        else:
            error_logger.error("Failed to create default configuration")
    
    return None

_DB_CONFIG = load_config()
LOCAL_DB_PATH = os.path.join(DATA_DIR, "local_cache.db")
SETTINGS_PATH = os.path.join(DATA_DIR, "settings.json")

# FIX #3: Initial state configuration
system_state.mode = "OFFLINE"
system_state.environment = "production"
system_state.system_ready = False

# FIX #9: Environment-specific settings
if system_state.environment == "dev":
    DEBUG = True
    LOG_LEVEL = "DEBUG"
elif system_state.environment == "offline":
    DEBUG = False
    LOG_LEVEL = "WARNING"
else:  # production
    DEBUG = False
    LOG_LEVEL = "INFO"

# FIXED: CRITICAL WRITE QUEUE INTEGRATION
# Initialize ThreadSafeSQLiteManager first (required by WriteQueueManager)
sqlite_manager = initialize_sqlite_manager(LOCAL_DB_PATH)
system_logger.info("✅ ThreadSafeSQLiteManager initialized")

# Initialize WriteQueueManager (worker started AFTER health check for deterministic startup)
write_queue_manager = WriteQueueManager(sqlite_manager)
# write_queue_manager.start_worker()  # MOVED to after perform_health_check() for deterministic startup

# Legacy queue system REMOVED - all writes go through WriteQueueManager

# متغير عالمي للـ Pool (يُعيّن بشكل غير متزامن)
connection_pool = None
_pool_ready = threading.Event()  # يُشير إلى أن الـ Pool جاهز
_pool_lock = threading.Lock()

def _init_pool_async():
    # إنشاء PostgreSQL Connection Pool بشكل غير متزامن - مُحسّن للأداء
    global connection_pool
    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            8,  # min connections (من 5 → 8 لأداء أفضل)
            30,  # max connections (من 20 → 30 لتحمل أثقل)
            host=_DB_CONFIG.get('db_host'),
            database=_DB_CONFIG.get('db_name'),
            user=_DB_CONFIG.get('user'),
            password=_DB_CONFIG.get('password'),
            port=_DB_CONFIG.get('port', 5432),
            sslmode=_DB_CONFIG.get('sslmode', 'prefer'),
            connect_timeout=5,  # تقليل من 10 إلى 5 ثواني
        )
        system_logger.info(f"✅ PostgreSQL Connection Pool initialized (8-30 connections, optimized)")
    except Exception as e:
        system_logger.warning(f"⚠️ PostgreSQL Connection Pool initialization failed: {e}")
        system_logger.warning("Working in Offline Mode - using SQLite only")
    finally:
        _pool_ready.set()

# تشغيل إنشاء الـ Pool في خيط خلفي فوراً عند تحميل الموديول
# FIX #1: Non-daemon thread, tracked in system_state.active_threads
pool_init_thread = threading.Thread(target=_init_pool_async, name="PoolInitializer")
pool_init_thread.start()
system_state.active_threads.append(pool_init_thread)


def get_connection():
    # الحصول على اتصال من PostgreSQL Pool (مع timeout أقصر)
    _pool_ready.wait(timeout=10)  # تقليل من 15 إلى 10 ثواني
    if connection_pool is not None:
        try:
            conn = connection_pool.getconn()
            return conn
        except Exception as e:
            error_logger.error(f"Error getting connection from pool: {e}")
            # In case of pool exhaustion, wait and retry once
            time.sleep(2)
            try:
                conn = connection_pool.getconn()
                return conn
            except Exception as retry_error:
                error_logger.error(f"Retry failed to get connection: {retry_error}")
    return None

def release_connection(conn):
    # إعادة الاتصال إلى Connection Pool
    if connection_pool is not None and conn is not None:
        try:
            connection_pool.putconn(conn)
        except Exception as e:
            error_logger.error(f"Error releasing connection to pool: {e}")
            # In critical cases, try to close the connection directly
            try:
                conn.close()
            except Exception:
                pass  # Already failed, ignore additional errors


# FIXED: REMOVED DUPLICATE DatabaseErrorHandler CLASS
# This class was already imported from error_handler module (line 52)
# Keeping local definition caused silent override - DELETED entirely
# Lines 324-429 removed to use ONLY imported version

    def safe_execute_db_operation(operation_func, db_type="sqlite", *args, **kwargs):
        """FIXED: Use thread-local connections with write queue enforcement
        
        Args:
            operation_func: الدالة المراد تنفيذها
            db_type: نوع قاعدة البيانات صراحةً ("sqlite" أو "postgres")
            *args: متغيرات إضافية للدالة
            **kwargs: متغيرات keyword إضافية
        """
        conn = None
        cursor = None
        try:
            # Handle different connection types
            if 'conn' in kwargs:
                conn = kwargs.pop('conn')
            else:
                # FIXED: Use thread-local connections for SQLite (NOT shared manager)
                if db_type == "sqlite":
                    conn = get_thread_connection(authorized=True)  # ✅ Thread-safe, isolated per thread
                elif db_type == "postgres":
                    conn = get_connection()
                else:
                    raise ValueError(f"Invalid db_type: {db_type}. Must be 'sqlite' or 'postgres'")
            
            if not conn:
                raise Exception("Failed to get database connection")
            
            cursor = conn.cursor()
            result = operation_func(cursor, conn, *args, **kwargs)
            
            return result
            
        except Exception as e:
            # Rollback if transaction was in progress
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass  # Ignore rollback errors
            
            error_logger.error(f"Database operation failed: {e}")
            raise
            
        finally:
            # Ensure proper cleanup
            if cursor:
                try:
                    cursor.close()
                except Exception as e:
                    error_logger.debug(f"Cursor close exception (ignored): {e}")
            
            # FIXED: Use thread-local return for SQLite (NOT shared manager)
            if conn and db_type == "sqlite":
                return_thread_connection(conn)  # ✅ Thread-safe return
            elif conn and db_type == "postgres":
                release_connection(conn)


class ConnectionPoolManager:
    """مدير تجمع الاتصالات مع ضمانات أمان وأداء"""
    
    def __init__(self):
        self.max_pool_size = 20
        self.connection_timeout = 30  # seconds
        self.idle_timeout = 300  # seconds
        
    def get_safe_connection(self):
        """الحصول على اتصال مع timeout آمن"""
        start_time = time.time()
        
        try:
            # Wait for pool with timeout
            if not _pool_ready.wait(timeout=self.connection_timeout):
                raise TimeoutError(f"Connection pool timeout after {self.connection_timeout} seconds")
            
            if connection_pool is None:
                raise Exception("Connection pool not initialized")
            
            conn = connection_pool.getconn()
            if conn is None:
                raise Exception("Failed to get connection from pool")
            
            # Set connection timeout
            conn.set_session(autocommit=False, isolation_level='READ COMMITTED')
            
            return conn
            
        except Exception as e:
            # Log with timing info
            elapsed = time.time() - start_time
            error_logger.error(f"Failed to get connection after {elapsed:.2f}s: {e}")
            raise
            
    def safe_release_connection(self, conn):
        """إعادة الاتصال بأمان"""
        try:
            if conn and connection_pool:
                # Check if connection is still valid
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                    connection_pool.putconn(conn)
                except Exception:
                    # Connection is broken, close directly
                    try:
                        conn.close()
                    except Exception as e:
                        error_logger.debug(f"Connection close exception (ignored): {e}")
        except Exception as e:
            error_logger.error(f"Error releasing connection: {e}")



def start_pg_keepalive():
    # FIX #1, #4: Start keepalive as tracked non-daemon thread
    def keepalive_worker():
        while system_state.running:  # FIX #3, #6: Use system_state.running flag for graceful shutdown
            try:
                # FIXED: Use system_state.mode instead of DB_AVAILABLE
                if system_state.mode == "ONLINE" and connection_pool is not None:
                    conn = get_connection()
                    if conn:
                        # استخدام cursor بسيط بدون prepared statement
                        cursor = conn.cursor()
                        cursor.execute("SELECT 1")
                        cursor.close()
                        release_connection(conn)
            except Exception as e:
                error_logger.debug(f"PostgreSQL keepalive error: {e}")
            time.sleep(600)  # كل 10 دقائق (تقليل العبء)
    
    # FIX #1: Non-daemon, tracked thread
    keepalive_thread = threading.Thread(target=keepalive_worker, name="PostgreSQLKeepalive")
    keepalive_thread.start()
    system_state.active_threads.append(keepalive_thread)

# ==========================================
# SQLite Connection Manager (Improvement #4)
# ==========================================
# Global SQLite Write Lock for thread-safe writes
# FIX: Use RLock (Reentrant Lock) to allow same thread to acquire lock multiple times
sqlite_write_lock = threading.RLock()

# ==========================================
# FIXED: REMOVED SQLiteConnectionManager CLASS
# Thread-local connections (get_thread_connection/return_thread_connection) 
# provide safer, per-thread isolation without shared state
# Lines 478-544 deleted entirely

def verify_system_integrity():
    """التحقق من سلامة النظام قبل البدء"""
    print("🔍 Verifying system integrity...")
    
    # Check required directories
    for directory in [DATA_DIR, LOG_DIR, BACKUP_DIR]:
        if not os.path.exists(directory):
            try:
                os.makedirs(directory)
                system_logger.info(f"Created directory: {directory}")
            except Exception as e:
                error_logger.error(f"Failed to create directory {directory}: {e}")
                return False
    
    # Check config.json
    if not os.path.exists(CONFIG_PATH):
        system_logger.warning("Config.json not found, will be created automatically")
    else:
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                config = json.load(f)
                required_fields = ['db_host', 'db_name', 'user', 'password', 'port', 'sslmode']
                missing = [field for field in required_fields if field not in config]
                if missing:
                    system_logger.warning(f"Missing config fields: {missing}")
                else:
                    system_logger.info("Config.json is valid")
        except Exception as e:
            error_logger.error(f"Invalid config.json: {e}")
    
    # Check database files
    if os.path.exists(LOCAL_DB_PATH):
        system_logger.info(f"SQLite database found: {LOCAL_DB_PATH}")
    else:
        system_logger.info("SQLite database will be created on first run")
    
    # Check log files
    system_log = os.path.join(LOG_DIR, "system.log")
    error_log = os.path.join(LOG_DIR, "errors.log")
    sync_log = os.path.join(LOG_DIR, "sync.log")
    
    for log_file in [system_log, error_log, sync_log]:
        if not os.path.exists(log_file):
            try:
                with open(log_file, 'w', encoding='utf-8') as f:
                    pass  # Create empty file
                system_logger.info(f"Created log file: {log_file}")
            except Exception as e:
                error_logger.error(f"Failed to create log file {log_file}: {e}")
                return False
    
    # ✅ تحسين #13: التحقق من الاتصال بالإنترنت
    system_logger.info("Checking internet connection...")
    # FIXED: Revert to global check as DatabaseManager is not yet initialized
    if check_internet_connection():
        system_logger.info("Internet connection: AVAILABLE")
    else:
        system_logger.warning("Internet connection: OFFLINE (will work in local mode)")
    
    # ✅ تحسين #15: التحقق من Schema لقاعدة البيانات
    system_logger.info("Validating database schema...")
    try:
        import sqlite3
        conn = sqlite3.connect(LOCAL_DB_PATH)
        cursor = conn.cursor()
        
        # Check required tables
        required_tables = ['patients', 'visits', 'sync_queue', 'users', 'audit_log', 'device_config']
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        missing_tables = [t for t in required_tables if t not in existing_tables]
        if missing_tables:
            system_logger.warning(f"Missing tables (will be created on first use): {missing_tables}")
        else:
            system_logger.info(f"All required tables present: {required_tables}")
        
        # Check critical columns in patients table
        cursor.execute("PRAGMA table_info(patients)")
        patient_cols = [col[1] for col in cursor.fetchall()]
        critical_cols = ['patient_id', 'name', 'national_id', 'is_unknown']
        missing_cols = [c for c in critical_cols if c not in patient_cols]
        
        if missing_cols:
            system_logger.warning(f"Missing columns in patients table: {missing_cols}")
        else:
            system_logger.info("Patients table schema valid")
        
        conn.close()
        system_logger.info("Database schema validation completed")
    except Exception as e:
        system_logger.warning(f"Schema validation skipped (database may not exist yet): {e}")
    
    system_logger.info("System integrity verified successfully!")
    return True


# FIX #3: Add graceful shutdown function with thread tracking
def start_background_threads():
    """
    FIX #1: Start and track all background threads properly
    - No critical threads use daemon=True
    - All threads tracked in system_state.active_threads list
    """
    system_logger.info("Starting background workers...")
    
    # FIXED: CRITICAL WRITE QUEUE INTEGRATION
    # REMOVED write_queue_worker thread - WriteQueueManager has its own internal worker
    # write_queue_manager.start_worker() already called at global initialization (line 279)
    
    # FIX #4: Keepalive is now started separately via start_pg_keepalive()
    # This function is called from _init_heavy_resources after pool is ready
    
    system_logger.info(f"Background workers initialized ({len(system_state.active_threads)} threads)")

def shutdown_system():
    """
    FIX #1, #3: Graceful shutdown lifecycle with proper thread cleanup
    - Set system_state.running = False
    - Stop all threads with join(timeout=5)
    - Close all connections
    """
    
    
    system_logger.info("🛑 Starting graceful shutdown...")
    system_state.running = False
    
    # Wait for all tracked threads to finish (with timeout)
    system_logger.info(f"Waiting for {len(system_state.active_threads)} threads to finish...")
    for t in system_state.active_threads:
        try:
            if t.is_alive():
                system_logger.info(f"Joining thread: {t.name}")
                t.join(timeout=5)  # Increased timeout for safer shutdown
                if t.is_alive():
                    error_logger.warning(f"Thread {t.name} did not shut down gracefully")
        except Exception as e:
            error_logger.error(f"Error joining thread {getattr(t, 'name', 'unknown')}: {e}")
    
    # Stop keepalive thread if running
    try:
        if system_state.pg_keepalive_running:
            system_state.pg_keepalive_running = False
            system_logger.info("✅ PostgreSQL keepalive signal sent")
    except Exception as e:
        error_logger.error(f"Error stopping keepalive: {e}")
    
    # Close PostgreSQL connection pool
    try:
        global connection_pool
        if connection_pool is not None:
            connection_pool.closeall()
            system_logger.info("✅ PostgreSQL connection pool closed")
            connection_pool = None
    except Exception as e:
        error_logger.error(f"Error closing PostgreSQL pool: {e}")
    
    # Stop write queue manager for clean shutdown
    try:
        write_queue_manager.stop()
        system_logger.info("✅ Write queue manager stopped")
    except Exception as e:
        error_logger.error(f"Error stopping write queue: {e}")
    
    # Clean up medical auto-completion system
    try:
        if 'medical_completer' in globals():
            medical_completer.cleanup()
            system_logger.info("✅ Medical auto-completion cleaned up")
    except Exception as e:
        error_logger.error(f"Error cleaning up medical completer: {e}")
    
    system_logger.info("✅ Graceful shutdown completed")


# FIX #4: Add system status function
def get_system_status():
    """
    FIXED: Return system_state.mode directly (single source of truth)
    """
    return system_state.mode


# FIX #5: Permanent write queue worker
# FIXED: CRITICAL WRITE QUEUE INTEGRATION
# REMOVED write_queue_worker() function entirely
# WriteQueueManager now handles all write operations with its own worker thread
# Lines 688-767 deleted - legacy queue worker removed


def check_internet_connection(timeout=5):
    """
    Utility function to check internet connectivity
    Returns True if online, False otherwise
    """
    import socket
    
    try:
        # محاولة الاتصال بـ Google DNS
        socket.create_connection(("8.8.8.8", 53), timeout=timeout)
        return True
    except (socket.timeout, socket.error, OSError) as e:
        error_logger.debug(f"Google DNS unreachable: {e}")
        pass
    
    try:
        # محاولة بديلة: Cloudflare DNS
        socket.create_connection(("1.1.1.1", 53), timeout=timeout)
        return True
    except (socket.timeout, socket.error, OSError) as e:
        error_logger.debug(f"Cloudflare DNS unreachable: {e}")
        pass
    
    return False

# FIX #4: Add comprehensive health check function
def perform_health_check():
    """
    FIX #4, #6, #8, #9: Comprehensive health check at startup
    Updates system_state.health, system_state.system_ready and sets system_state.mode
    DB_AVAILABLE removed - use ONLY system_state.mode
    """
    
    
    print("\n🏥 Performing system health check...")
    
    # Default to not ready
    system_state.system_ready = False
    
    # Check SQLite
    try:
        import sqlite3
        conn = sqlite3.connect(LOCAL_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        conn.close()
        system_state.health["sqlite_ok"] = True
        print("✅ SQLite: OK")
    except Exception as e:
        system_state.health["sqlite_ok"] = False
        error_logger.error(f"SQLite health check failed: {e}")
        print(f"❌ SQLite: FAILED - {e}")
        return  # Cannot continue without SQLite
    
    # Check PostgreSQL (if configured)
    if _DB_CONFIG and _DB_CONFIG.get('password') != 'your_password_here':
        conn = None
        try:
            if connection_pool is not None:
                conn = connection_pool.getconn()
                if conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    cursor.close()
                    if result and result[0] == 1:
                        system_state.health["db_online"] = True
                        # DB_AVAILABLE removed - use system_state.mode only
                        print("✅ PostgreSQL: ONLINE")
                    else:
                        raise Exception("PostgreSQL query failed")
        except Exception as e:
            system_state.health["db_online"] = False
            # DB_AVAILABLE removed - use system_state.mode only
            error_logger.error(f"PostgreSQL health check failed: {e}")
            print(f"❌ PostgreSQL: OFFLINE - {e}")
        finally:
            # FIX #5: Ensure connection is returned even on error
            if conn and connection_pool:
                try:
                    connection_pool.putconn(conn)
                except Exception as e:
                    error_logger.error(f"Error returning connection: {e}")
    else:
        system_state.health["db_online"] = False
        # DB_AVAILABLE removed - use system_state.mode only
        print("⚠️  PostgreSQL: SKIPPED (placeholder password or no config)")
    
    # FIXED: Use system_state.mode logic only (removed DB_AVAILABLE dependency)
    # FIX: Declare global to avoid UnboundLocalError
    if system_state.health["db_online"] and system_state.health["sqlite_ok"]:
        system_state.mode = "ONLINE"
        system_state.system_ready = True  # FIX #9: System ready
        print("\n🎉 SYSTEM MODE: ONLINE (PostgreSQL + SQLite ready)")
    elif system_state.health["sqlite_ok"]:
        system_state.mode = "OFFLINE"
        system_state.system_ready = True  # FIX #9: Still ready in offline mode
        print("\n⚠️  SYSTEM MODE: OFFLINE (SQLite only - PostgreSQL unreachable)")
    else:
        system_state.mode = "OFFLINE"
        system_state.system_ready = False  # FIX #9: Critical failure
        print("\n❌ SYSTEM MODE: OFFLINE (Critical failure)")
    
    system_state.health["last_check"] = datetime.now().isoformat()
    
    # FIXED: Log final status (DB_AVAILABLE removed)
    system_logger.info(f"Health check completed: system_state.mode={system_state.mode}, system_state.system_ready={system_state.system_ready}")
    
    return system_state.health

# FIXED: REMOVED GLOBAL sqlite_manager INSTANCE
# Thread-local connections are used directly via get_thread_connection(authorized=True)
# Line 854 removed - no global manager needed

# Login Protection System will be instantiated after class definition

def get_auto_device_id() -> str:
    # توليد device_id تلقائياً:
    # 1) socket.gethostname()
    # 2) uuid.getnode() (MAC)
    # 3) fallback ثابت في حال الفشل
    try:
        host = socket.gethostname()
        if host:
            return host
    except Exception as e:
        error_logger.error(f"Error getting hostname for device_id: {e}")

    try:
        mac_int = uuid.getnode()
        mac_hex = f"{mac_int:012x}"
        return f"MAC_{mac_hex}"
    except Exception as e:
        error_logger.error(f"Error getting MAC for device_id: {e}")

    return "UNKNOWN_DEVICE"

# ==========================================
# Login Protection System
# ==========================================
class LogViewerDialog(QDialog):
    """نافذة عرض سجلات النظام مع تحديث تلقائي"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("عارض السجلات")
        self.setFixedSize(900, 600)
        self.setLayoutDirection(Qt.RightToLeft)
        self.log_files = {
            'system': os.path.join(LOG_DIR, "system.log"),
            'errors': os.path.join(LOG_DIR, "errors.log"),
            'sync': os.path.join(LOG_DIR, "sync.log")
        }
        self.init_ui()
        
        # تحديث تلقائي كل 5 ثواني
        self.refresh_timer = QTimer(self)
        self.refresh_timer.timeout.connect(self.refresh_logs)
        self.refresh_timer.start(5000)
    
    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # Tabs
        tabs = QTabWidget()
        
        for log_type, log_file in self.log_files.items():
            tab = QWidget()
            tab_layout = QVBoxLayout(tab)
            
            # Text area
            text_area = QTextEdit()
            text_area.setReadOnly(True)
            text_area.setFont(QFont("Consolas", 10))
            text_area.setStyleSheet("""
                QTextEdit {
                    background-color: #1E1E1E;
                    color: #D4D4D4;
                    border: 1px solid #3E3E3E;
                    border-radius: 5px;
                    padding: 10px;
                }
            """)
            
            # Load initial content
            if os.path.exists(log_file):
                with open(log_file, 'r', encoding='utf-8') as f:
                    text_area.setPlainText(f.read())
            else:
                text_area.setPlainText("لا توجد سجلات متاحة")
            
            tab_layout.addWidget(text_area)
            tabs.addTab(tab, f"📋 {log_type.title()}")
            
            # Store reference for updates
            setattr(self, f'{log_type}_text', text_area)
        
        layout.addWidget(tabs)
        
        # Buttons
        btn_layout = QHBoxLayout()
        
        refresh_btn = QPushButton("🔄 تحديث")
        refresh_btn.clicked.connect(self.refresh_logs)
        refresh_btn.setStyleSheet("""
            QPushButton {
                background-color: #007AFF;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px 20px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #0062CC;
            }
        """)
        btn_layout.addWidget(refresh_btn)
        
        clear_btn = QPushButton("🗑️ مسح")
        clear_btn.clicked.connect(lambda: self.clear_current_tab(tabs))
        clear_btn.setStyleSheet("""
            QPushButton {
                background-color: #EF4444;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px 20px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #DC2626;
            }
        """)
        btn_layout.addWidget(clear_btn)
        
        close_btn = QPushButton("❌ إغلاق")
        close_btn.clicked.connect(self.close)
        close_btn.setStyleSheet("""
            QPushButton {
                background-color: #6B7280;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px 20px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #4B5563;
            }
        """)
        btn_layout.addWidget(close_btn)
        
        layout.addLayout(btn_layout)
    
    def refresh_logs(self):
        """تحديث محتوى السجلات"""
        for log_type, log_file in self.log_files.items():
            if os.path.exists(log_file):
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                        text_widget = getattr(self, f'{log_type}_text', None)
                        if text_widget:
                            text_widget.setPlainText(content)
                            # Scroll to bottom
                            scrollbar = text_widget.verticalScrollBar()
                            scrollbar.setValue(scrollbar.maximum())
                except Exception as e:
                    error_logger.error(f"Error reading {log_type} log: {e}")
    
    def clear_current_tab(self, tabs):
        """مسح السجل الحالي"""
        current_index = tabs.currentIndex()
        log_types = list(self.log_files.keys())
        if current_index < len(log_types):
            log_type = log_types[current_index]
            text_widget = getattr(self, f'{log_type}_text', None)
            if text_widget:
                reply = QMessageBox.question(
                    self,
                    "تأكيد المسح",
                    f"هل أنت متأكد من مسح سجل {log_type}?",
                    QMessageBox.Yes | QMessageBox.No
                )
                if reply == QMessageBox.Yes:
                    # Clear file content
                    try:
                        with open(self.log_files[log_type], 'w', encoding='utf-8') as f:
                            pass  # Truncate file
                        text_widget.setPlainText(f"تم مسح سجل {log_type}")
                        system_logger.info(f"{log_type} log cleared by user")
                    except Exception as e:
                        error_logger.error(f"Error clearing {log_type} log: {e}")
                        QMessageBox.critical(self, "خطأ", f"فشل مسح السجل: {e}")


class LoadingDialog(QDialog):
    """نافذة تحميل احترافية مع مؤشر تقدم"""
    def __init__(self, message="جاري التحميل...", parent=None):
        super().__init__(parent)
        self.setWindowTitle("جاري المعالجة")
        self.setFixedSize(300, 150)
        self.setLayoutDirection(Qt.RightToLeft)
        self.init_ui(message)
    
    def init_ui(self, message):
        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignCenter)
        
        # Spinner
        spinner = QProgressBar()
        spinner.setRange(0, 0)  # Indeterminate mode
        spinner.setStyleSheet("""
            QProgressBar {
                border: 2px solid #E0E0E0;
                border-radius: 10px;
                background-color: #F5F5F5;
            }
            QProgressBar::chunk {
                background-color: #007AFF;
                border-radius: 8px;
            }
        """)
        layout.addWidget(spinner)
        
        # Message
        msg_label = QLabel(message)
        msg_label.setAlignment(Qt.AlignCenter)
        msg_label.setStyleSheet("font-size: 14px; color: #555; margin-top: 10px;")
        layout.addWidget(msg_label)
        
        # Status label
        self.status_label = QLabel("")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setStyleSheet("font-size: 12px; color: #888; margin-top: 5px;")
        layout.addWidget(self.status_label)
        
        self.setWindowModality(Qt.ApplicationModal)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
    
    def set_status(self, status):
        self.status_label.setText(status)


class LoginProtection:
    """نظام حماية تسجيل الدخول مع rate limiting"""
    def __init__(self):
        self.attempts = {}  # {username: (attempts_count, first_attempt_time)}
        self.lock = threading.Lock()
        self.lockout_duration = 300  # 5 دقائق
        self.max_attempts = 5
    
    def record_attempt(self, username: str, success: bool = False):
        """تسجيل محاولة تسجيل الدخول"""
        with self.lock:
            if success:
                # مسح السجل عند نجاح الدخول
                if username in self.attempts:
                    del self.attempts[username]
                return True
            
            now = time.time()
            if username in self.attempts:
                attempts_count, first_attempt = self.attempts[username]
                # إذا مر أكثر من 5 دقائق، إعادة تعيين المحاولات
                if now - first_attempt > self.lockout_duration:
                    self.attempts[username] = (1, now)
                else:
                    self.attempts[username] = (attempts_count + 1, first_attempt)
            else:
                self.attempts[username] = (1, now)
            
            return False
    
    def is_locked(self, username: str) -> tuple[bool, int]:
        """التحقق إذا كان الح_locked. يرجع (locked, remaining_seconds)"""
        with self.lock:
            if username not in self.attempts:
                return False, 0
            
            attempts_count, first_attempt = self.attempts[username]
            if attempts_count >= self.max_attempts:
                elapsed = time.time() - first_attempt
                if elapsed < self.lockout_duration:
                    remaining = int(self.lockout_duration - elapsed)
                    return True, remaining
                else:
                    # إعادة تعيين بعد انتهاء فترة الحظر
                    del self.attempts[username]
                    return False, 0
            return False, 0
    
    def get_attempts_info(self, username: str) -> dict:
        """الحصول على معلومات المحاولات"""
        with self.lock:
            if username not in self.attempts:
                return {"attempts": 0, "remaining": self.max_attempts}
            
            attempts_count, first_attempt = self.attempts[username]
            remaining = max(0, self.max_attempts - attempts_count)
            return {"attempts": attempts_count, "remaining": remaining}


class MedicalAutoCompleter:
    """
    Intelligent auto-completion system for medical input fields.
    Provides smart suggestions as users type to speed up data entry.
    """
    
    # Comprehensive medical terminology database
    MEDICAL_SUGGESTIONS = {
        'complaints': [
            'صداع حاد', 'صداع نصفي', 'دوار', 'غثيان', 'تقيؤ',
            'ألم في الصدر', 'ضيق تنفس', 'سعال جاف', 'سعال مصحوب ببلغم',
            'ألم في البطن', 'إسهال', 'إمساك', 'انتفاخ البطن',
            'حمى', 'قشعريرة', 'تعرق ليلي', 'فقدان وزن', 'فقدان شهية',
            'ألم في المفاصل', 'تورم المفاصل', 'تيبس الصباح',
            'ألم في الظهر', 'ألم في الرقبة', 'ألم في الأطراف',
            'خدر في الأطراف', 'تنميل', 'ضعف عام', 'إرهاق مزمن',
            'طفح جلدي', 'حكة', 'احمرار الجلد', 'تورم الوجه',
            'صعوبة البلع', 'ألم عند البلع', 'بحة الصوت',
            'نزيف', 'كدمات سهلة', 'تورم الغدد الليمفاوية',
            'خفقان القلب', 'عدم انتظام ضربات القلب',
            'ألم في العين', 'تشوش الرؤية', 'جفاف العين',
            'طنين الأذن', 'دوار الوضع', 'فقدان التوازن',
            'صعوبة التبول', 'ألم عند التبول', 'كثرة التبول',
            'ألم خاصرة', 'مغص كلوي'
        ],
        'diagnoses': [
            'ارتفاع ضغط الدم الأساسي', 'داء السكري النوع الثاني',
            'داء السكري النوع الأول', 'فرط شحميات الدم',
            'قصور الغدة الدرقية', 'فرط نشاط الغدة الدرقية',
            'الربو القصبي', 'مرض الانسداد الرئوي المزمن',
            'التهاب الشعب الهوائية الحاد', 'ذات الرئة',
            'التهاب المعدة الحاد', 'قرحة المعدة', 'ارتجاع المريء',
            'التهاب المرارة', 'حصى المرارة', 'التهاب الزائدة الدودية',
            'المغص الكلوي', 'حصى الكلى', 'التهاب المسالك البولية',
            'التهاب المفاصل التنكسي', 'هشاشة العظام', 'التهاب الفقار اللاصق',
            'النقرس', 'التهاب المفاصل الروماتويدي', 'الذئبة الحمامية',
            'الاكتئاب السريري', 'اضطراب القلق العام', 'نوبات الهلع',
            'الأرق المزمن', 'الصداع النصفي', 'الصداع التوتري',
            'فقر الدم بعوز الحديد', 'فقر الدم الخبيث',
            'التهاب الكبد الفيروسي أ', 'التهاب الكبد الفيروسي ب',
            'التهاب الكبد الفيروسي ج', 'سل رئوي',
            'حمى التيفوئيد', 'الملاريا', 'حمى الضنك',
            'الحساسية الموسمية', 'التهاب الأنف التحسسي', 'الشرى',
            'الإكزيما', 'الصدفية', 'التهاب الجلد التماسي',
            'قصور القلب الاحتقاني', 'الذبحة الصدرية', 'احتشاء عضلة القلب',
            'الرجفان الأذيني', 'تسرع القلب فوق البطيني',
            'التهاب الجيوب الأنفية الحاد', 'التهاب اللوزتين الحاد',
            'التهاب الأذن الوسطى', 'التهاب الملتحمة',
            'التهاب القولون العصبي', 'داء كرون', 'التهاب القولون التقرحي',
            'الإمساك المزمن', 'الإسهال المزمن',
            'تضخم البروستاتا الحميد', 'التهاب البروستاتا',
            'عدوى الجهاز التنفسي العلوي', 'عدوى الجهاز التنفسي السفلي',
            'كوفيد-19', 'إنفلونزا موسمية', 'زكام عادي'
        ],
        'chronic_diseases': [
            'ارتفاع ضغط الدم المزمن', 'داء السكري',
            'أمراض القلب التاجية', 'قصور القلب المزمن',
            'الرجفان الأذيني المزمن', 'الربو المزمن',
            'مرض الانسداد الرئوي المزمن (COPD)',
            'قصور الغدة الدرقية المزمن', 'فرط نشاط الغدة الدرقية المزمن',
            'هشاشة العظام المتقدمة', 'التهاب المفاصل الروماتويدي',
            'الذئبة الحمامية الجهازية', 'التصلب المتعدد',
            'مرض باركنسون', 'الصرع', 'الفشل الكلوي المزمن',
            'تليف الكبد', 'التهاب الكبد المزمن ب', 'التهاب الكبد المزمن ج',
            'داء الأمعاء الالتهابي', 'داء كرون', 'التهاب القولون التقرحي',
            'فقر الدم المنجلي', 'الثلاسيميا', 'الهيموفيليا',
            'السرطان النشط', 'العلاج الكيميائي الحالي',
            'العلاج الإشعاعي الحالي', 'زراعة الأعضاء',
            'فيروس نقص المناعة البشرية (HIV)', 'السل المزمن',
            'الانسداد الرئوي المزمن', 'توسع القصبات',
            'التليف الرئوي مجهول السبب', 'ارتفاع ضغط الدم الرئوي',
            'مرض الشريان المحيطي', 'السكتة الدماغية السابقة',
            'الخرف ومرض الزهايمر', 'الاعتلال العصبي السكري',
            'اعتلال الكلى السكري', 'اعتلال الشبكية السكري'
        ],
        'medications': [
            'أسبرين 81 ملغ', 'أسبرين 325 ملغ',
            'ميتفورمين 500 ملغ', 'ميتفورمين 850 ملغ', 'ميتفورمين 1000 ملغ',
            'أتورفاستاتين 10 ملغ', 'أتورفاستاتين 20 ملغ', 'أتورفاستاتين 40 ملغ',
            'روزوفاستاتين 5 ملغ', 'روزوفاستاتين 10 ملغ', 'روزوفاستاتين 20 ملغ',
            'ليسيبريل 5 ملغ', 'ليسيبريل 10 ملغ', 'ليسيبريل 20 ملغ',
            'إنالابريل 2.5 ملغ', 'إنالابريل 5 ملغ', 'إنالابريل 10 ملغ',
            'أملوديبين 5 ملغ', 'أملوديبين 10 ملغ',
            'ميتوبرولول 25 ملغ', 'ميتوبرولول 50 ملغ', 'ميتوبرولول 100 ملغ',
            'بيسوبورول 2.5 ملغ', 'بيسوبورول 5 ملغ', 'بيسوبورول 10 ملغ',
            'أوميبرازول 20 ملغ', 'إيسوميبرازول 20 ملغ', 'بانتوبرازول 40 ملغ',
            'رانيتيدين 150 ملغ', 'فاموتيدين 20 ملغ',
            'باراسيتامول 500 ملغ', 'باراسيتامول 1000 ملغ',
            'إيبوبروفين 400 ملغ', 'إيبوبروفين 600 ملغ', 'ديكلوفيناك 50 ملغ',
            'نابروكسين 250 ملغ', 'سيليكوكسيب 200 ملغ',
            'أموكسيسيللين 500 ملغ', 'أموكسيسيللين 1000 ملغ',
            'أزيثروميسين 250 ملغ', 'أزيثروميسين 500 ملغ',
            'كلاريثروميسين 250 ملغ', 'كلاريثروميسين 500 ملغ',
            'سيبروفلوكساسين 250 ملغ', 'سيبروفلوكساسين 500 ملغ',
            'ليفوفلوكساسين 500 ملغ', 'ليفوفلوكساسين 750 ملغ',
            'سيفترياكسون 1 غ', 'سيفترياكسون 2 غ',
            'فUROSEMIDE 20 ملغ', 'فUROSEMIDE 40 ملغ',
            'هيدروكلوروثيازيد 25 ملغ', 'إنداباميد 1.5 ملغ',
            'سبيرونولاكتون 25 ملغ', 'سبيرونولاكتون 50 ملغ',
            'وارفارين 2 ملغ', 'وارفارين 5 ملغ',
            'ريفاروكسابان 10 ملغ', 'ريفاروكسابان 15 ملغ', 'ريفاروكسابان 20 ملغ',
            'أبيكسابان 2.5 ملغ', 'أبيكسابان 5 ملغ',
            'كلوبيدوجريل 75 ملغ', 'تيكاغلورور 90 ملغ',
            'أنسولين جلارجين', 'أنسولين ديتيمير', 'أنسولين ليسبرو',
            'أنسولين أسبارت', 'أنسولين ريجولار',
            'غليمبيريد 1 ملغ', 'غليمبيريد 2 ملغ', 'غليمبيريد 4 ملغ',
            'غليكلازيد 30 ملغ', 'غليكلازيد 60 ملغ', 'غليكلازيد 80 ملغ',
            'سيتاغليبتين 25 ملغ', 'سيتاغليبتين 50 ملغ', 'سيتاغليبتين 100 ملغ',
            'فوغليبوز 0.2 ملغ', 'فوغليبوز 0.3 ملغ',
            'ليفوثيروكسين 25 مكغ', 'ليفوثيروكسين 50 مكغ', 'ليفوثيروكسين 100 مكغ',
            'بريدنيزولون 5 ملغ', 'بريدنيزولون 10 ملغ', 'بريدنيزولون 20 ملغ',
            'ديكساميثازون 0.5 ملغ', 'ديكساميثازون 4 ملغ',
            'هيدروكورتيزون 10 ملغ', 'هيدروكورتيزون 20 ملغ',
            'سال بوتامول 100 مكغ', 'فلوتيكازون 50 مكغ',
            'بوديزونيد 200 مكغ', 'تيوتروبيوم 18 مكغ',
            'مونتلوكاست 10 ملغ', 'لوراتادين 10 ملغ', 'سيتيريزين 10 ملغ',
            'فيكسوفينادين 120 ملغ', 'فيكسوفينادين 180 ملغ',
            'جبابنتين 100 ملغ', 'جبابنتين 300 ملغ', 'جبابنتين 400 ملغ',
            'بريجابالين 75 ملغ', 'بريجابالين 150 ملغ', 'بريجابالين 300 ملغ',
            'أميتريبتيلين 10 ملغ', 'أميتريبتيلين 25 ملغ',
            'فلوكستين 20 ملغ', 'سيرترالين 50 ملغ', 'سيرترالين 100 ملغ',
            'إسيتالوبرام 10 ملغ', 'إسيتالوبرام 20 ملغ',
            'فينلافاكسين 37.5 ملغ', 'فينلافاكسين 75 ملغ',
            'ألبرازولام 0.25 ملغ', 'ألبرازولام 0.5 ملغ', 'ألبرازولام 1 ملغ',
            'كلونازيبام 0.5 ملغ', 'كلونازيبام 2 ملغ',
            'زولبيديم 5 ملغ', 'زولبيديم 10 ملغ',
            'أريبيبرازول 5 ملغ', 'أريبيبرازول 10 ملغ', 'أريبيبرازول 15 ملغ',
            'كيتيابين 25 ملغ', 'كيتيابين 100 ملغ', 'كيتيابين 200 ملغ',
            'ريسبيريدون 1 ملغ', 'ريسبيريدون 2 ملغ', 'ريسبيريدون 3 ملغ',
            'فالبروات الصوديوم 200 ملغ', 'فالبروات الصوديوم 500 ملغ',
            'كاربامازيبين 200 ملغ', 'كاربامازيبين 400 ملغ',
            'لاموتريجين 25 ملغ', 'لاموتريجين 50 ملغ', 'لاموتريجين 100 ملغ',
            'توبيرامات 25 ملغ', 'توبيرامات 50 ملغ', 'توبيرامات 100 ملغ',
            '_donepezil_5_ملغ', 'donepezil_10_ملغ', 'ميمانتين 10 ملغ',
            'ليvodopa_100_ملغ', 'براميبكسول 0.25 ملغ', 'روبينيرول 1 ملغ',
            'ألو بورينول 100 ملغ', 'ألو بورينول 300 ملغ',
            'كولشيسين 0.5 ملغ', 'باكلوفين 10 ملغ', 'tizanidine_2_ملغ',
            'سيكلوبنزابرين 5 ملغ', 'ميثوكاربامول 500 ملغ',
            'كالسيوم + فيتامين د', 'كالسيوم سيتريت 500 ملغ',
            'فيتامين د 1000 وحدة', 'فيتامين د 2000 وحدة', 'فيتامين د 5000 وحدة',
            'فيتامين ب12 500 مكغ', 'فيتامين ب المركب',
            'حديد 65 ملغ', 'حديد فومارات 200 ملغ',
            'أوميغا 3 1000 ملغ', 'كوإنزيم Q10 100 ملغ',
            'بروبيوتيك', 'ألياف غذائية', 'مكملات غذائية'
        ],
        'allergies': [
            'بنسلين', 'أموكسيسيللين', 'سيفالوسبورين',
            'أسبرين', 'مضادات الالتهاب غير الستيرويدية', 'إيبوبروفين',
            'سلفوناميدات', 'كو-تريموكسازول',
            'ماكروليدات', 'أزيثروميسين', 'إريثروميسين',
            'فلوروكوينولونات', 'سيبروفلوكساسين', 'ليفوفلوكساسين',
            'أمينوغليكوزيدات', 'جنتاميسين', 'أميكاسين',
            'تتراسيكلينات', 'دوكسيسيكلين',
            'مضادات الاختلاج', 'فينيتوين', 'كاربامازيبين',
            'أدوية الضغط', 'مثبطات ACE', 'حاصرات بيتا',
            'أدوية السكري', 'ميتفورمين', 'سلفونيل يوريا',
            'أدوية النقرس', 'ألو بورينول',
            'أدوية العلاج الكيميائي',
            'لقاحات', 'مصل', 'جلوتين', 'لاكتوز',
            'فول سوداني', 'مكسرات', 'محار', 'سمك',
            'بيض', 'حليب', 'قمح', 'صويا',
            'لدغات الحشرات', 'نحل', 'لاتكس',
            'مواد تباين شعاعي', 'يود', 'جادولينيوم',
            'مخدر موضعي', 'ليدوكائين', 'بنزوكائين',
            'مخدر عام', 'بروبوفول', 'ثيوبنتال',
            'مرخيات العضلات', 'سكسينيل كولين',
            'أفيونيات', 'مورفين', 'كوديين', 'أوكسيكودون',
            'بنزوديازيبينات', 'ديازيبام', 'لورازيبام',
            'SSRIs', 'SNRIs', 'مضادات الذهان',
            'ستيرويدات قشرية', 'بريدنيزولون', 'ديكساميثازون',
            'أدوية الربو', 'سال بوتامول',
            'مضادات الهيستامين', 'لوراتادين', 'سيتيريزين',
            'أدوية الغدة الدرقية', 'ليفوثيروكسين',
            'مدرات البول', 'فوروسيميد', 'هيدروكلوروثيازيد',
            'مميعات الدم', 'وارفارين', 'هيبارين',
            'أدوية القلب', 'ديجوكسين', 'نترات',
            'أدوية العيون', 'قطرات عينية',
            'مستحضرات التجميل', 'عطور', 'صبغات شعر',
            'معادن', 'نيكل', 'كوبالت', 'كروم'
        ],
        'notes': [
            'حالة مستقرة', 'تحسن ملحوظ', 'بحاجة للمتابعة',
            'التزام بالعلاج', 'غير ملتزم بالعلاج', 'فقد للمتابعة',
            'مراجعة منتظمة', 'تحويل للاختصاصي', 'فحوصات مطلوبة',
            'خطة علاجية معدلة', 'جرعة معدلة', 'إيقاف الدواء',
            'بدء دواء جديد', 'تبديل الدواء', 'إضافة دواء',
            'نصائح غذائية', 'نصائح نمط الحياة', 'توقف عن التدخين',
            'زيادة النشاط البدني', 'حمية خاصة', 'تقليل الملح',
            'تقليل السكر', 'تقليل الدهون', 'زيادة السوائل',
            'راحة في الفراش', 'تقييد النشاط', 'علاج طبيعي',
            'علاج تنفسي', 'أكسجين إضافي', 'مراقبة سكر',
            'مراقبة ضغط', 'مراقبة حرارة', 'مراقبة نبض',
            'تسجيل سوائل', 'قياس وزن يومي', 'عناية بالجروح',
            'تغيير ضمادات', 'غيار معقم', 'خياطة جرح',
            'إزالة خياطة', 'تركيب قسطرة', 'إزالة قسطرة',
            'تركيب محاليل', 'إبرة وريدية', 'أدوية وريدية',
            'أدوية عضلية', 'أدوية تحت الجلد', 'أدوية بالفم',
            'تعليمات للمريض', 'توعية صحية', 'نصائح وقائية',
            'متابعة خارجية', 'مراجعة عاجلة', 'تحويل طوارئ',
            'حالة حرجة', 'حالة مستقرة', 'حالة خطيرة',
            'تحت المراقبة', 'عزل صحي', 'احتياطات عدوى',
            'استشارة طبية', 'رأي ثانٍ', 'فريق متعدد التخصصات',
            'خطة رعاية', 'أهداف علاجية', 'تقييم مستمر',
            'مراجعة خطة', 'تحديث بروتوكول', 'بروتوكول علاجي'
        ],
        'departments': [
            'الطوارئ', 'الباطنية', 'الجراحة العامة',
            'الأطفال', 'النساء والتوليد', 'العظام',
            'القلب', 'الأعصاب', 'الجلدية',
            'الأنف والأذن والحنجرة', 'العيون', 'المسالك البولية',
            'التخدير', 'الأشعة', 'المختبر',
            'العلاج الطبيعي', 'التغذية', 'الصيدلية',
            'العناية المركزة', 'العناية القلبية', 'عناية الأطفال',
            'الجراحة العصبية', 'جراحة القلب', 'جراحة العظام',
            'جراحة التجميل', 'جراحة الأوعية', 'جراحة الأطفال',
            'طب الأسرة', 'الصحة النفسية', 'الإدمان',
            'الأورام', 'العلاج الإشعاعي', 'أمراض الدم',
            'الغدد الصماء', 'الكلى', 'الجهاز الهضمي',
            'الروماتيزل', 'الحساسية والمناعة', 'الصدرية',
            'العدوى', 'الشيخوخة', 'الرعاية الملطفة',
            'إعادة التأهيل', 'طب الرياضة', 'طب النوم'
        ],
        'blood_types': [
            'A+', 'A-', 'B+', 'B-',
            'AB+', 'AB-', 'O+', 'O-'
        ]
    }
    
    # Fields that should NOT have auto-completion
    EXCLUDED_FIELDS = [
        'national_id', 'nid', 'phone', 'telephone', 'mobile',
        'name', 'full_name', 'username', 'password', 'email',
        'address', 'id_number', 'passport', 'license'
    ]
    
    def __init__(self):
        self.active_completers = []
    
    @classmethod
    def get_suggestions(cls, field_type: str, text: str) -> list:
        """
        Get medical suggestions based on field type and current text.
        
        Args:
            field_type: Type of field (complaints, diagnoses, medications, etc.)
            text: Current text in the input field
            
        Returns:
            List of matching suggestions
        """
        if not text or len(text.strip()) < 2:
            return []
        
        suggestions = cls.MEDICAL_SUGGESTIONS.get(field_type, [])
        text_lower = text.lower().strip()
        
        # Find matches that contain the typed text
        matches = [s for s in suggestions if text_lower in s.lower()]
        
        # Sort by relevance (starts with typed text first)
        starts_with = [s for s in matches if s.lower().startswith(text_lower)]
        contains = [s for s in matches if s not in starts_with]
        
        return starts_with + contains
    
    @classmethod
    def is_field_excluded(cls, object_name: str) -> bool:
        """
        Check if a field should be excluded from auto-completion.
        
        Args:
            object_name: The object name of the input field
            
        Returns:
            True if field should be excluded
        """
        object_name_lower = object_name.lower() if object_name else ''
        return any(excluded in object_name_lower for excluded in cls.EXCLUDED_FIELDS)
    
    @classmethod
    def detect_field_type(cls, placeholder: str, label: str = '') -> str:
        """
        Detect the type of medical field based on placeholder or label text.
        
        Args:
            placeholder: Placeholder text of the input field
            label: Label text (optional)
            
        Returns:
            Field type string or empty if not recognized
        """
        text = f"{placeholder} {label}".lower()
        
        if any(word in text for word in ['شكوى', 'complaint', 'chief']):
            return 'complaints'
        elif any(word in text for word in ['تشخيص', 'diagnosis', 'diagnoses']):
            return 'diagnoses'
        elif any(word in text for word in ['مزمن', 'chronic', 'دائم']):
            return 'chronic_diseases'
        elif any(word in text for word in ['دواء', 'medication', 'علاج', 'drug']):
            return 'medications'
        elif any(word in text for word in ['حساسية', 'allergy', 'allergies']):
            return 'allergies'
        elif any(word in text for word in ['ملاحظة', 'notes', 'ملاحظات', 'بيان']):
            return 'notes'
        elif any(word in text for word in ['قسم', 'department', 'تخصص']):
            return 'departments'
        elif any(word in text for word in ['فصيلة', 'blood', 'زمرة']):
            return 'blood_types'
        
        return ''
    
    def attach_to_input(self, input_widget, field_type: str = ''):
        """
        Attach auto-completion to an input widget.
        
        Args:
            input_widget: QLineEdit or QTextEdit widget
            field_type: Explicit field type (optional, will auto-detect if not provided)
        """
        # Skip if widget type is excluded
        if hasattr(input_widget, 'objectName'):
            if self.is_field_excluded(input_widget.objectName()):
                return
        
        # Auto-detect field type if not provided
        if not field_type:
            placeholder = input_widget.placeholderText() if hasattr(input_widget, 'placeholderText') else ''
            field_type = self.detect_field_type(placeholder)
        
        # Skip if no field type detected
        if not field_type:
            return
        
        # Create completion popup
        completer = QCompleter()
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        completer.setFilterMode(Qt.MatchContains)
        
        model = QStringListModel()
        completer.setModel(model)
        input_widget.setCompleter(completer)
        
        # Store reference for cleanup
        self.active_completers.append((input_widget, completer, model))
        
        # Update suggestions as user types
        if hasattr(input_widget, 'textChanged'):
            def update_completions(text):
                suggestions = self.get_suggestions(field_type, text)
                model.setStringList(suggestions)
                if suggestions:
                    completer.complete()
            
            input_widget.textChanged.connect(update_completions)
    
    def cleanup(self):
        """
        Clean up all active completers.
        Call this before application shutdown to prevent memory leaks.
        """
        for widget, completer, model in self.active_completers:
            try:
                # Disconnect signals to prevent crashes
                if hasattr(widget, 'textChanged'):
                    widget.textChanged.disconnect()
                
                # Clear completer reference
                if hasattr(widget, 'setCompleter'):
                    widget.setCompleter(None)
            except Exception as e:
                error_logger.debug(f"Error cleaning up completer: {e}")
        
        self.active_completers.clear()
        system_logger.info("Medical auto-completion system cleaned up")


# ==========================================
# Initialize Login Protection System
# ==========================================
login_protection = LoginProtection()

# ==========================================
# Initialize Medical Auto-Completion System
# ==========================================
medical_completer = MedicalAutoCompleter()

# ==========================================
# Constants and UI Configuration
# ==========================================
HOSPITALS = {
    "MCH": "مستشفى حميات مطوبس",
    "BCH": "مستشفى حميات بيلا", 
    "MOH": "مستشفى الرمد الرئيسي",
    "FCH": "مستشفى فوه المركزي",
    "RCH": "مستشفى الرياض المركزي",
    "SLH": "مستشفى جلدية وجذام كفرالشيخ",
    "FSH": "مستشفي فيصل سعود الفليج",
    "MTH": "مستشفى مطوبس المركزى",
    "HCH": "مستشفي الحامول المركزي",
    "BBH": "مستشفى برج البرلس المركزى",
    "KSH": "مستشفى كفر الشيخ العام",
    "SGH": "مستشفى سيدى غازى",
    "KBH": "مستشفى حميات كفر الشيخ",
    "SSH": "مستشفى سيدي سالم المركزي",
    "DGH": "مستشفى دسوق العام",
    "CLH": "مستشفى صدر كفرالشيخ",
    "BCH2": "مستشفي بيلا المركزي",
    "DDH": "مستشفى حميات دسوق"
}

TYPE_MAP_AR = {'Clinic': 'عيادات', 'Emergency': 'طوارئ', 'Inpatient': 'داخلي'}
TYPE_MAP_EN = {v: k for k, v in TYPE_MAP_AR.items()}
TYPE_COLORS = {'عيادات': '#3B82F6', 'طوارئ': '#EF4444', 'داخلي': '#10B981'}


class LoginWindow(QDialog):
    # نافذة تسجيل الدخول الاحترافية
    def __init__(self, db_manager, parent=None):
        super().__init__(parent)
        self.db = db_manager
        self.user_data = None
        
        self.setWindowTitle("برنامج الرقم الموحد - تسجيل الدخول")
        self.setFixedSize(450, 550)
        self.setWindowFlags(Qt.Window | Qt.FramelessWindowHint)
        self.setAttribute(Qt.WA_TranslucentBackground)
        
        # تعيين اتجاه النص
        self.setLayoutDirection(Qt.RightToLeft)
        
        self.setStyleSheet("""
            QDialog {
                background-color: transparent;
            }
            #loginCard {
                background-color: #FFFFFF;
                border-radius: 20px;
                border: 1px solid #E5E5E7;
            }
            QLabel {
                color: #1D1D1F;
            }
            QLineEdit {
                background-color: #F2F2F7;
                border: 1px solid #D2D2D7;
                border-radius: 12px;
                padding: 12px 16px;
                font-size: 15px;
                color: #1D1D1F;
            }
            QLineEdit:focus {
                border-color: #007AFF;
                background-color: #FFFFFF;
            }
            QPushButton {
                background-color: #007AFF;
                color: white;
                border: none;
                border-radius: 12px;
                padding: 12px 24px;
                font-size: 15px;
                font-weight: 600;
                min-height: 44px;
            }
            QPushButton:hover {
                background-color: #0071E3;
            }
        """)
        
        self.init_ui()

    def init_ui(self):
        # الحاوية الرئيسية مع خلفية شفافة لعمل ظل
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(10, 10, 10, 10)
        
        # الكارد الأبيض
        self.card = QFrame()
        self.card.setObjectName("loginCard")
        self.card.setStyleSheet("""
            #loginCard {
                background-color: #FFFFFF;
                border-radius: 20px;
                border: 1px solid #E5E5E7;
            }
        """)
        
        # إضافة ظل للكارد
        shadow = QGraphicsDropShadowEffect()
        shadow.setBlurRadius(20)
        shadow.setXOffset(0)
        shadow.setYOffset(5)
        shadow.setColor(QColor(0, 0, 0, 50))
        self.card.setGraphicsEffect(shadow)
        
        card_layout = QVBoxLayout(self.card)
        card_layout.setContentsMargins(30, 40, 30, 40)
        card_layout.setSpacing(20)
        
        # زر الإغلاق
        close_btn = QPushButton("✕")
        close_btn.setFixedSize(30, 30)
        close_btn.setStyleSheet("""
            QPushButton {
                background-color: transparent;
                color: #888;
                font-size: 18px;
                border: none;
            }
            QPushButton:hover { color: #ff5252; }
        """)
        close_btn.clicked.connect(self.reject)
        
        header_layout = QHBoxLayout()
        header_layout.addStretch()
        header_layout.addWidget(close_btn)
        card_layout.addLayout(header_layout)

        # شعار المستشفى (أيقونة افتراضية)
        logo_label = QLabel()
        logo_icon = self.style().standardIcon(QStyle.SP_ComputerIcon) # يمكن استبداله بشعار حقيقي
        logo_label.setPixmap(logo_icon.pixmap(80, 80))
        logo_label.setAlignment(Qt.AlignCenter)
        card_layout.addWidget(logo_label)
        
        # العنوان
        title_label = QLabel("نظام إدارة المرضى")
        title_label.setStyleSheet("font-size: 24px; font-weight: bold; color: #2c3e50;")
        title_label.setAlignment(Qt.AlignCenter)
        card_layout.addWidget(title_label)
        
        subtitle_label = QLabel("تسجيل الدخول للمتابعة")
        subtitle_label.setStyleSheet("font-size: 14px; color: #7f8c8d;")
        subtitle_label.setAlignment(Qt.AlignCenter)
        card_layout.addWidget(subtitle_label)
        
        card_layout.addSpacing(10)
        
        # حقول الإدخال
        self.username_input = QLineEdit()
        self.username_input.setPlaceholderText("اسم المستخدم")
        self.username_input.setFixedHeight(50)
        self.username_input.setStyleSheet(self.get_input_style())
        card_layout.addWidget(self.username_input)
        
        self.password_input = QLineEdit()
        self.password_input.setPlaceholderText("كلمة المرور")
        self.password_input.setEchoMode(QLineEdit.Password)
        self.password_input.setFixedHeight(50)
        self.password_input.setStyleSheet(self.get_input_style())
        card_layout.addWidget(self.password_input)
        
        # تذكرني
        self.remember_cb = QCheckBox("تذكر تسجيل الدخول")
        self.remember_cb.setStyleSheet("color: #2c3e50; font-size: 13px;")
        card_layout.addWidget(self.remember_cb)
        
        # زر تسجيل الدخول
        self.login_btn = QPushButton("تسجيل الدخول")
        self.login_btn.setFixedHeight(50)
        self.login_btn.setCursor(Qt.PointingHandCursor)
        self.login_btn.setStyleSheet("""
            QPushButton {
                background-color: #007AFF;
                color: white;
                border-radius: 12px;
                font-size: 15px;
                font-weight: 600;
                border: none;
                min-height: 44px;
            }
            QPushButton:hover { background-color: #0071E3; }
            QPushButton:pressed { background-color: #0056CC; }
        """)
        self.login_btn.clicked.connect(self.handle_login)
        card_layout.addWidget(self.login_btn)
        
        main_layout.addWidget(self.card)
        
        # الفوتر الرسمي
        footer_label = QLabel("✨ تم التطوير والبرمجة عن طريق إدارة سلامة المرضى بكفرالشيخ\n📞 للدعم الفني والتواصل: 01068422223")
        footer_label.setStyleSheet("color: #7f8c8d; font-size: 13px; font-weight: bold;")
        footer_label.setAlignment(Qt.AlignCenter)
        main_layout.addSpacing(10)
        main_layout.addWidget(footer_label)
        
        # دعم مفتاح Enter
        self.username_input.returnPressed.connect(self.handle_login)
        self.password_input.returnPressed.connect(self.handle_login)

    def get_input_style(self):
        return """
            QLineEdit {
                background-color: #F2F2F7;
                border: 1px solid #D2D2D7;
                border-radius: 12px;
                padding: 12px 16px;
                font-size: 15px;
                color: #1D1D1F;
            }
            QLineEdit:focus {
                border-color: #007AFF;
                background-color: #F8FBFF;
            }
        """

    def handle_login(self):
        username = self.username_input.text().strip()
        password = self.password_input.text().strip()
        
        if not username or not password:
            QMessageBox.warning(self, "تنبيه", "يرجى إدخال اسم المستخدم وكلمة المرور")
            return
        
        # OPTIONAL HARDENING: Check if user is locked
        try:
            locked, remaining = login_protection.is_locked(username)
            if locked:
                QMessageBox.critical(self, "مقفور",
                    f"تم قفل الحساب بسبب محاولات فاشلة كثيرة.\nالمدة المتبقية: {remaining} ثانية")
                return
        except Exception as e:
            error_logger.debug(f"Login protection check failed: {e}")
        
        user = self.db.authenticate_user(username, password)
        if isinstance(user, dict):  # FIX #1: Enforce dict type only
            # Record successful attempt
            try:
                login_protection.record_attempt(username, success=True)
            except Exception as e:
                error_logger.debug(f"Failed to record successful login attempt: {e}")
            
            self.user_data = user
            if self.remember_cb.isChecked():
                self.save_settings(username)
            self.accept()
        else:
            # Record failed attempt
            try:
                login_protection.record_attempt(username, success=False)
            except Exception as e:
                error_logger.debug(f"Failed to record failed login attempt: {e}")
            
            QMessageBox.critical(self, "خطأ", "اسم المستخدم أو كلمة المرور غير صحيحة")

    def save_settings(self, username):
        settings = {"remember_user": username}
        try:
            with open(SETTINGS_PATH, "w", encoding='utf-8') as f:
                json.dump(settings, f)
        except Exception as e:
            error_logger.error(f"Error saving settings: {e}")
            QMessageBox.critical(self, "خطأ", f"فشل حفظ إعدادات تسجيل الدخول:\n{e}")

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self._drag_pos = event.globalPosition().toPoint() - self.frameGeometry().topLeft()
            event.accept()

    def mouseMoveEvent(self, event):
        if event.buttons() == Qt.LeftButton and hasattr(self, '_drag_pos'):
            self.move(event.globalPosition().toPoint() - self._drag_pos)
            event.accept()

class UserPasswordDialog(QDialog):
    # نافذة تغيير كلمة المرور الاحترافية
    def __init__(self, username, parent=None):
        super().__init__(parent)
        self.username = username
        self.setWindowTitle(f"تغيير كلمة المرور - {username}")
        self.setFixedSize(400, 300)
        self.setLayoutDirection(Qt.RightToLeft)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(30, 30, 30, 30)
        layout.setSpacing(15)

        title = QLabel("🔐 تعيين كلمة مرور جديدة")
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: #2c3e50; margin-bottom: 10px;")
        layout.addWidget(title)

        self.new_pass = QLineEdit()
        self.new_pass.setPlaceholderText("كلمة المرور الجديدة")
        self.new_pass.setEchoMode(QLineEdit.Password)
        self.new_pass.setStyleSheet("padding: 12px; border: 2px solid #e0e0e0; border-radius: 8px;")
        layout.addWidget(self.new_pass)

        self.confirm_pass = QLineEdit()
        self.confirm_pass.setPlaceholderText("تأكيد كلمة المرور")
        self.confirm_pass.setEchoMode(QLineEdit.Password)
        self.confirm_pass.setStyleSheet("padding: 12px; border: 2px solid #e0e0e0; border-radius: 8px;")
        layout.addWidget(self.confirm_pass)

        btn_box = QHBoxLayout()
        save_btn = QPushButton("💾 حفظ التغييرات")
        save_btn.setStyleSheet("""
            QPushButton { background-color: #3498db; color: white; padding: 12px; border-radius: 8px; font-weight: bold; }
            QPushButton:hover { background-color: #2980b9; }
        """)
        save_btn.clicked.connect(self.handle_save)
        
        cancel_btn = QPushButton("إلغاء")
        cancel_btn.setStyleSheet("padding: 12px; border-radius: 8px;")
        cancel_btn.clicked.connect(self.reject)
        
        btn_box.addWidget(save_btn)
        btn_box.addWidget(cancel_btn)
        layout.addLayout(btn_box)

    def handle_save(self):
        p1 = self.new_pass.text().strip()
        p2 = self.confirm_pass.text().strip()
        
        if not p1:
            QMessageBox.warning(self, "تنبيه", "يرجى إدخال كلمة المرور")
            return
        if p1 != p2:
            QMessageBox.warning(self, "خطأ", "كلمة المرور غير متطابقة")
            return
        
        self.password = p1
        self.accept()

class AddUserDialog(QDialog):
    # نافذة إضافة مستخدم جديد
    def __init__(self, hospitals, parent=None):
        super().__init__(parent)
        self.hospitals = hospitals
        self.setWindowTitle("إضافة مستخدم جديد")
        self.setFixedSize(450, 450)
        self.setLayoutDirection(Qt.RightToLeft)
        self.setStyleSheet("""
            QDialog {
                background-color: #F2F2F7;
            }
            QLabel {
                color: #1D1D1F;
                font-size: 15px;
                font-weight: 500;
            }
            QLineEdit {
                background-color: #FFFFFF;
                border: 1px solid #D2D2D7;
                border-radius: 12px;
                padding: 12px 16px;
                font-size: 15px;
                color: #1D1D1F;
            }
            QLineEdit:focus {
                border-color: #007AFF;
                background-color: #F8FBFF;
            }
            QComboBox {
                background-color: #FFFFFF;
                border: 1px solid #D2D2D7;
                border-radius: 12px;
                padding: 12px 16px;
                font-size: 15px;
                color: #1D1D1F;
            }
            QPushButton {
                background-color: #007AFF;
                color: white;
                border: none;
                border-radius: 12px;
                padding: 12px 24px;
                font-size: 15px;
                font-weight: 600;
                min-height: 44px;
            }
            QPushButton:hover {
                background-color: #0071E3;
            }
        """)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(30, 30, 30, 30)
        layout.setSpacing(15)

        title = QLabel("👤 إنشاء حساب مستخدم جديد")
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: #2c3e50; margin-bottom: 10px;")
        layout.addWidget(title)

        self.username = QLineEdit()
        self.username.setPlaceholderText("اسم المستخدم")
        self.username.setStyleSheet("padding: 12px; border: 2px solid #e0e0e0; border-radius: 8px;")
        layout.addWidget(self.username)

        self.password = QLineEdit()
        self.password.setPlaceholderText("كلمة المرور")
        self.password.setEchoMode(QLineEdit.Password)
        self.password.setStyleSheet("padding: 12px; border: 2px solid #e0e0e0; border-radius: 8px;")
        layout.addWidget(self.password)

        self.hospital_combo = QComboBox()
        self.hospital_combo.addItem("إدارة النظام (ADMIN)", "ADMIN")
        for code, name in self.hospitals.items():
            self.hospital_combo.addItem(f"{name} ({code})", code)
        self.hospital_combo.setStyleSheet("padding: 10px; border: 2px solid #e0e0e0; border-radius: 8px;")
        layout.addWidget(QLabel("المستشفى المرتبط:"))
        layout.addWidget(self.hospital_combo)

        self.role_combo = QComboBox()
        self.role_combo.addItems(["hospital", "admin"])
        self.role_combo.setStyleSheet("padding: 10px; border: 2px solid #e0e0e0; border-radius: 8px;")
        layout.addWidget(QLabel("صلاحية المستخدم:"))
        layout.addWidget(self.role_combo)

        btn_box = QHBoxLayout()
        save_btn = QPushButton("➕ إضافة المستخدم")
        save_btn.setStyleSheet("""
            QPushButton { background-color: #007AFF; color: white; border-radius: 12px; font-weight: 600; }
            QPushButton:hover { background-color: #0071E3; }
        """)
        save_btn.clicked.connect(self.handle_save)
        
        cancel_btn = QPushButton("إلغاء")
        cancel_btn.clicked.connect(self.reject)
        
        btn_box.addWidget(save_btn)
        btn_box.addWidget(cancel_btn)
        layout.addLayout(btn_box)

    def handle_save(self):
        u = self.username.text().strip()
        p = self.password.text().strip()
        h = self.hospital_combo.currentData()
        r = self.role_combo.currentText()
        
        if not u or not p:
            QMessageBox.warning(self, "تنبيه", "جميع الحقول مطلوبة")
            return
            
        self.user_info = {'username': u, 'password': p, 'hospital_code': h, 'role': r}
        self.accept()


class VisitEditDialog(QDialog):
    """نافذة تعديل الزيارة بتصميم احترافي محسّن"""
    def __init__(self, visit, db, parent=None):
        super().__init__(parent)
        self.db = db
        self.visit_id = visit["visit_id"]
        
        self.setWindowTitle(f"✏️ تعديل الزيارة #{self.visit_id}")
        self.setMinimumSize(1000, 750)
        self.setLayoutDirection(Qt.RightToLeft)
        self.setStyleSheet("""
            QDialog { background-color: #F8F9FA; }
            QLabel { color: #2C3E50; font-weight: 500; }
            QLineEdit, QComboBox, QTextEdit {
                padding: 10px 14px;
                border: 2px solid #E8EAED;
                border-radius: 8px;
                background-color: #FFFFFF;
                font-size: 14px;
                color: #2C3E50;
                selection-background-color: #007AFF;
            }
            QLineEdit:focus, QComboBox:focus, QTextEdit:focus {
                border: 2px solid #007AFF;
                background-color: #F8FBFF;
            }
            QLineEdit:disabled, QComboBox:disabled, QTextEdit:disabled {
                background-color: #F5F5F6;
                color: #86868B;
                border: 1px solid #E8EAED;
            }
            QGroupBox {
                color: #2C3E50;
                font-weight: 600;
                border: 2px solid #E8EAED;
                border-radius: 8px;
                margin-top: 10px;
                padding-top: 10px;
            }
            QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 3px 0 3px; }
            QPushButton {
                background-color: #007AFF;
                color: white;
                border: none;
                border-radius: 8px;
                padding: 10px 20px;
                font-weight: 600;
            }
            QPushButton:hover { background-color: #0056CC; }
            QPushButton:pressed { background-color: #003D99; }
            QTabWidget::pane { border: 2px solid #E8EAED; }
            QTabBar::tab {
                background-color: #F5F5F6;
                color: #2C3E50;
                padding: 8px 16px;
                margin-right: 2px;
                border: 1px solid #E8EAED;
            }
            QTabBar::tab:selected {
                background-color: #007AFF;
                color: white;
                border: 1px solid #007AFF;
            }
        """)
        
        # FIXED: Use thread-local connection for UI read
        conn = get_thread_connection(authorized=True)
        try:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM visits WHERE visit_id=?", (self.visit_id,))
            db_row = cursor.fetchone()
        finally:
            return_thread_connection(conn)
        
        class SafeDict(dict):
            def __missing__(self, key):
                return None
        
        row_data = dict(visit)
        if db_row:
            row_data.update(dict(db_row))
        self.row = SafeDict(row_data)
        self.closed_visit = self.row["discharge_time"] not in (None, "", "NULL")
        
        self.init_ui()
    
    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # Header
        header = QFrame()
        header.setStyleSheet("background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #007AFF, stop:1 #0056CC); border-radius: 0px;")
        header.setFixedHeight(60)
        h_layout = QHBoxLayout(header)
        h_layout.setContentsMargins(20, 10, 20, 10)
        
        title = QLabel(f"✏️ تعديل الزيارة #{self.visit_id}")
        title.setStyleSheet("color: white; font-size: 18px; font-weight: bold;")
        status_badge = QLabel("🔒 مغلقة" if self.closed_visit else "🔓 مفتوحة")
        status_badge.setStyleSheet(f"color: white; font-size: 12px; font-weight: bold; padding: 4px 12px; border-radius: 4px; background-color: {'#E74C3C' if self.closed_visit else '#27AE60'};")
        
        h_layout.addWidget(title)
        h_layout.addStretch()
        h_layout.addWidget(status_badge)
        main_layout.addWidget(header)
        
        # Tabs
        tabs = QTabWidget()
        
        # Tab 1: Admission
        admission_widget = QWidget()
        admission_scroll = QScrollArea()
        admission_scroll.setWidget(admission_widget)
        admission_scroll.setWidgetResizable(True)
        admission_scroll.setStyleSheet("QScrollArea { border: none; }")
        
        admission_layout = QVBoxLayout(admission_widget)
        admission_layout.setContentsMargins(30, 30, 30, 30)
        admission_layout.setSpacing(25)
        
        # Basic Information Section
        basic_title = QLabel("📋 بيانات الدخول الأساسية")
        basic_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #007AFF; margin-bottom: 10px;")
        admission_layout.addWidget(basic_title)
        
        grid1 = QGridLayout()
        grid1.setSpacing(15)
        grid1.setColumnStretch(1, 1)
        grid1.setColumnStretch(3, 1)
        
        self.admission_time = QLineEdit(self.row["admission_time"] or "")
        self.admission_time.setMinimumHeight(40)
        self.entry_type = QComboBox()
        self.entry_type.addItems(["عيادات", "طوارئ", "داخلي"])
        self.entry_type.setCurrentText(self.row["entry_type"] or "")
        self.entry_type.setMinimumHeight(40)
        self.department = QLineEdit(self.row["admission_department"] or "")
        self.department.setMinimumHeight(40)
        
        time_label = QLabel("⏰ وقت الدخول:")
        time_label.setStyleSheet("font-weight: 600; font-size: 14px;")
        entry_label = QLabel("📍 مكان الدخول:")
        entry_label.setStyleSheet("font-weight: 600; font-size: 14px;")
        dept_label = QLabel("🏥 القسم:")
        dept_label.setStyleSheet("font-weight: 600; font-size: 14px;")
        
        grid1.addWidget(time_label, 0, 0)
        grid1.addWidget(self.admission_time, 0, 1)
        grid1.addWidget(entry_label, 0, 2)
        grid1.addWidget(self.entry_type, 0, 3)
        
        grid1.addWidget(dept_label, 1, 0)
        grid1.addWidget(self.department, 1, 1, 1, 3)
        
        admission_layout.addLayout(grid1)
        
        # Clinical Information Section
        clinical_title = QLabel("🔍 البيانات السريرية")
        clinical_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #007AFF; margin-top: 15px; margin-bottom: 10px;")
        admission_layout.addWidget(clinical_title)
        
        self.complaint = QLineEdit(self.row["chief_complaint"] or "")
        self.complaint.setObjectName("chief_complaint_input")  # FIXED: Set object name for exclusion logic
        self.complaint.setMinimumHeight(45)
        self.initial_diagnosis = QLineEdit(self.row["initial_diagnosis"] or "")
        self.initial_diagnosis.setObjectName("initial_diagnosis_input")  # FIXED: Set object name for exclusion logic
        self.initial_diagnosis.setMinimumHeight(45)
        
        complaint_label = QLabel("🤒 الشكوى الحالية:")
        complaint_label.setStyleSheet("font-weight: 600; font-size: 14px; margin-bottom: 8px;")
        diagnosis_label = QLabel("🔍 التشخيص المبدئي:")
        diagnosis_label.setStyleSheet("font-weight: 600; font-size: 14px; margin-bottom: 8px;")
        
        admission_layout.addWidget(complaint_label)
        admission_layout.addWidget(self.complaint)
        admission_layout.addWidget(diagnosis_label)
        admission_layout.addWidget(self.initial_diagnosis)
        
        # Attach auto-completion to medical fields
        if 'medical_completer' in globals():
            medical_completer.attach_to_input(self.complaint, 'complaints')
            medical_completer.attach_to_input(self.initial_diagnosis, 'diagnoses')
        
        # Transfer section
        transfer_title = QLabel("🔄 معلومات التحويل للداخلي")
        transfer_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #007AFF; margin-top: 15px; margin-bottom: 10px;")
        admission_layout.addWidget(transfer_title)
        
        transfer_box = QGroupBox("")
        transfer_box.setStyleSheet("QGroupBox { border: 2px solid #E8EAED; border-radius: 8px; padding: 15px; }")
        transfer_layout = QVBoxLayout()
        transfer_layout.setSpacing(12)
        
        self.transfer_checkbox = QCheckBox("✓ تم تحويل المريض للداخلي")
        self.transfer_checkbox.setChecked(bool(self.row["transfer_to_inpatient"]))
        self.transfer_checkbox.setStyleSheet("QCheckBox { font-size: 14px; font-weight: 600; }")
        
        self.transfer_department = QLineEdit(self.row["transfer_department"] or "")
        self.transfer_department.setPlaceholderText("أدخل القسم المحول إليه")
        self.transfer_department.setMinimumHeight(40)
        
        transfer_dept_label = QLabel("🏥 القسم المحول إليه:")
        transfer_dept_label.setStyleSheet("font-weight: 600; font-size: 14px;")
        
        transfer_layout.addWidget(self.transfer_checkbox)
        transfer_layout.addSpacing(10)
        transfer_layout.addWidget(transfer_dept_label)
        transfer_layout.addWidget(self.transfer_department)
        transfer_box.setLayout(transfer_layout)
        
        admission_layout.addWidget(transfer_box)
        admission_layout.addStretch()
        tabs.addTab(admission_scroll, "📥 بيانات الدخول")
        
        # Tab 2: Discharge
        discharge_widget = QWidget()
        discharge_scroll = QScrollArea()
        discharge_scroll.setWidget(discharge_widget)
        discharge_scroll.setWidgetResizable(True)
        discharge_scroll.setStyleSheet("QScrollArea { border: none; }")
        
        discharge_layout = QVBoxLayout(discharge_widget)
        discharge_layout.setContentsMargins(30, 30, 30, 30)
        discharge_layout.setSpacing(25)
        
        # Discharge Status Section
        status_title = QLabel("📋 بيانات الخروج")
        status_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #007AFF; margin-bottom: 10px;")
        discharge_layout.addWidget(status_title)
        
        grid2 = QGridLayout()
        grid2.setSpacing(15)
        grid2.setColumnStretch(1, 1)
        grid2.setColumnStretch(3, 1)
        
        self.discharge_status = QComboBox()
        self.discharge_status.addItems(["", "خروج تحسن ✨", "خروج حسب الطلب 📄", "وفاة 🕯️", "تحويل لمستشفى 🚑"])
        self.discharge_status.setCurrentText(self.row["discharge_status"] or "")
        self.discharge_status.setMinimumHeight(40)
        
        self.discharge_time = QLineEdit(self.row["discharge_time"] or "")
        self.discharge_time.setMinimumHeight(40)
        
        self.discharge_doctor = QLineEdit(self.row["discharge_doctor"] or "")
        self.discharge_doctor.setMinimumHeight(40)
        
        status_label = QLabel("📋 حالة الخروج:")
        status_label.setStyleSheet("font-weight: 600; font-size: 14px;")
        time_label2 = QLabel("⏰ وقت الخروج:")
        time_label2.setStyleSheet("font-weight: 600; font-size: 14px;")
        doctor_label = QLabel("👨‍⚕️ اسم الطبيب:")
        doctor_label.setStyleSheet("font-weight: 600; font-size: 14px;")
        
        grid2.addWidget(status_label, 0, 0)
        grid2.addWidget(self.discharge_status, 0, 1)
        grid2.addWidget(time_label2, 0, 2)
        grid2.addWidget(self.discharge_time, 0, 3)
        
        grid2.addWidget(doctor_label, 1, 0)
        grid2.addWidget(self.discharge_doctor, 1, 1, 1, 3)
        
        discharge_layout.addLayout(grid2)
        
        # Diagnosis Section
        diagnosis_title = QLabel("🔍 التشخيص والخيارات الطبية")
        diagnosis_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #007AFF; margin-top: 15px; margin-bottom: 10px;")
        discharge_layout.addWidget(diagnosis_title)
        
        self.final_diagnosis = QLineEdit(self.row["final_diagnosis"] or "")
        self.final_diagnosis.setObjectName("final_diagnosis_input")  # FIXED: Set object name for exclusion logic
        self.final_diagnosis.setMinimumHeight(45)
        self.final_diagnosis.setPlaceholderText("أدخل التشخيص النهائي للمريض")
        
        final_diagnosis_label = QLabel("🔍 التشخيص النهائي:")
        final_diagnosis_label.setStyleSheet("font-weight: 600; font-size: 14px; margin-bottom: 8px;")
        
        discharge_layout.addWidget(final_diagnosis_label)
        discharge_layout.addWidget(self.final_diagnosis)
        
        # Attach auto-completion to diagnosis field
        if 'medical_completer' in globals():
            medical_completer.attach_to_input(self.final_diagnosis, 'diagnoses')
        
        # Transfer Hospital Section
        self.transfer_hospital = QLineEdit(self.row["transfer_hospital"] or "")
        self.transfer_hospital.setMinimumHeight(40)
        self.transfer_hospital.setPlaceholderText("أدخل اسم المستشفى صإذا لزم الأمر")
        
        transfer_hosp_label = QLabel("🚑 المستشفى المحول إليها:")
        transfer_hosp_label.setStyleSheet("font-weight: 600; font-size: 14px; margin-bottom: 8px;")
        
        discharge_layout.addWidget(transfer_hosp_label)
        discharge_layout.addWidget(self.transfer_hospital)
        
        # Surgery section
        surgery_title = QLabel("✂️ العمليات الجراحية")
        surgery_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #007AFF; margin-top: 15px; margin-bottom: 10px;")
        discharge_layout.addWidget(surgery_title)
        
        surgery_box = QGroupBox("")
        surgery_box.setStyleSheet("QGroupBox { border: 2px solid #E8EAED; border-radius: 8px; padding: 15px; }")
        surgery_layout = QVBoxLayout()
        surgery_layout.setSpacing(12)
        
        self.surgery = QCheckBox("✂️ تم إجراء عملية")
        self.surgery.setChecked(bool(self.row["surgery_performed"]))
        self.surgery.setStyleSheet("QCheckBox { font-size: 14px; font-weight: 600; }")
        
        self.surgery_name = QLineEdit(self.row["surgery_name"] or "")
        self.surgery_name.setPlaceholderText("أدخل اسم العملية الجراحية")
        self.surgery_name.setMinimumHeight(40)
        
        surgery_name_label = QLabel("📝 اسم العملية:")
        surgery_name_label.setStyleSheet("font-weight: 600; font-size: 14px;")
        
        surgery_layout.addWidget(self.surgery)
        surgery_layout.addSpacing(10)
        surgery_layout.addWidget(surgery_name_label)
        surgery_layout.addWidget(self.surgery_name)
        surgery_box.setLayout(surgery_layout)
        
        discharge_layout.addWidget(surgery_box)
        discharge_layout.addStretch()
        tabs.addTab(discharge_scroll, "📤 بيانات الخروج")
        
        main_layout.addWidget(tabs)
        
        # Footer with buttons
        footer = QFrame()
        footer.setStyleSheet("background-color: #FFFFFF; border-top: 1px solid #E8EAED;")
        footer_layout = QHBoxLayout(footer)
        footer_layout.setContentsMargins(20, 15, 20, 15)
        
        save_admission_btn = QPushButton("💾 حفظ الدخول")
        save_admission_btn.setMinimumWidth(150)
        save_all_btn = QPushButton("✅ حفظ وغلق الزيارة")
        save_all_btn.setMinimumWidth(150)
        cancel_btn = QPushButton("❌ إلغاء")
        cancel_btn.setStyleSheet("""
            QPushButton {
                background-color: #FFFFFF;
                color: #2C3E50;
                border: 2px solid #E8EAED;
                border-radius: 8px;
                padding: 10px 20px;
                font-weight: 600;
            }
            QPushButton:hover { background-color: #F5F5F6; border-color: #2C3E50; }
        """)
        cancel_btn.setMinimumWidth(120)
        
        footer_layout.addStretch()
        footer_layout.addWidget(save_admission_btn)
        footer_layout.addWidget(save_all_btn)
        footer_layout.addWidget(cancel_btn)
        main_layout.addWidget(footer)
        
        # Connections
        save_admission_btn.clicked.connect(self.save_admission)
        save_all_btn.clicked.connect(self.close_visit)
        cancel_btn.clicked.connect(self.reject)
        
        # منع التعديل بعد الغلق
        if self.closed_visit:
            for widget in self.findChildren((QLineEdit, QComboBox, QCheckBox, QPushButton)):
                if widget not in [cancel_btn]:
                    widget.setDisabled(True)
            QMessageBox.information(self, "تنبيه", "🔒 هذه الزيارة مغلقة ولا يمكن تعديلها")
    
    def save_admission(self):
        """FIXED: Use write queue for thread-safe operation instead of direct SQLite write"""
        
        # Prepare update data
        update_data = {
            'admission_time': self.admission_time.text(),
            'entry_type': self.entry_type.currentText(),
            'transfer_to_inpatient': 1 if self.transfer_checkbox.isChecked() else 0,
            'transfer_department': self.transfer_department.text(),
            'admission_department': self.department.text(),
            'chief_complaint': self.complaint.text(),
            'initial_diagnosis': self.initial_diagnosis.text(),
            'updated_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'visit_id': self.visit_id
        }
        
        # FIXED: Enqueue operation to write queue instead of direct write
        def _update_admission_operation(cursor, conn):
            cursor.execute("""
                UPDATE visits
                SET admission_time=?, entry_type=?, transfer_to_inpatient=?,
                    transfer_department=?, admission_department=?, chief_complaint=?,
                    initial_diagnosis=?, updated_at=?
                WHERE visit_id=?
            """, (
                update_data['admission_time'], update_data['entry_type'],
                update_data['transfer_to_inpatient'],
                update_data['transfer_department'], update_data['admission_department'],
                update_data['chief_complaint'], update_data['initial_diagnosis'],
                update_data['updated_at'], update_data['visit_id']
            ))
        
        # Add to write queue with explicit db_type
        write_queue.put((_update_admission_operation, (), {"db_type": "sqlite"}))
        
        QMessageBox.information(self, "✅ نجح", "تم حفظ ملخص الدخول بنجاح")
    
    def close_visit(self):
        """FIXED: Use write queue for thread-safe operation instead of direct SQLite write"""
        
        if not self.discharge_status.currentText():
            confirm = QMessageBox.question(self, "⚠️ تنبيه", 
                "لم يتم كتابة ملخص الخروج.  هل أنت متأكد من غلق الزيارة؟")
            if confirm != QMessageBox.Yes:
                return
        
        # Prepare update data
        update_data = {
            'discharge_status': self.discharge_status.currentText(),
            'discharge_time': self.discharge_time.text(),
            'discharge_doctor': self.discharge_doctor.text(),
            'final_diagnosis': self.final_diagnosis.text(),
            'surgery_performed': 1 if self.surgery.isChecked() else 0,
            'surgery_name': self.surgery_name.text(),
            'transfer_hospital': self.transfer_hospital.text(),
            'updated_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'visit_id': self.visit_id
        }
        
        # FIXED: Enqueue operation to write queue instead of direct write
        def _close_visit_operation(cursor, conn):
            cursor.execute("""
                UPDATE visits
                SET discharge_status=?, discharge_time=?, discharge_doctor=?,
                    final_diagnosis=?, surgery_performed=?, surgery_name=?,
                    transfer_hospital=?, updated_at=?
                WHERE visit_id=?
            """, (
                update_data['discharge_status'], update_data['discharge_time'],
                update_data['discharge_doctor'], update_data['final_diagnosis'],
                update_data['surgery_performed'], update_data['surgery_name'],
                update_data['transfer_hospital'], update_data['updated_at'],
                update_data['visit_id']
            ))
        
        # Add to write queue with explicit db_type
        write_queue.put((_close_visit_operation, (), {"db_type": "sqlite"}))
        
        QMessageBox.information(self, "✅ نجح", "تم غلق الزيارة بنجاح")
        self.accept()


# ==========================================
# Utility Classes for Optimizations
# ==========================================

class PasswordHasher:
    # تجزئة وتحقق كلمات المرور باستخدام bcrypt
    
    @staticmethod
    def hash_password(password: str) -> str:
        # تحويل كلمة المرور إلى hash - rounds=10 (آمن وأسرع 4x من 12)
        try:
            salt = bcrypt.gensalt(rounds=10)  # 10 rounds: آمن جداً وأسرع بكثير من 12
            return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
        except Exception as e:
            error_logger.error(f"Password hashing error: {e}")
            return None
    
    @staticmethod
    def verify_password(password: str, hashed: str) -> bool:
        try:
            if not hashed:
                return False

            # Security improvement: Force bcrypt only, no plain text fallback
            if not hashed.startswith("$2"):
                return False

            return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))
        except Exception as e:
            error_logger.error(f"Password verification error: {e}")
            return False


class CacheLayer:
    # طبقة كاش مع TTL و LRU
    
    def __init__(self, max_size: int = 1000, ttl: int = 60):
        self.cache = OrderedDict()
        self.max_size = max_size
        self.ttl = ttl  # في الثواني
        self.timestamps = {}
        self._lock = threading.RLock()
    
    def get(self, key: str):
        # الحصول على القيمة من الكاش إذا لم تنته مدة صلاحيتها
        with self._lock:
            if key not in self.cache:
                return None
            
            # التحقق من انتهاء صلاحية المدخل
            if time.time() - self.timestamps[key] > self.ttl:
                del self.cache[key]
                del self.timestamps[key]
                return None
            
            # تحديث موضع العنصر (LRU)
            self.cache.move_to_end(key)
            return self.cache[key]
    
    def set(self, key: str, value):
        # إضافة قيمة إلى الكاش
        with self._lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            else:
                if len(self.cache) >= self.max_size:
                    oldest = next(iter(self.cache))
                    del self.cache[oldest]
                    del self.timestamps[oldest]
            
            self.cache[key] = value
            self.timestamps[key] = time.time()
    
    def invalidate(self, key: str = None):
        # حذف المدخل أو مسح الكاش بالكامل
        with self._lock:
            if key:
                self.cache.pop(key, None)
                self.timestamps.pop(key, None)
            else:
                self.cache.clear()
                self.timestamps.clear()

    def purge_expired(self):
        # تنظيف دوري للعناصر المنتهية الصلاحية
        with self._lock:
            now = time.time()
            keys_to_remove = [k for k, t in self.timestamps.items() if now - t > self.ttl]
            for k in keys_to_remove:
                self.cache.pop(k, None)
                self.timestamps.pop(k, None)


class SyncQueueManager:
    # مدير طابور المزامنة مع retry logic و locking
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        self.backoff_delays = [2, 5, 10, 30]  # exponential backoff بالثواني
    
    def add_item(self, action: str, table_name: str, record_id: str, payload: dict) -> bool:
        # إضافة عملية إلى طابور المزامنة
        with sqlite_write_lock:
            try:
                conn = sqlite_manager.get_connection(authorized=True)
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO sync_queue 
                    (action, table_name, record_id, payload, status, retries, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (action, table_name, record_id, safe_json_dumps(payload), 'pending', 0,
                      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                      datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                conn.commit()
                sqlite_manager.return_connection(conn)
                # Improvement #3: Add structured sync logging
                sync_logger.info("Added to sync queue: action=%s, table=%s, record_id=%s", action, table_name, record_id)
                
                # تحديث لوحة التحكم/الواجهة (Fix 5)
                # ملاحظة: trigger_sync يتم استدعاؤه هنا لتنبيه الواجهة بوجود عمل جديد في الطابور
                # سيتم استخدام logic داخلي لتجنب الـ infinite loop
                return True
            except Exception as e:
                error_logger.error(f"Error adding to sync queue: {e}")
                return False
    
    def get_pending_items(self, limit: int = 200) -> list:
        """الحصول على العمليات المعلقة للمعالجة"""
        try:
            conn = sqlite_manager.get_connection(authorized=True)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM sync_queue 
                WHERE status IN ('pending', 'failed')
                AND retries < 5
                ORDER BY created_at ASC
                LIMIT ?
            """, (limit,))
            items = [dict(row) for row in cursor.fetchall()]
            sqlite_manager.return_connection(conn)
            return items
        except Exception as e:
            error_logger.error(f"Error getting pending sync items: {e}")
            return []
    
    def mark_processing(self, item_id: int) -> bool:
        """تحديث حالة العملية إلى processing"""
        with sqlite_write_lock:
            try:
                conn = sqlite_manager.get_connection(authorized=True)
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE sync_queue 
                    SET status = 'processing', updated_at = ?
                    WHERE id = ?
                """, (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), item_id))
                conn.commit()
                sqlite_manager.return_connection(conn)
                return True
            except Exception as e:
                error_logger.error(f"Error marking as processing: {e}")
                return False
    
    def mark_success(self, item_id: int) -> bool:
        """تحديث حالة العملية إلى done
        
        FIX #3: تطبيق sqlite_write_lock لضمان عدم حدوث race conditions
        """
        with sqlite_write_lock:
            try:
                conn = sqlite_manager.get_connection(authorized=True)
                cursor = conn.cursor()
                # Get action and record_id before update for logging
                cursor.execute("SELECT action, record_id FROM sync_queue WHERE id = ?", (item_id,))
                row = cursor.fetchone()
                action, record_id = row[0], row[1] if row else (None, None)
                
                cursor.execute("""
                    UPDATE sync_queue 
                    SET status = 'done', updated_at = ?
                    WHERE id = ?
                """, (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), item_id))
                conn.commit()
                sqlite_manager.return_connection(conn)
                # Improvement #10: Add structured sync success logging
                sync_logger.info("Sync success: action=%s, record_id=%s, item_id=%d", action, record_id, item_id)
                return True
            except Exception as e:
                error_logger.error(f"Error marking as done: {e}")
                return False
    
    def mark_failed(self, item_id: int, error: str = None) -> bool:
        """تحديث حالة العملية وإضافة محاولة جديدة أو تحديثها إلى failed"""
        # FIX #3: تطبيق sqlite_write_lock لضمان عدم حدوث race conditions
        conn = None
        with sqlite_write_lock:
            try:
                conn = sqlite_manager.get_connection(authorized=True)
                cursor = conn.cursor()
                
                # الحصول على عدد المحاولات الحالية
                cursor.execute("SELECT retries, action, record_id FROM sync_queue WHERE id = ?", (item_id,))
                row = cursor.fetchone()
                if not row:
                    sqlite_manager.return_connection(conn)
                    return False
                
                retries = row[0] + 1
                action, record_id = row[1], row[2]
                # FIX #7: Mark as 'dead' after 5 retries (no more retries)
                new_status = 'dead' if retries >= 5 else 'pending'
                
                cursor.execute("""
                    UPDATE sync_queue 
                    SET status = ?, retries = ?, last_error = ?, updated_at = ?
                    WHERE id = ?
                """, (new_status, retries, error or '', datetime.now().strftime("%Y-%m-%d %H:%M:%S"), item_id))
                
                conn.commit()
                # Improvement #3: Add structured sync failure logging
                if new_status == 'dead':  # FIX #7: Dead items need manual intervention
                    error_logger.critical("💀 Sync marked as DEAD (max retries reached): action=%s, record_id=%s, item_id=%d, error=%s", 
                                      action, record_id, item_id, error)
                elif new_status == 'failed':
                    error_logger.error("Sync permanently failed: action=%s, record_id=%s, item_id=%d, error=%s", 
                                  action, record_id, item_id, error)
                else:
                    sync_logger.warning("Sync retry: action=%s, record_id=%s, item_id=%d, retry=%d/5, error=%s",
                                   action, record_id, item_id, retries, error)
                return True
            except Exception as e:
                error_logger.error(f"Error marking as failed: {e}")
                return False
            finally:
                if conn:
                    sqlite_manager.return_connection(conn)
    
    def get_backoff_delay(self, retry_count: int) -> int:
        """الحصول على مدة الانتظار قبل إعادة المحاولة (exponential backoff)"""
        if retry_count < len(self.backoff_delays):
            return self.backoff_delays[retry_count]
        return self.backoff_delays[-1]

    def cleanup_stuck_processing(self):
        """إعادة تعيين العمليات العالقة في حالة جاري المعالجة"""
        # FIX #3: تطبيق sqlite_write_lock لضمان عدم حدوث race conditions
        conn = None
        with sqlite_write_lock:
            try:
                conn = sqlite_manager.get_connection(authorized=True)
                cursor = conn.cursor()
    
                cursor.execute("""
                    UPDATE sync_queue
                    SET status='pending'
                    WHERE status='processing'
                    AND updated_at < datetime('now','-5 minutes')
                """)
    
                conn.commit()
            except Exception as e:
                error_logger.error(f"Error in cleanup_stuck_processing: {e}")
                if conn:
                    try:
                        conn.rollback()
                    except Exception as er:
                        error_logger.debug(f"Rollback exception (ignored): {er}")
            finally:
                if conn:
                    sqlite_manager.return_connection(conn)


class DatabaseManager:
    """مدير قاعدة البيانات الهجين (Online/Offline)"""
    
    # Maximum number of pages to cache (to prevent memory leaks)
    _MAX_PAGE_CACHE_SIZE = 100
    
    def __init__(self):
        self._page_cache = {}
        self._online_status = None
        self._last_online_check = 0
        self._online_status_cached = None
        self._last_online_check_time = 0.0
        
        # Initialize utility layers
        self.cache = CacheLayer(max_size=1000, ttl=300)  # Cache with 300 second TTL
        
        # CRITICAL: Initialize new thread-safe SQLite manager first
        try:
            self.sqlite_manager = initialize_sqlite_manager(LOCAL_DB_PATH)
            system_logger.info("✅ ThreadSafeSQLiteManager initialized")
        except Exception as e:
            error_logger.critical(f"❌ Failed to initialize SQLite manager: {e}")
            raise
        
        # CRITICAL: Initialize centralized write queue
        try:
            self.write_queue_manager = initialize_write_queue(self.sqlite_manager)
            system_logger.info("✅ WriteQueueManager initialized and worker started")
        except Exception as e:
            error_logger.critical(f"❌ Failed to initialize write queue: {e}")
            raise
        
        # Keep old sync_queue_manager for backward compatibility during migration
        self.sync_queue_manager = SyncQueueManager(LOCAL_DB_PATH)
        
        # Initialize refactored sync manager for proper offline-first sync
        try:
            self.refactored_sync_manager = initialize_refactored_sync(
                self.sqlite_manager, 
                self.sync_queue_manager
            )
            system_logger.info("✅ RefactoredSyncManager initialized")
        except Exception as e:
            error_logger.critical(f"❌ Failed to initialize sync manager: {e}")
            raise
        
        self._sync_lock = threading.Lock()  # Prevent concurrent sync workers
        # Improvement #1: Add lock for patient ID generation to prevent race conditions
        self._id_lock = threading.Lock()  # Protect patient ID generation
        self._integrity_checked = False
        
        # Improvement #6: Debounce mechanism for trigger_sync
        self._debounce_timers = {}  # {key: (timer, pending_args)}
        self._debounce_lock = threading.Lock()
        
        # start background cleaner for cache
        threading.Thread(target=self._cache_cleaner, daemon=True).start()
        
        # CRITICAL: Initialize SQLite tables and default users
        self.init_sqlite()
        
        # تشغيل التهيئة الثقيلة (PostgreSQL + سلامة النظام) في خيط خلفي
        # حتى تظهر واجهة المستخدم فوراً بدون انتظار
        threading.Thread(target=self._init_heavy_resources, daemon=True).start()
        
        # تشغيل خيط المزامنة في الخلفية (Background Sync Worker)
        self.sync_thread = threading.Thread(target=self._background_sync_worker, name="SyncWorker", daemon=True)
        self.sync_thread.start()
        system_state.active_threads.append(self.sync_thread) # FIX: Track for shutdown

    def _init_heavy_resources(self):
        """تهيئة الموارد الثقيلة في خيط خلفي لتسريع إقلاع الواجهة"""
        
        # انتظار Pool إذا كان لا يزال يتصل
        _pool_ready.wait(timeout=10)
        
        # FIX #4: Run comprehensive health check
        try:
            perform_health_check()
            system_logger.info(f"✅ Health check completed: system_state.mode={system_state.mode}")
            
            # FIXED: Start write queue worker AFTER health check for deterministic startup
            write_queue_manager.start_worker()
            system_logger.info("✅ Write queue worker started (deterministic startup)")
            
            # FIX: Update status indicators after health check
            try:
                from PySide6.QtWidgets import QApplication
                # Note: This is called from background thread, so we need to use signals/slots or QMetaObject.invokeMethod
                # For now, the status bar will be updated in the main UI thread when needed
            except Exception as e:
                error_logger.error(f"Error updating status indicators from health check: {e}")
        except Exception as e:
            error_logger.critical(f"Health check failed: {e}")
            system_state.mode = "OFFLINE"
        
        # FIX #3: Test PostgreSQL connection at startup
        if _DB_CONFIG and _DB_CONFIG.get('password') != 'your_password_here':
            try:
                self.test_postgresql_connection()
                system_logger.info("✅ PostgreSQL connection test PASSED")
            except Exception as e:
                error_logger.critical(f"❌ PostgreSQL connection test FAILED: {e}")
                system_logger.warning("⚠️  Running in OFFLINE mode only (PostgreSQL unreachable)")
        else:
            system_logger.warning("⚠️  Skipping PostgreSQL test (placeholder password detected)")
        self.init_postgresql()
        start_pg_keepalive()
        # التحقق من سلامة النظام في الخلفية
        try:
            if not self._integrity_checked:
                if not self.verify_system_integrity():
                    system_logger.warning("⚠️ تحذير: تم اكتشاف مشاكل في سلامة النظام")
                else:
                    system_logger.info("✅ سلامة النظام: تم التحقق بنجاح")
                self._integrity_checked = True
        except Exception as e:
            error_logger.error(f"System integrity check error: {e}")

    def _background_sync_worker(self):
        """خيط يعمل في الخلفية للمزامنة التلقائية مع adaptive waiting"""
        last_retry_times = {}
        _loop_counter = 0  # عداد للتحكم في عمليات الصيانة الدورية
        
        # انتظر قليلاً حتى تكتمل التهيئة الأولية
        _pool_ready.wait(timeout=15)
        
        while system_state.running:  # FIX #3: Use system_state.running flag for graceful shutdown
            try:
                self.sync_queue_manager.cleanup_stuck_processing()
                _loop_counter += 1

                # CRITICAL: Use refactored sync manager with system_state.mode enforcement
                # Only sync in ONLINE mode with PostgreSQL available
                if system_state.mode == "ONLINE" and self.is_online() and self._sync_lock.acquire(blocking=False):
                    try:
                        # Use refactored sync manager instead of old process_sync_queue_with_retries
                        items_processed = self.refactored_sync_manager.process_sync_queue(
                            system_mode=system_state.mode,
                            is_online=self.is_online()
                        )
                        # Adaptive wait: shorter interval if items were processed
                        # Speed up: 1s if busy, 5s if idle
                        wait_time = 1 if items_processed > 0 else 5
                    finally:
                        self._sync_lock.release()
                else:
                    # Skip sync in OFFLINE or CRITICAL mode
                    if system_state.mode != "ONLINE":
                        sync_logger.debug(f"Sync skipped in {system_state.mode} mode")
                    wait_time = 5
            except Exception as e:
                error_logger.error(f"Sync worker crash prevented: {e}")
                wait_time = 5
            
            # فحص حجم الطابور كل 10 دورات فقط (بدلاً من كل دورة) لتقليل I/O
            if _loop_counter % 10 == 0:
                try:
                    conn = sqlite_manager.get_connection(authorized=True)
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM sync_queue WHERE status IN ('pending', 'processing', 'failed')")
                    queue_size = cursor.fetchone()[0]
                    sqlite_manager.return_connection(conn)
                    
                    # تحسين #10: تحذير إذا كان الطابور كبير جداً
                    if queue_size > 5000:
                        error_logger.warning(f"⚠️ Sync queue is very large: {queue_size} items pending. Consider cleanup.")
                        # تنظيف تلقائي للعناصر القديمة المفشلة
                        if queue_size > 10000:
                            error_logger.critical(f"🚨 Sync queue critical size: {queue_size} items. Auto-cleanup initiated.")
                            self._emergency_cleanup_sync_queue()
                except Exception as e:
                    error_logger.error(f"Error checking sync queue size: {e}")
            
            # تنظيف عناصر الطابور المنجزة الأقدم من 7 أيام كل 20 دورة لتقليل الحجم
            if _loop_counter % 20 == 0:
                with sqlite_write_lock:
                    try:
                        conn = sqlite_manager.get_connection(authorized=True)
                        conn.execute("""
                            DELETE FROM sync_queue
                            WHERE status='done'
                            AND created_at < datetime('now','-7 days')
                        """)
                        conn.commit()
                        sqlite_manager.return_connection(conn)
                    except Exception as e:
                        error_logger.error(f"Error cleaning old sync_queue records: {e}")
            
            # FIX #6: تنظيف عناصر الطابور المفشلة والقديمة جداً كل 50 دورة
            if _loop_counter % 50 == 0:
                with sqlite_write_lock:
                    try:
                        conn = sqlite_manager.get_connection(authorized=True)
                        # Delete permanently failed items older than 3 days
                        conn.execute("""
                            DELETE FROM sync_queue
                            WHERE status='failed'
                            AND retries >= 5
                            AND created_at < datetime('now','-3 days')
                        """)
                        deleted = conn.total_changes
                        conn.commit()
                        sqlite_manager.return_connection(conn)
                        if deleted > 0:
                            sync_logger.info(f"Cleaned up {deleted} permanently failed sync items")
                    except Exception as e:
                        error_logger.error(f"Error cleaning failed sync_queue items: {e}")
    
    def _emergency_cleanup_sync_queue(self):
        """تنظيف طوارئ للطابور عندما يتجاوز الحد الحرج"""
        with sqlite_write_lock:
            try:
                conn = sqlite_manager.get_connection(authorized=True)
                cursor = conn.cursor()
                
                # حذف العناصر المفشلة القديمة (> 1 يوم)
                cursor.execute("""
                    DELETE FROM sync_queue
                    WHERE status='failed'
                    AND retries >= 5
                    AND created_at < datetime('now','-1 day')
                """)
                deleted_failed = cursor.rowcount
                
                # حذف العناصر المنجزة القديمة (> 7 أيام)
                cursor.execute("""
                    DELETE FROM sync_queue
                    WHERE status='done'
                    AND created_at < datetime('now','-7 days')
                """)
                deleted_done = cursor.rowcount
                
                conn.commit()
                sqlite_manager.return_connection(conn)
                
                total_deleted = deleted_failed + deleted_done
                if total_deleted > 0:
                    error_logger.warning(f"🧹 Emergency cleanup completed: {total_deleted} items removed ({deleted_failed} failed, {deleted_done} done)")
            except Exception as e:
                error_logger.error(f"Emergency cleanup failed: {e}")

            # الانتظار الديناميكي (Dynamic/Adaptive waiting)
            time.sleep(wait_time)
    

    def test_postgresql_connection(self):
        """
        FIX #3: Test PostgreSQL connection at startup (fail-fast)
        Raises exception if connection fails
        """
        if connection_pool is None:
            raise Exception("PostgreSQL connection pool not initialized")
        
        try:
            conn = connection_pool.getconn()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                connection_pool.putconn(conn)
                system_logger.info("PostgreSQL test connection successful")
            else:
                raise Exception("Could not get connection from pool")
        except Exception as e:
            error_logger.error(f"PostgreSQL connection test failed: {e}")
            raise
    
    def is_online(self):
        """
        التحقق المتوازن من الاتصال (مع كاش لتجنب الضغط)
        """
        now = time.time()
        # استخدام كاش لمدة 10 ثوانٍ لعمليات الفحص المتكررة
        if hasattr(self, '_online_status_cached') and (now - self._last_online_check_time < 10):
            return self._online_status_cached

        status = self._perform_online_check()
        self._online_status_cached = status
        self._last_online_check_time = now
        return status

    def _perform_online_check(self):
        """التحقق الفعلي من الاتصال بالإنترنت وPostgreSQL"""
        import socket
        try:
            # محاولة سريعة لـ Google DNS
            socket.create_connection(("8.8.8.8", 53), timeout=2)
            internet_available = True
        except (socket.timeout, socket.error, OSError):
            internet_available = False
        
        if not internet_available:
            return False
            
        if connection_pool is None:
            return False
            
        try:
            conn = connection_pool.getconn()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                connection_pool.putconn(conn)
                return True
        except Exception:
            pass
        return False
    
    def check_internet_connection(self, timeout=5):
        """
        التحقق من الاتصال بالإنترنت فقط (بدون فحص PostgreSQL)
        Returns True if online, False otherwise
        """
        import socket
        
        try:
            # محاولة الاتصال بـ Google DNS
            socket.create_connection(("8.8.8.8", 53), timeout=timeout)
            return True
        except (socket.timeout, socket.error, OSError) as e:
            error_logger.debug(f"Google DNS unreachable (test_internet_connection): {e}")
            pass
        
        try:
            # محاولة بديلة: Cloudflare DNS
            socket.create_connection(("1.1.1.1", 53), timeout=timeout)
            return True
        except (socket.timeout, socket.error, OSError) as e:
            error_logger.debug(f"Cloudflare DNS unreachable (test_internet_connection): {e}")
            pass
        
        return False
    
    def close_all_connections(self):
        """
        FIX #5: Close all PostgreSQL connections gracefully
        """
        global connection_pool
        if connection_pool is not None:
            try:
                connection_pool.closeall()
                system_logger.info("PostgreSQL connection pool closed")
            except Exception as e:
                error_logger.error(f"Error closing PostgreSQL pool: {e}")

    def verify_system_integrity(self):
        """التحقق من سلامة النظام والتأكد من عدم وجود أي بقايا من النظام القديم
        Improvement #8: Enhanced with UNIQUE constraint verification"""
        warnings = []
        errors = []
        conn_pg = None
        
        # فحص SQLite
        try:
            conn = sqlite_manager.get_connection(authorized=True)
            cursor = conn.cursor()
            
            # التحقق من أن patients لا يحتوي على entry_type
            cursor.execute("PRAGMA table_info(patients)")
            patients_cols = [col[1] for col in cursor.fetchall()]
            
            if 'entry_type' in patients_cols:
                errors.append("❌ خطأ: جدول patients يحتوي على عمود entry_type - يجب إزالته!")
            
            # التحقق من أن visits يحتوي على entry_type
            cursor.execute("PRAGMA table_info(visits)")
            visits_cols = [col[1] for col in cursor.fetchall()]
            
            if 'entry_type' not in visits_cols:
                errors.append("❌ خطأ: جدول visits لا يحتوي على عمود entry_type - مطلوب!")
            
            # التحقق من الأعمدة الأساسية
            required_patients_cols = ['patient_id', 'name', 'national_id', 'birth_date', 'phone', 'created_at']
            for col in required_patients_cols:
                if col not in patients_cols:
                    errors.append(f"❌ خطأ: جدول patients لا يحتوي على العمود المطلوب: {col}")
            
            required_visits_cols = ['visit_id', 'patient_id', 'hospital_code', 'entry_type', 'admission_time']
            for col in required_visits_cols:
                if col not in visits_cols:
                    errors.append(f"❌ خطأ: جدول visits لا يحتوي على العمود المطلوب: {col}")
            
            # Improvement #8: Check UNIQUE constraints
            # Check for duplicate national_ids (must be unique)
            cursor.execute("""
                SELECT national_id, COUNT(*) as cnt 
                FROM patients 
                WHERE national_id IS NOT NULL 
                GROUP BY national_id 
                HAVING cnt > 1 
                LIMIT 10
            """)
            duplicates = cursor.fetchall()
            if duplicates:
                dup_ids = [d[0] for d in duplicates]
                warnings.append(f"⚠️ تحذير: وجدنا {len(duplicates)} أرقام قومية مكررة: {', '.join(dup_ids[:3])}...")
            
            # Check for duplicate (patient_id, admission_time) in visits
            cursor.execute("""
                SELECT patient_id, admission_time, COUNT(*) as cnt 
                FROM visits 
                WHERE patient_id IS NOT NULL AND admission_time IS NOT NULL
                GROUP BY patient_id, admission_time 
                HAVING cnt > 1 
                LIMIT 10
            """)
            visit_duplicates = cursor.fetchall()
            if visit_duplicates:
                warnings.append(f"⚠️ تحذير: وجدنا {len(visit_duplicates)} بيانات زيارة مكررة (patient_id, admission_time)")
            
            # Improvement #10: Verify indexes exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='patients'")
            patient_indexes = [row[0] for row in cursor.fetchall()]
            required_patient_indexes = ['idx_patients_national_id', 'idx_patients_patient_id']
            for idx in required_patient_indexes:
                if idx not in patient_indexes:
                    # Create missing index
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx} ON patients({idx.replace('idx_patients_', '')})")
                    system_logger.info(f"Created missing index: {idx}")
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='visits'")
            visit_indexes = [row[0] for row in cursor.fetchall()]
            required_visit_indexes = ['idx_visits_patient_id', 'idx_visits_admission_time']
            for idx in required_visit_indexes:
                if idx not in visit_indexes:
                    # Create missing index
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx} ON visits({idx.replace('idx_visits_', '')})")
                    system_logger.info(f"Created missing index: {idx}")
            
            sqlite_manager.return_connection(conn)

            # التحقق من تداخل نطاقات الأجهزة داخل نفس المستشفى في SQLite
            try:
                conn_ranges = sqlite_manager.get_connection(authorized=True)
                cur_ranges = conn_ranges.cursor()
                cur_ranges.execute("""
                    SELECT hospital_code, device_id, range_start, range_end
                    FROM device_config
                    WHERE hospital_code IS NOT NULL
                """)
                rows = cur_ranges.fetchall()
                sqlite_manager.return_connection(conn_ranges)

                by_hospital = {}
                for hospital_code, device_id, r_start, r_end in rows:
                    by_hospital.setdefault(hospital_code, []).append(
                        (device_id, int(r_start or 0), int(r_end or 0))
                    )

                for hosp, ranges in by_hospital.items():
                    ranges_sorted = sorted(ranges, key=lambda x: x[1])
                    for i in range(len(ranges_sorted) - 1):
                        dev_a, a_start, a_end = ranges_sorted[i]
                        dev_b, b_start, b_end = ranges_sorted[i + 1]
                        # overlap if startB <= endA
                        if b_start <= a_end:
                            msg = (
                                f"❌ خطأ حرج: تداخل في نطاق device_config للمستشفى {hosp}: "
                                f"{dev_a} [{a_start}-{a_end}] مع {dev_b} [{b_start}-{b_end}]"
                            )
                            errors.append(msg)
                            error_logger.critical(msg)
                            break
            except Exception as e:
                error_logger.error(f"خطأ في فحص نطاقات الأجهزة device_config: {e}")

            # فحص PostgreSQL لنفس قواعد entry_type
            try:
                conn_pg = get_connection()
                if conn_pg:
                    cursor_pg = conn_pg.cursor()
                    cursor_pg.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = 'patients'
                    """)
                    pg_patients_cols = [r[0] for r in cursor_pg.fetchall()]

                    cursor_pg.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = 'visits'
                    """)
                    pg_visits_cols = [r[0] for r in cursor_pg.fetchall()]

                    if 'entry_type' in pg_patients_cols:
                        errors.append("❌ خطأ: جدول patients في PostgreSQL يحتوي على عمود entry_type - يجب إزالته!")
                    if 'entry_type' not in pg_visits_cols:
                        errors.append("❌ خطأ: جدول visits في PostgreSQL لا يحتوي على عمود entry_type - مطلوب!")

                    cursor_pg.close()
            except Exception as e:
                error_logger.error(f"خطأ في التحقق من سلامة PostgreSQL: {e}")
            finally:
                try:
                    release_connection(conn_pg)
                except Exception as e:
                    error_logger.error(f"Error releasing connection during integrity check: {e}")
                    raise

            if errors:
                for error in errors:
                    error_logger.error(error)
                    print(error)
            if warnings:
                for warning in warnings:
                    system_logger.warning(warning)
            
            return len(errors) == 0
        except Exception as e:
            error_logger.error(f"خطأ في التحقق من سلامة النظام: {e}")
            return False

    def _cache_cleaner(self):
        """خيط يقوم بتنظيف الكاش من العناصر المنتهية بشكل دوري"""
        while system_state.running:  # FIX #3: Use system_state.running flag for graceful shutdown
            time.sleep(60)
            self.cache.purge_expired()

    def init_sqlite(self):
        """إنشاء وكاش قاعدة البيانات المحلية SQLite"""
        try:
            conn = sqlite_manager.get_connection(authorized=True)
            # تمكين الوضع السريع لـ SQLite وتحسين أداء الإدخال/القراءة (WAL Mode, Cache, Memory Temp)
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA cache_size = -64000;")
            conn.execute("PRAGMA temp_store = MEMORY;")
            # Improvement #2: Add SQLite busy timeout to prevent "database is locked" errors
            conn.execute("PRAGMA busy_timeout = 5000;")
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS patients (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    patient_id TEXT UNIQUE,
                    name TEXT,
                    national_id TEXT UNIQUE,
                    birth_date TEXT,
                    age INTEGER,
                    gender TEXT,
                    governorate TEXT,
                    phone TEXT,
                    address TEXT,
                    hospital TEXT,
                    registration_date TEXT,
                    fingerprint TEXT,
                    blood_type TEXT,
                    chronic_diseases TEXT,
                    chronic_medications TEXT,
                    allergies TEXT,
                    medical_notes TEXT,
                    current_complaint TEXT,
                    is_unknown INTEGER DEFAULT 0,
                    identified_at TEXT,
                    merged_into TEXT,
                    status TEXT DEFAULT 'active',
                    created_by TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            ''')
            
            # جدول الزيارات (Visits) - يحتوي على entry_type للدخول (عيادات/طوارئ/داخلي)
            # FIX #4: Add visit_uuid for reliable conflict resolution
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS visits (
                    visit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    patient_id TEXT,
                    hospital_code TEXT,
                    hospital_name TEXT,
                    entry_type TEXT,
                    admission_time TEXT,
                    admission_department TEXT,
                    chief_complaint TEXT,
                    initial_diagnosis TEXT,
                    chronic_diseases TEXT,
                    chronic_medications TEXT,
                    allergies TEXT,
                    admission_notes TEXT,
                    discharge_time TEXT,
                    discharge_doctor TEXT,
                    discharge_status TEXT,
                    transfer_to_inpatient INTEGER,
                    transfer_reason TEXT,
                    final_diagnosis TEXT,
                    discharge_medications TEXT,
                    surgery_performed INTEGER,
                    surgery_name TEXT,
                    visit_uuid TEXT UNIQUE,
                    created_by TEXT,
                    created_at TEXT,
                    updated_at TEXT,
                    FOREIGN KEY(patient_id) REFERENCES patients(patient_id)
                )
            ''')

            # التحقق من وجود الأعمدة الجديدة (Migration)
            cursor.execute("PRAGMA table_info(patients)")
            columns = [info[1] for info in cursor.fetchall()]
            new_patient_cols = {
                'blood_type': 'TEXT',
                'chronic_diseases': 'TEXT',
                'chronic_medications': 'TEXT',
                'allergies': 'TEXT',
                'medical_notes': 'TEXT',
                'current_complaint': 'TEXT',
                'gender': 'TEXT',
                'governorate': 'TEXT',
                'age': 'INTEGER',
                'data_entry_personnel': 'TEXT',
                'created_by': 'TEXT',
                'created_at': 'TEXT'
            }
            for col, col_type in new_patient_cols.items():
                if col not in columns:
                    cursor.execute(f"ALTER TABLE patients ADD COLUMN {col} {col_type}")
            
            if 'fingerprint' not in columns:
                cursor.execute("ALTER TABLE patients ADD COLUMN fingerprint TEXT")
                system_logger.info("Added missing 'fingerprint' column to SQLite")
            
            # Fix: Add missing version and updated_at columns to SQLite patients table
            if 'version' not in columns:
                cursor.execute("ALTER TABLE patients ADD COLUMN version INTEGER DEFAULT 1")
                system_logger.info("Added missing 'version' column to SQLite patients table")
            if 'updated_at' not in columns:
                cursor.execute("ALTER TABLE patients ADD COLUMN updated_at TEXT")
                system_logger.info("Added missing 'updated_at' column to SQLite patients table")
            
            # Migration: Add unknown patient support columns
            if 'is_unknown' not in columns:
                cursor.execute("ALTER TABLE patients ADD COLUMN is_unknown INTEGER DEFAULT 0")
                system_logger.info("Added missing 'is_unknown' column to SQLite patients table")
            if 'identified_at' not in columns:
                cursor.execute("ALTER TABLE patients ADD COLUMN identified_at TEXT")
                system_logger.info("Added missing 'identified_at' column to SQLite patients table")
            if 'merged_into' not in columns:
                cursor.execute("ALTER TABLE patients ADD COLUMN merged_into TEXT")
                system_logger.info("Added missing 'merged_into' column to SQLite patients table")
            if 'status' not in columns:
                cursor.execute("ALTER TABLE patients ADD COLUMN status TEXT DEFAULT 'active'")
                system_logger.info("Added missing 'status' column to SQLite patients table")
            
            # Check and add new columns to visits table
            cursor.execute("PRAGMA table_info(visits)")
            visits_columns = [info[1] for info in cursor.fetchall()]
            if 'discharge_status' not in visits_columns:
                cursor.execute("ALTER TABLE visits ADD COLUMN discharge_status TEXT DEFAULT 'غير محدد'")
                system_logger.info("Added 'discharge_status' column to visits table")
            if 'transfer_to_inpatient' not in visits_columns:
                cursor.execute("ALTER TABLE visits ADD COLUMN transfer_to_inpatient INTEGER DEFAULT 0")
                system_logger.info("Added 'transfer_to_inpatient' column to visits table")
            if 'transfer_reason' not in visits_columns:
                cursor.execute("ALTER TABLE visits ADD COLUMN transfer_reason TEXT")
                system_logger.info("Added 'transfer_reason' column to visits table")
            # Improvement #3: Add discharge_department column
            if 'discharge_department' not in visits_columns:
                cursor.execute("ALTER TABLE visits ADD COLUMN discharge_department TEXT")
                system_logger.info("Added 'discharge_department' column to visits table")
            # Improvement #5: Add data_entry_personnel column to visits
            if 'data_entry_personnel' not in visits_columns:
                cursor.execute("ALTER TABLE visits ADD COLUMN data_entry_personnel TEXT")
                system_logger.info("Added 'data_entry_personnel' column to visits table")
            # Add transfer_hospital column for discharge form
            if 'transfer_hospital' not in visits_columns:
                cursor.execute("ALTER TABLE visits ADD COLUMN transfer_hospital TEXT")
                system_logger.info("Added 'transfer_hospital' column to visits table")
            # Add transfer_department column for admission form
            if 'transfer_department' not in visits_columns:
                cursor.execute("ALTER TABLE visits ADD COLUMN transfer_department TEXT")
                system_logger.info("Added 'transfer_department' column to visits table")
            
            # FIX #4: Migration for visit_uuid
            if 'visit_uuid' not in visits_columns:
                cursor.execute("ALTER TABLE visits ADD COLUMN visit_uuid TEXT")
                cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_visits_uuid ON visits(visit_uuid)")
                system_logger.info("Added 'visit_uuid' column and index to visits table")
            

            
            # جدول طابور المزامنة مع فهارس أداء
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sync_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT,
                    table_name TEXT,
                    record_id TEXT,
                    payload TEXT,
                    status TEXT DEFAULT 'pending',
                    retries INTEGER DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            ''')
            # Improvement #2: Add optimized indexes for better query performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sync_status_retry ON sync_queue(status, retries)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sync_created ON sync_queue(created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sync_queue_status ON sync_queue(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sync_queue_created_at ON sync_queue(created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sync_table_record ON sync_queue(table_name, record_id)')
            
            # Improvement: Specific user requested indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_patients_national_id ON patients(national_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_visits_patient_id ON visits(patient_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_visits_admission_time ON visits(admission_time);")
            
            # Migration: إضافة الأعمدة الجديدة إلى sync_queue إن لم تكن موجودة
            cursor.execute("PRAGMA table_info(sync_queue)")
            sync_queue_columns = [info[1] for info in cursor.fetchall()]
            new_sync_cols = {
                'table_name': 'TEXT',
                'record_id': 'TEXT',
                'status': 'TEXT',
                'retries': 'INTEGER',
                'last_error': 'TEXT',
                'updated_at': 'TEXT'
            }
            for col, col_type in new_sync_cols.items():
                if col not in sync_queue_columns:
                    cursor.execute(f"ALTER TABLE sync_queue ADD COLUMN {col} {col_type}")
                    system_logger.info(f"Added '{col}' column to sync_queue table")
            
            # إضافة الفهارس الحرجة لتحسين الأداء
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_patients_national_id ON patients(national_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_patients_patient_id ON patients(patient_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_patients_name ON patients(name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_visits_patient_id ON visits(patient_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_visits_admission_time ON visits(admission_time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_visits_hospital_code ON visits(hospital_code)')
            cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS ux_visits_patient_admission ON visits(patient_id, admission_time)')


            # قيود UNIQUE لمنع تكرار المرضى محلياً
            cursor.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS ux_patients_national_id_unique
                ON patients(national_id)
                WHERE national_id IS NOT NULL
            ''')
            cursor.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS ux_patients_fingerprint_unique
                ON patients(fingerprint)
                WHERE fingerprint IS NOT NULL
            ''')

            # جدول العدادات المحلية لتوليد الرقم الموحد Sequential
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS counters (
                    hospital TEXT PRIMARY KEY,
                    counter INTEGER
                )
            ''')

            # جدول إعدادات الأجهزة لكل مستشفى لتخصيص نطاق أرقام فريد لكل جهاز
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS device_config (
                    device_id       TEXT PRIMARY KEY,
                    hospital_code   TEXT,
                    range_start     INTEGER,
                    range_end       INTEGER,
                    current_counter INTEGER
                )
            ''')

            # فهرس لتحسين سرعة البحث عن أجهزة المستشفى
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_device_hospital
                ON device_config(hospital_code)
            ''')
            
            # جدول المستخدمين المحلي (للتحقق offline) - استخدام bcrypt للأمان
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    password TEXT,
                    hospital_code TEXT,
                    role TEXT
                )
            ''')
            
            # إضافة المستخدمين الافتراضيين محلياً
            try:
                # المسؤول - استخدام bcrypt
                admin_hashed = PasswordHasher.hash_password('admin123')
                cursor.execute("""
                    INSERT OR IGNORE INTO users (username, password, hospital_code, role)
                    VALUES (?, ?, ?, ?)
                """, ('admin', admin_hashed, 'ADMIN', 'admin'))
                
                # مستخدمو المستشفيات - استخدام bcrypt
                hospital_hashed = PasswordHasher.hash_password('123456')
                for code, name in HOSPITALS.items():
                    cursor.execute("""
                        INSERT OR IGNORE INTO users (username, password, hospital_code, role)
                        VALUES (?, ?, ?, ?)
                    """, (code.lower(), hospital_hashed, code, 'hospital'))
            except Exception as e:
                error_logger.error(f"Error creating default users in SQLite: {e}")
            
            conn.commit()
            sqlite_manager.return_connection(conn)
            system_logger.info("SQLite local cache initialized")
        except Exception as e:
            error_logger.error(f"SQLite Initialization Error: {e}")

    def init_postgresql(self):
        """إنشاء الجداول والقيود في PostgreSQL لدعم المزامنة والأمان"""
        conn = None
        try:
            conn = get_connection()
            if not conn: return
            cursor = conn.cursor()
            
            # تعطيل الـ transactions للإنشاء الأولي
            conn.autocommit = True
            
            # جدول المرضى مع فهارس للبحث السريع
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS patients (
                    id SERIAL PRIMARY KEY,
                    patient_id TEXT UNIQUE,
                    name TEXT,
                    national_id TEXT UNIQUE,
                    birth_date TEXT,
                    age INTEGER,
                    gender TEXT,
                    governorate TEXT,
                    phone TEXT,
                    address TEXT,
                    hospital TEXT,
                    registration_date TEXT,
                    fingerprint TEXT,
                    blood_type TEXT,
                    chronic_diseases TEXT,
                    chronic_medications TEXT,
                    allergies TEXT,
                    medical_notes TEXT,
                    current_complaint TEXT,
                    created_by TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    search_vector tsvector
                )
            ''')
            
            # جدول الزيارات (Visits) - جدول موحد لجميع الزيارات بالمستشفيات المختلفة
            # FIX #4: Add visit_uuid for conflict resolution in PostgreSQL
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS visits (
                    visit_id SERIAL PRIMARY KEY,
                    patient_id TEXT,
                    hospital_code TEXT,
                    hospital_name TEXT,
                    entry_type TEXT,
                    admission_time TEXT,
                    admission_department TEXT,
                    chief_complaint TEXT,
                    initial_diagnosis TEXT,
                    chronic_diseases TEXT,
                    chronic_medications TEXT,
                    allergies TEXT,
                    admission_notes TEXT,
                    discharge_time TEXT,
                    discharge_doctor TEXT,
                    discharge_status TEXT,
                    transfer_to_inpatient INTEGER,
                    transfer_reason TEXT,
                    final_diagnosis TEXT,
                    discharge_medications TEXT,
                    surgery_performed INTEGER,
                    surgery_name TEXT,
                    transfer_hospital VARCHAR(200),
                    visit_uuid UUID DEFAULT gen_random_uuid() UNIQUE,
                    created_by TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(patient_id) REFERENCES patients(patient_id)
                )
            ''')
            
            # FIX: Ensure visit_uuid has a UNIQUE constraint for ON CONFLICT to work
            cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_visits_uuid_unique ON visits (visit_uuid)')
            
            # التأكد من وجود الأعمدة في حالة استخدام جدول قديم (SQLite Migration)
            try:
                cursor.execute("ALTER TABLE patients ADD COLUMN IF NOT EXISTS fingerprint TEXT")
                cursor.execute("ALTER TABLE patients ADD COLUMN IF NOT EXISTS blood_type TEXT")
                cursor.execute("ALTER TABLE patients ADD COLUMN IF NOT EXISTS chronic_diseases TEXT")
                cursor.execute("ALTER TABLE patients ADD COLUMN IF NOT EXISTS chronic_medications TEXT")
                cursor.execute("ALTER TABLE patients ADD COLUMN IF NOT EXISTS allergies TEXT")
                cursor.execute("ALTER TABLE patients ADD COLUMN IF NOT EXISTS medical_notes TEXT")
                cursor.execute("ALTER TABLE patients ADD COLUMN IF NOT EXISTS current_complaint TEXT")
                cursor.execute("ALTER TABLE patients ADD COLUMN IF NOT EXISTS gender TEXT")
                cursor.execute("ALTER TABLE patients ADD COLUMN IF NOT EXISTS governorate TEXT")
                cursor.execute("ALTER TABLE patients ADD COLUMN IF NOT EXISTS age INTEGER")
                cursor.execute("ALTER TABLE patients ADD COLUMN IF NOT EXISTS updated_at TEXT")
                
                # Migration for visits table - add new columns
                cursor.execute("ALTER TABLE visits ADD COLUMN IF NOT EXISTS discharge_status TEXT")
                cursor.execute("ALTER TABLE visits ADD COLUMN IF NOT EXISTS transfer_to_inpatient INTEGER")
                cursor.execute("ALTER TABLE visits ADD COLUMN IF NOT EXISTS transfer_reason TEXT")
                cursor.execute("ALTER TABLE visits ADD COLUMN IF NOT EXISTS discharge_department TEXT")
                cursor.execute("ALTER TABLE visits ADD COLUMN IF NOT EXISTS data_entry_personnel TEXT")
                cursor.execute("ALTER TABLE visits ADD COLUMN IF NOT EXISTS transfer_hospital TEXT")
                cursor.execute("ALTER TABLE visits ADD COLUMN IF NOT EXISTS transfer_department TEXT")
                cursor.execute("ALTER TABLE visits ADD COLUMN IF NOT EXISTS visit_uuid UUID")
            except Exception as e:
                error_logger.error(f"SQLite Alter Table Error: {e}")
            
            # فهارس محسّنة للبحث السريع والبصمة (SQLite compatible)
            # فهارس محسّنة للبحث السريع والبصمة (SQL compatible)
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_patients_national_id ON patients(national_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_patients_patient_id  ON patients(patient_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_patients_fingerprint ON patients(fingerprint)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_patients_search      ON patients USING GIN(search_vector)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_patients_name        ON patients(name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_patients_phone       ON patients(phone)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_patients_hospital    ON patients(hospital)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_patients_regdate     ON patients(registration_date)')
            
            # فهارس محسّنة لجدول الزيارات للبحث السريع
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_visits_patient ON visits(patient_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_visits_hospital ON visits(hospital_code)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_visits_date     ON visits(admission_time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_visits_entry    ON visits(entry_type)')
            cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS ux_visits_patient_admission ON visits(patient_id, admission_time)')
            
            # تحديث الـ search_vector للأسماء الموجودة
            cursor.execute("UPDATE patients SET search_vector = to_tsvector('simple', COALESCE(name,'') || ' ' || COALESCE(phone,'') || ' ' || COALESCE(national_id,''))")
            
            # جدول المستخدمين - مع دعم bcrypt passwords
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    password TEXT,
                    hospital_code TEXT,
                    role TEXT,
                    last_login TIMESTAMP,
                    is_active BOOLEAN DEFAULT true
                )
            ''')
            
            # البدء بمستخدم أدمن افتراضي - استخدام bcrypt للوقاية من الهجمات
            admin_pwd_hashed = PasswordHasher.hash_password('admin123')
            cursor.execute("""
                INSERT INTO users (username, password, hospital_code, role)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (username) DO NOTHING
            """, ('admin', admin_pwd_hashed, 'ADMIN', 'admin'))
                
            # إضافة مستخدمين افتراضيين للمستشفيات - استخدام bcrypt
            hospital_pwd_hashed = PasswordHasher.hash_password('123456')
            for code, name in HOSPITALS.items():
                cursor.execute("""
                    INSERT INTO users (username, password, hospital_code, role)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (username) DO NOTHING
                """, (code.lower(), hospital_pwd_hashed, code, 'hospital'))

            # جدول الـ Audit Log لمراقبة العمليات
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS audit_log (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT,
                    action TEXT,
                    table_name TEXT,
                    record_id TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # جدول النسخ الإصدارات (Record Versioning للـ Audit Trail والـ Optimistic Locking)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS record_versions (
                    id SERIAL PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    version INTEGER DEFAULT 1,
                    data_json JSONB,
                    updated_by TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    changes_description TEXT,
                    UNIQUE(table_name, record_id, version)
                )
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_versions_table_record ON record_versions(table_name, record_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_versions_updated_at ON record_versions(updated_at)')
            
            # إضافة عمودي تحديث النسخة للـ Optimistic Locking على PostgreSQL
            try:
                cursor.execute('ALTER TABLE patients ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
            except Exception as e:
                error_logger.debug(f"Column updated_at already exists: {e}")
                pass  # العمود موجود بالفعل
            try:
                cursor.execute('ALTER TABLE patients ADD COLUMN version INTEGER DEFAULT 1')
            except Exception as e:
                error_logger.debug(f"Column version already exists: {e}")
                pass
            try:
                cursor.execute('ALTER TABLE visits ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
            except Exception as e:
                error_logger.debug(f"Column updated_at (visits) already exists: {e}")
                pass
            try:
                cursor.execute('ALTER TABLE visits ADD COLUMN version INTEGER DEFAULT 1')
            except Exception as e:
                error_logger.debug(f"Column version (visits) already exists: {e}")
                pass

            conn.autocommit = False
            # إضافة قيد الفرادة للرقم القومي لضمان منع التكرار
            # FIX #2: Add UNIQUE constraint for national_id to prevent duplicates
            try:
                cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_national_id ON patients(national_id);")
                conn.commit()
                system_logger.info("UNIQUE constraint on national_id verified/created")
            except Exception as e:
                conn.rollback()
                error_logger.warning(f"Could not add UNIQUE constraint on national_id: {e}")
            
            # Also ensure fingerprint uniqueness where available
            try:
                cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fingerprint ON patients(fingerprint) WHERE fingerprint IS NOT NULL;")
                conn.commit()
                system_logger.info("UNIQUE constraint on fingerprint (where not NULL) verified/created")
            except Exception as e:
                conn.rollback()
                error_logger.warning(f"Could not add UNIQUE constraint on fingerprint: {e}")

        finally:
            if 'cursor' in locals() and cursor:
                try:
                    cursor.close()
                except Exception as e:
                    error_logger.debug(f"Cursor close exception (ignored): {e}")
            release_connection(conn)

    def get_pending_sync_count(self):
        """الحصول على عدد العمليات المعلقة في طابور المزامنة"""
        conn = None
        try:
            conn = sqlite_manager.get_connection(authorized=True)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT COUNT(*)
                FROM sync_queue
                WHERE status IN ('pending','processing','failed')
            """)

            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            error_logger.error(f"Error in get_pending_sync_count: {e}")
            return 0
        finally:
            if conn:
                sqlite_manager.return_connection(conn)
    
    def get_sync_status(self):
        """الحصول على حالة المزامنة الكاملة"""
        conn = None
        try:
            conn = sqlite_manager.get_connection(authorized=True)
            cursor = conn.cursor()
            
            # الحصول على إحصائيات حالة المزامنة
            cursor.execute("SELECT status, COUNT(*) FROM sync_queue GROUP BY status")
            counts = {row[0]: row[1] for row in cursor.fetchall()}
            
            # الحصول على آخر وقت مزامنة
            cursor.execute("SELECT MAX(updated_at) FROM sync_queue WHERE status='done' LIMIT 1")
            last_sync_row = cursor.fetchone()
            last_sync = last_sync_row[0] if last_sync_row else None
            
            return {
                'pending': counts.get('pending', 0),
                'processing': counts.get('processing', 0), 
                'failed': counts.get('failed', 0),
                'done': counts.get('done', 0),
                'last_sync': last_sync,
                'is_online': self.is_online()
            }
        except Exception as e:
            error_logger.error(f"Error getting sync status: {e}")
            return {
                'pending': 0, 'processing': 0, 'failed': 0, 'done': 0,
                'last_sync': None, 'is_online': self.is_online()
            }
        finally:
            if conn:
                sqlite_manager.return_connection(conn)

    def add_to_sync_queue(self, action, payload):
        table = 'patients' if 'patient' in action else 'visits'
        self.trigger_sync(action, table, payload.get('patient_id', 'unknown'), payload)

    def trigger_sync(self, action: str, table_name: str, record_id: str, payload: dict = None):
        """تشغيل المزامنة فوراً عند حدوث تغيير في البيانات"""
        if payload is None:
            payload = {}
        
        # SKIP: لا يتم مزامنة المرضى المجهولين (is_unknown = 1)
        if payload.get('is_unknown') == 1 or record_id.startswith('U-') or '-U-' in record_id:
            sync_logger.info(f"Skipping sync trigger for unknown patient: {record_id}")
            return
        
        if not self.is_online():
            self.sync_queue_manager.add_item(action, table_name, record_id, payload)
        else:
            threading.Thread(
                target=lambda: self._execute_immediate_sync(action, table_name, record_id, payload),
                daemon=True
            ).start()
    
    def _execute_immediate_sync(self, action: str, table_name: str, record_id: str, payload: dict):
        """تنفيذ المزامنة الفورية - مزامنة فوراً بدون انتظار الطابور"""
        try:
            item = {
                'action': action,
                'table_name': table_name,
                'record_id': record_id,
                'payload': safe_json_dumps(payload) if isinstance(payload, dict) else payload,
                'retries': 0
            }
            
            # FIXED: Use refactored sync manager for immediate sync
            success = self.refactored_sync_manager._process_single_item(item)
            if success:
                # FIXED: Record successful immediate sync in sync_queue table for "Last Sync" tracking
                try:
                    conn = sqlite_manager.get_connection(authorized=True)
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO sync_queue (action, table_name, record_id, payload, status, updated_at)
                        VALUES (?, ?, ?, ?, 'done', ?)
                    """, (action, table_name, record_id, item['payload'], datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    sqlite_manager.return_connection(conn)
                except Exception as e:
                    error_logger.error(f"Failed to record immediate sync success: {e}")
            else:
                # في حالة المشكلة، أضف للطابور للمحاولة لاحقاً
                self.sync_queue_manager.add_item(action, table_name, record_id, payload)
                sync_logger.warning(f"Immediate sync failed for {action}, queued for later")
        except Exception as e:
            error_logger.error(f"Error in immediate sync: {e}")
            self.sync_queue_manager.add_item(action, table_name, record_id, payload)
    
    def _record_version(self, record_id: str, table_name: str, version: int, data: dict, user_id: str, description: str = ""):
        """تسجيل نسخة من السجل في جدول record_versions لـ Audit Trail"""
        if not self.is_online():
            return
        
        conn = None
        try:
            conn = get_connection()
            if conn:
                cursor = conn.cursor()
                # Use INSERT ON CONFLICT DO NOTHING to handle duplicate versions
                cursor.execute("""
                    INSERT INTO record_versions (table_name, record_id, version, data_json, updated_by, changes_description)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (table_name, record_id, version) DO NOTHING
                """, (table_name, record_id, version, safe_json_dumps(data), user_id, description))
                conn.commit()
                cursor.close()
        except Exception as e:
            error_logger.error(f"Error recording version: {e}")
        finally:
            release_connection(conn)

    def get_sync_queue_count_total(self):
        """جلب العدد الإجمالي للـ Records المنتظرة للمزامنة"""
        try:
            conn = sqlite_manager.get_connection(authorized=True)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM sync_queue")
            count = cursor.fetchone()[0]
            sqlite_manager.return_connection(conn)
            return count
        except Exception as e:
            error_logger.error(f"Error getting sync queue count: {e}")
            return 0

    def get_total_patients(self):
        """إرجاع عدد المرضى الحاليين (بما فيهم المجهولين من SQLite)"""
        conn_pg = None
        cursor_pg = None
        conn_sq = None
        cursor_sq = None
        try:
            total = 0
            
            # جلب العدد من PostgreSQL إذا كان متصلاً
            if self.is_online():
                conn_pg = get_connection()
                if conn_pg:
                    cursor_pg = conn_pg.cursor()
                    cursor_pg.execute("SELECT COUNT(*) FROM patients")
                    total = cursor_pg.fetchone()[0]
            
            # جلب عدد المجهولين من SQLite دائماً (لأنهم Local Only)
            conn_sq = sqlite_manager.get_connection(authorized=True)
            cursor_sq = conn_sq.cursor()
            cursor_sq.execute("SELECT COUNT(*) FROM patients WHERE patient_id LIKE '%-U-%' OR is_unknown = 1")
            unknown_count = cursor_sq.fetchone()[0]
            
            # إذا كان Offline، جلب العدد الكلي من SQLite
            if not self.is_online():
                cursor_sq.execute("SELECT COUNT(*) FROM patients")
                return cursor_sq.fetchone()[0]
            else:
                # إذا كان Online، أضف المجهولين للعدد
                total += unknown_count
            
            return total
        except Exception as e:
            error_logger.error(f"Error getting total patients count: {e}")
            return 0
        finally:
            if cursor_pg:
                try:
                    cursor_pg.close()
                except Exception:
                    pass
            if conn_pg:
                release_connection(conn_pg)
            if cursor_sq:
                try:
                    cursor_sq.close()
                except Exception:
                    pass
            if conn_sq:
                sqlite_manager.return_connection(conn_sq)

    def get_total_visits(self):
        """إرجاع عدد الزيارات الحالية"""
        try:
            if self.is_online():
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM visits")
                n = cursor.fetchone()[0]
                cursor.close()
                release_connection(conn)
                return n
            else:
                conn = sqlite_manager.get_connection(authorized=True)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM visits")
                n = cursor.fetchone()[0]
                sqlite_manager.return_connection(conn)
                return n
        except Exception as e:
            error_logger.error(f"Error getting total visits count: {e}")
            return 0

    def _execute_pg_add_patient(self, cursor, data):
        """FIXED: Add system_state.mode guard at start to prevent offline PostgreSQL usage"""
        
        # GUARD: Prevent PostgreSQL usage in OFFLINE/CRITICAL modes
        if system_state.mode != "ONLINE":
            error_logger.warning(f"PostgreSQL add patient skipped in {system_state.mode} mode")
            return
        
        # Support both single dict and list of dicts for batch insert
        def _build_tuple(d):
            # composite search vector: name phone national_id
            return (
                d['patient_id'], d['name'], d['national_id'], d['birth_date'],
                d.get('age'), d.get('gender'), d['phone'], d['address'], d['hospital'], d['registration_date'],
                d.get('fingerprint'), d.get('blood_type',''), d.get('chronic_diseases',''),
                d.get('chronic_medications',''), d.get('allergies',''), d.get('medical_notes',''),
                d.get('current_complaint', ''), d.get('created_by', 'system'),
                d.get('created_at', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                # search_vector will be computed below
                d['name'], d.get('phone',''), d.get('national_id','')
            )

        if isinstance(data, list):
            # batch insert باستخدام execute_values على دفعات (100 سجل في كل دفعة)
            values = [_build_tuple(d) for d in data]
            sql = ("INSERT INTO patients (patient_id, name, national_id, birth_date, age, gender, phone, address, hospital, registration_date, fingerprint, "
                   "blood_type, chronic_diseases, chronic_medications, allergies, medical_notes, current_complaint, created_by, created_at, "
                   "search_vector) VALUES %s ON CONFLICT (national_id) DO UPDATE SET "
                   "name = EXCLUDED.name, age = EXCLUDED.age, gender = EXCLUDED.gender, phone = EXCLUDED.phone, address = EXCLUDED.address, fingerprint = EXCLUDED.fingerprint, "
                   "blood_type = EXCLUDED.blood_type, chronic_diseases = EXCLUDED.chronic_diseases, "
                   "chronic_medications = EXCLUDED.chronic_medications, allergies = EXCLUDED.allergies, "
                   "medical_notes = EXCLUDED.medical_notes, current_complaint = EXCLUDED.current_complaint")

            BATCH_SIZE = 100
            for i in range(0, len(values), BATCH_SIZE):
                batch = values[i:i + BATCH_SIZE]
                # build search_vector expression using the last three placeholders
                psycopg2.extras.execute_values(
                    cursor,
                    sql,
                    batch,
                    template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,to_tsvector('simple', COALESCE(%s,'') || ' ' || COALESCE(%s,'') || ' ' || COALESCE(%s,'')))",
                    page_size=BATCH_SIZE,
                )
        else:
            tup = _build_tuple(data)
            cursor.execute("""
                INSERT INTO patients (patient_id, name, national_id, birth_date, age, gender, phone, address, hospital, registration_date, fingerprint, 
                                     blood_type, chronic_diseases, chronic_medications, allergies, medical_notes, current_complaint, created_by, created_at, search_vector)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        to_tsvector('simple', COALESCE(%s,'') || ' ' || COALESCE(%s,'') || ' ' || COALESCE(%s,'')))
                ON CONFLICT (national_id) DO UPDATE SET 
                name = EXCLUDED.name, age = EXCLUDED.age, gender = EXCLUDED.gender, phone = EXCLUDED.phone, address = EXCLUDED.address, fingerprint = EXCLUDED.fingerprint,
                blood_type = EXCLUDED.blood_type, chronic_diseases = EXCLUDED.chronic_diseases, 
                chronic_medications = EXCLUDED.chronic_medications, allergies = EXCLUDED.allergies, 
                medical_notes = EXCLUDED.medical_notes, current_complaint = EXCLUDED.current_complaint
            """, tup)
    def _execute_pg_update_patient(self, cursor, p_id, data):
        """FIXED: Add system_state.mode guard at start to prevent offline PostgreSQL usage"""
        
        # GUARD: Prevent PostgreSQL usage in OFFLINE/CRITICAL modes
        if system_state.mode != "ONLINE":
            error_logger.warning(f"PostgreSQL update patient skipped in {system_state.mode} mode")
            return
        
        # Bug fix: search_vector يجب أن يتضمن الاسم والهاتف والرقم القومي (مطابق لـ add_patient)
        cursor.execute("""
            UPDATE patients SET name=%s, national_id=%s, birth_date=%s, age=%s, gender=%s, phone=%s, address=%s,
            fingerprint=%s, blood_type=%s, chronic_diseases=%s, chronic_medications=%s, allergies=%s, medical_notes=%s,
            current_complaint=%s, search_vector=to_tsvector('simple',
                COALESCE(%s,'') || ' ' || COALESCE(%s,'') || ' ' || COALESCE(%s,''))
            WHERE patient_id=%s
        """, (data['name'], data['national_id'], data['birth_date'], data.get('age'), data.get('gender'),
              data['phone'], data['address'], data.get('fingerprint'),
              data.get('blood_type', ''), data.get('chronic_diseases', ''),
              data.get('chronic_medications', ''), data.get('allergies', ''),
              data.get('medical_notes', ''), data.get('current_complaint', ''),
              data['name'], data.get('phone', ''), data.get('national_id', ''), p_id))

    def backup_data(self):
        """إنشاء نسخة احتياطية كل 6 ساعات من البيانات بصيغة JSON (يعمل في خيط غير محظور)"""
        def _do_backup():
            try:
                # إنشاء مجلد النسخ الاحتياطي إذا لم يكن موجوداً
                if not os.path.exists(BACKUP_DIR):
                    os.makedirs(BACKUP_DIR)
                    system_logger.info(f"Created backup directory: {BACKUP_DIR}")
                
                backup_file = os.path.join(BACKUP_DIR, f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                
                # التحقق من وجود نسخة لنفس اليوم (لتجنب التكرار)
                today_prefix = datetime.now().strftime("%Y%m%d_")
                existing_backups = [f for f in os.listdir(BACKUP_DIR) if f.startswith(today_prefix)]
                if existing_backups:
                    system_logger.info(f"Backup for today already exists: {existing_backups[0]}")
                    return
                
                data = {}
                conn = None
                cursor = None
                
                try:
                    # جلب البيانات من SQLite (الأحدث) عبر مدير الاتصال
                    conn = sqlite_manager.get_connection(authorized=True)
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    
                    cursor.execute("SELECT * FROM patients")
                    data['patients'] = [dict(row) for row in cursor.fetchall()]
                    
                    cursor.execute("SELECT * FROM visits")
                    data['visits'] = [dict(row) for row in cursor.fetchall()]
                    
                    cursor.execute("SELECT * FROM sync_queue")
                    data['sync_queue'] = [dict(row) for row in cursor.fetchall()]
                    
                    cursor.execute("SELECT * FROM users")
                    data['users'] = [dict(row) for row in cursor.fetchall()]
                except Exception as e:
                    error_logger.error(f"Error fetching data for backup: {e}")
                    raise
                finally:
                    if cursor:
                        try:
                            cursor.close()
                        except Exception:
                            pass
                    if conn:
                        sqlite_manager.return_connection(conn)
                
                with open(backup_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4, cls=DateTimeEncoder)
                
                # Verify backup file was created successfully
                if os.path.exists(backup_file) and os.path.getsize(backup_file) > 0:
                    system_logger.info(f"✅ Automated backup created successfully: {backup_file} ({os.path.getsize(backup_file)} bytes)")
                else:
                    error_logger.error(f"❌ Backup file creation failed or empty: {backup_file}")
                    return
                
                # تنظيف النسخ القديمة (الاحتفاظ بآخر 7 أيام فقط)
                self._cleanup_old_backups()
                
            except Exception as e:
                error_logger.error(f"Backup Error: {e}")
        
        # أطلق الخيط لكن لا تنتظر انتهاءه
        threading.Thread(target=_do_backup, daemon=True).start()
    
    def _cleanup_old_backups(self):
        """تنظيف النسخ الاحتياطية القديمة (الاحتفاظ بآخر 7 أيام)"""
        try:
            if not os.path.exists(BACKUP_DIR):
                return
            
            cutoff_date = datetime.now() - timedelta(days=7)
            cutoff_str = cutoff_date.strftime("%Y%m%d")
            
            deleted_count = 0
            for filename in os.listdir(BACKUP_DIR):
                if filename.startswith("backup_") and len(filename) > 13:
                    # استخراج التاريخ من اسم الملف
                    file_date_str = filename[7:15]  # YYYYMMDD
                    if file_date_str < cutoff_str:
                        try:
                            os.remove(os.path.join(BACKUP_DIR, filename))
                            system_logger.info(f"🗑️ Deleted old backup: {filename}")
                            deleted_count += 1
                        except Exception as e:
                            error_logger.error(f"Error deleting old backup {filename}: {e}")
            
            if deleted_count > 0:
                system_logger.info(f"✅ Cleanup completed: {deleted_count} old backup(s) removed (retention policy: 7 days)")
        except Exception as e:
            error_logger.error(f"Error cleaning up old backups: {e}")
    
    def generate_fingerprint(self, name, birth_date, national_id):
        """إنشاء بصمة فريدة للمريض لمنع التكرار"""
        raw = f"{str(name).strip().lower()}{str(birth_date).strip()}{str(national_id).strip()}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def _device_config_table_exists(self, conn: sqlite3.Connection) -> bool:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='device_config' LIMIT 1"
        )
        return cursor.fetchone() is not None

    def _generate_patient_id_legacy(self, conn: sqlite3.Connection, hospital_code: str) -> str:
        """
        منطق التوليد المعتمد على Device Offset لمنع التعارض بين الأجهزة.
        كل جهاز يبدأ من Range مختلف بناءً على device_id.
        الصيغة: hospital_code + 6 أرقام (بدون شرطة)
        """
        conn.execute("PRAGMA busy_timeout=5000")
        cursor = conn.cursor()
        
        # 1) الحصول على device_id وتوليد device_code (1-9)
        device_id = get_auto_device_id()
        device_hash = hashlib.md5(device_id.encode()).hexdigest()
        device_code = (int(device_hash, 16) % 9) + 1  # من 1 إلى 9
        
        # 2) حساب offset للجهاز (مثال: 100000, 200000, ...)
        offset = device_code * 100000
        range_start = offset + 1
        range_end = offset + 99999
        
        # 3) البحث عن أعلى رقم داخل نفس المستشفى ونفس الرينج
        cursor.execute("""
            SELECT patient_id FROM patients 
            WHERE patient_id LIKE ?
        """, (f"{hospital_code}%",))
        
        rows = cursor.fetchall()
        max_seq = 0
        
        for row in rows:
            if row[0]:
                # استخراج الرقم من patient_id (مثال: KFS100123 -> 100123)
                try:
                    seq_str = row[0].replace(hospital_code, "")
                    seq_num = int(seq_str)
                    # فقط الأرقام داخل الرينج الحالي
                    if range_start <= seq_num <= range_end:
                        if seq_num > max_seq:
                            max_seq = seq_num
                except (ValueError, IndexError):
                    continue
        
        # 4) توليد الرقم الجديد
        if max_seq > 0:
            next_seq = max_seq + 1
        else:
            next_seq = range_start
        
        # 5) التحقق من عدم تجاوز الرينج
        if next_seq > range_end:
            msg = (
                f"Device ID range exhausted – contact system administrator. "
                f"(device_id={device_id}, device_code={device_code}, "
                f"range={range_start}-{range_end}, next_seq={next_seq})"
            )
            error_logger.critical(msg)
            raise RuntimeError(msg)
        
        if hasattr(conn, 'authorize'):
            conn.authorize()
        conn.commit()
        return f"{hospital_code}{next_seq:06d}"

    def _generate_unknown_patient_id(self, conn: sqlite3.Connection, hospital_code: str) -> str:
        """
        توليد رقم مريض مجهول الهوية باستخدام نفس منطق device offset.
        الصيغة: hospital_code-U-XXXXXX (مثال: KFS-U-000001)
        """
        conn.execute("PRAGMA busy_timeout=5000")
        cursor = conn.cursor()
        
        # 1) الحصول على device_id وتوليد device_code (1-9)
        device_id = get_auto_device_id()
        device_hash = hashlib.md5(device_id.encode()).hexdigest()
        device_code = (int(device_hash, 16) % 9) + 1  # من 1 إلى 9
        
        # 2) حساب offset للجهاز (مثال: 100000, 200000, ...)
        offset = device_code * 100000
        range_start = offset + 1
        range_end = offset + 99999
        
        # 3) البحث عن أعلى رقم داخل نفس المستشفى ونفس الرينج للمجهولين
        # نبحث عن كل المرضى الذين لديهم نمط U- في الرقم (بغض النظر عن is_unknown)
        cursor.execute("""
            SELECT patient_id FROM patients 
            WHERE patient_id LIKE ?
        """, (f"{hospital_code}-U-%",))
        
        rows = cursor.fetchall()
        max_seq = 0
        
        for row in rows:
            if row[0]:
                # استخراج الرقم من patient_id (مثال: KFS-U-100123 -> 100123)
                try:
                    # إزالة البادئة hospital_code-U-
                    seq_str = row[0].replace(f"{hospital_code}-U-", "")
                    seq_num = int(seq_str)
                    # فقط الأرقام داخل الرينج الحالي
                    if range_start <= seq_num <= range_end:
                        if seq_num > max_seq:
                            max_seq = seq_num
                except (ValueError, IndexError):
                    continue
        
        # 4) توليد الرقم الجديد
        if max_seq > 0:
            next_seq = max_seq + 1
        else:
            next_seq = range_start
        
        # 5) التحقق من عدم تجاوز الرينج
        if next_seq > range_end:
            msg = (
                f"Unknown patient ID range exhausted – contact system administrator. "
                f"(device_id={device_id}, device_code={device_code}, "
                f"range={range_start}-{range_end}, next_seq={next_seq})"
            )
            error_logger.critical(msg)
            raise RuntimeError(msg)
        
        if hasattr(conn, 'authorize'):
            conn.authorize()
        conn.commit()
        return f"{hospital_code}-U-{next_seq:06d}"

    def _generate_patient_id_device_range(
        self, conn: sqlite3.Connection, hospital_code: str
    ) -> str:
        """
        توليد رقم مريض باستخدام نطاق الجهاز من جدول device_config.
        الصيغة: hospital_code + 6 أرقام بدون فواصل.
        """
        conn.execute("PRAGMA busy_timeout=5000")  # Ensure busy timeout is set
        device_id = get_auto_device_id()
        cursor = conn.cursor()
        # SQLite starts transaction automatically, no need for explicit BEGIN
        cursor.execute(
            """
            SELECT hospital_code, range_start, range_end, current_counter
            FROM device_config
            WHERE device_id = ?
            """,
            (device_id,),
        )
        row = cursor.fetchone()
        if row is None:
            # رسالة معلومات عن استخدام المنطق التلقائي
            system_logger.info(
                f"device_config not explicitly configured for device_id={device_id}; "
                f"using default legacy counter generation."
            )
            return self._generate_patient_id_legacy(conn, hospital_code)

        cfg_hospital_code, range_start, range_end, current_counter = row
        # استخدام hospital_code من إعداد الجهاز لضمان التوافق مع التهيئة
        effective_hospital_code = cfg_hospital_code or hospital_code

        # إذا لم يتم استخدام النطاق من قبل، نبدأ من بداية النطاق
        if current_counter is None:
            new_counter = range_start
        else:
            new_counter = current_counter + 1

        if new_counter > range_end:
            msg = (
                f"Device ID range exhausted – contact system administrator. "
                f"(device_id={device_id}, hospital={effective_hospital_code}, "
                f"current_counter={current_counter}, range_end={range_end})"
            )
            error_logger.critical(msg)
            raise RuntimeError(msg)

        cursor.execute(
            "UPDATE device_config SET current_counter=? WHERE device_id=?",
            (new_counter, device_id),
        )
        if cursor.rowcount != 1:
            raise RuntimeError(
                f"فشل تحديث جدول device_config لهذا الجهاز (device_id={device_id})."
            )

        if hasattr(conn, 'authorize'):
            conn.authorize()
        conn.commit()
        return f"{effective_hospital_code}{int(new_counter):06d}"

    def generate_patient_id(self, hospital_code, is_unknown=False):
        """
        توليد رقم مريض موحد يعمل Online/Offline مع دعم نطاقات الأجهزة.

        - إذا كان جدول device_config موجوداً: يستخدم نطاق الجهاز.
        - إذا لم يكن موجوداً: يعود للمنطق القديم المعتمد على device offset.
        - للمرضى المجهولين: يستخدم بادئة U- مع نفس منطق الـ device offset

        الصيغة:
            - مريض عادي: hospital_code + 6 أرقام (مثال: KSH000001)
            - مريض مجهول: hospital_code-U- + 6 أرقام (مثال: KSH-U-000001)
        
        Improvement #1: Uses _id_lock to prevent race conditions when generating IDs
        """
        # Use lock to prevent race conditions in ID generation
        with self._id_lock:
            try:
                conn = self.sqlite_manager.get_connection(authorized=True)
                if conn is None:
                    raise RuntimeError("تعذر الحصول على اتصال SQLite لتوليد الرقم الموحد.")

                if is_unknown:
                    patient_id = self._generate_unknown_patient_id(conn, hospital_code)
                elif self._device_config_table_exists(conn):
                    patient_id = self._generate_patient_id_device_range(conn, hospital_code)
                else:
                    patient_id = self._generate_patient_id_legacy(conn, hospital_code)

                sqlite_manager.return_connection(conn)
                return patient_id
            except Exception as e:
                error_logger.error(f"Error generating patient ID: {e}")
                # fallback آمن مع الحفاظ على الصيغة الأساسية (6 أرقام)
                rand_suffix = "".join(
                    str(int.from_bytes(os.urandom(1), "big") % 10) for _ in range(6)
                )
                prefix = "U-" if is_unknown else ""
                return f"{hospital_code}-{prefix}{rand_suffix}"

    # ===========================
    # Device status helpers
    # ===========================
    # Improvement #12: System health diagnostics
    # ===========================
    def get_device_status(self):
        """
        إرجاع بيانات حالة الجهاز الحالي لاستخدامها في لوحة التحكم:
        - device_id
        - hospital_code
        - range_start
        - range_end
        - current_counter
        - is_online
        - sync_queue_pending_count
        - sync_queue_failed_count
        """
        device_id = get_auto_device_id()
        status = {
            'device_id': device_id,
            'hospital_code': None,
            'range_start': None,
            'range_end': None,
            'current_counter': None,
            'is_online': self.is_online(),
            'sync_queue_pending': 0,
            'sync_queue_failed': 0,
            'sync_queue_done': 0,
        }

        try:
            conn = sqlite_manager.get_connection(authorized=True)
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT hospital_code, range_start, range_end, current_counter
                FROM device_config
                WHERE device_id = ?
                """,
                (device_id,),
            )
            row = cursor.fetchone()
            if row:
                status['hospital_code'] = row[0]
                status['range_start'] = row[1]
                status['range_end'] = row[2]
                status['current_counter'] = row[3]
            
            # Get sync queue counts
            cursor.execute("SELECT status, COUNT(*) FROM sync_queue GROUP BY status")
            for row in cursor.fetchall():
                status_key = f"sync_queue_{row[0]}"
                status[status_key] = row[1]
            
            sqlite_manager.return_connection(conn)
        except Exception as e:
            error_logger.error(f"Error reading device status: {e}")

        return status
    
    def get_sync_queue_count(self) -> dict:
        """Get current sync queue counts for monitoring"""
        try:
            conn = sqlite_manager.get_connection(authorized=True)
            cursor = conn.cursor()
            cursor.execute("SELECT status, COUNT(*) FROM sync_queue GROUP BY status")
            counts = {row[0]: row[1] for row in cursor.fetchall()}
            sqlite_manager.return_connection(conn)
            return counts
        except Exception as e:
            error_logger.error(f"Error getting sync queue count: {e}")
            return {}

    def log_audit(self, user_id, action, table_name, record_id):
        """تسجيل العمليات في الـ Audit Log"""
        if self.is_online():
            threading.Thread(target=self._execute_audit_pg, args=(user_id, action, table_name, record_id), daemon=True).start()

    def _execute_audit_pg(self, user_id, action, table_name, record_id):
        conn = None
        cursor = None
        try:
            conn = get_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO audit_log (user_id, action, table_name, record_id)
                    VALUES (%s, %s, %s, %s)
                """, (user_id, action, table_name, record_id))
                conn.commit()
        except Exception as e:
            error_logger.error(f"Audit Log Error: {e}")
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                release_connection(conn)


    
    def search_similar_patients(self, name=None, national_id_prefix=None, limit=5):
        """بحث عن مرضى مشابهين مع دعم Offline والـ Caching"""
        # Check cache first
        cache_key = f"similar_{name}_{national_id_prefix}_{limit}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result
        
        similar = []
        if self.is_online():
            conn = None
            try:
                conn = get_connection()
                if conn:
                    cursor = conn.cursor()
                    if name and len(name) > 3:
                        cursor.execute('''
                            SELECT patient_id, name, national_id FROM patients 
                            WHERE name ILIKE %s ORDER BY id DESC LIMIT %s
                        ''', (f'%{name[:10]}%', limit))
                        for row in cursor.fetchall():
                            similar.append({'patient_id': row[0], 'name': row[1], 'national_id': row[2]})
                    
                    if national_id_prefix and len(national_id_prefix) >= 10:
                        cursor.execute('''
                            SELECT patient_id, name, national_id FROM patients 
                            WHERE national_id LIKE %s ORDER BY id DESC LIMIT %s
                        ''', (f'{national_id_prefix}%', limit))
                        similar_ids = {s['national_id'] for s in similar}
                        for row in cursor.fetchall():
                            if row[2] not in similar_ids:
                                similar.append({'patient_id': row[0], 'name': row[1], 'national_id': row[2]})
                                similar_ids.add(row[2])
                    cursor.close()
                    result = similar[:limit]
                    self.cache.set(cache_key, result)
                    return result
            except Exception as e:
                error_logger.error(f"PG Similar Search Error: {e}")
            finally:
                release_connection(conn)

        # Fallback to Local SQLite
        try:
            conn = sqlite_manager.get_connection(authorized=True)
            cursor = conn.cursor()
            if name and len(name) > 3:
                cursor.execute("SELECT patient_id, name, national_id FROM patients WHERE name LIKE ? ORDER BY id DESC LIMIT ?", (f'%{name[:10]}%', limit))
                for row in cursor.fetchall():
                    similar.append({'patient_id': row[0], 'name': row[1], 'national_id': row[2]})
                    
            if national_id_prefix and len(national_id_prefix) >= 10:
                cursor.execute("SELECT patient_id, name, national_id FROM patients WHERE national_id LIKE ? ORDER BY id DESC LIMIT ?", (f'{national_id_prefix}%', limit))
                similar_ids = {s['national_id'] for s in similar}
                for row in cursor.fetchall():
                    if row[2] not in similar_ids:
                        similar.append({'patient_id': row[0], 'name': row[1], 'national_id': row[2]})
                        similar_ids.add(row[2])
            sqlite_manager.return_connection(conn)
            return similar[:limit]
        except Exception as e:
            self.log_error("SQLite Similar Search Error", e)
            return []
    
    def search_patients(self, query, limit=100):
        """بحث فائق السرعة باستخدام PostgreSQL Full Text Search مع Fallback محلي"""
        # caching
        cache_key = f"search_{query}_{limit}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached

        if self.is_online():
            conn = None
            try:
                conn = get_connection()
                if conn:
                    cursor = conn.cursor()
                    # البحث بالاسم (FTS) أو الرقم القومي أو الهاتف أو الرقم الموحد
                    query_fixed = query.strip()
                    cursor.execute("""
                        SELECT id, patient_id, name, national_id, birth_date, age, gender, phone, address, hospital, registration_date
                        FROM patients 
                        WHERE search_vector @@ plainto_tsquery('simple', %s)
                           OR phone = %s
                           OR national_id = %s
                           OR patient_id = %s
                        ORDER BY ts_rank(search_vector, plainto_tsquery('simple', %s)) DESC
                        LIMIT %s
                    """, (query_fixed, query_fixed, query_fixed, query_fixed, query_fixed, limit))
                    cols = ['id', 'patient_id', 'name', 'national_id', 'birth_date', 'age', 'gender', 'phone', 'address', 'hospital', 'registration_date']
                    rows = cursor.fetchall()
                    result = [{cols[i]: v for i, v in enumerate(row)} for row in rows]
                    cursor.close()
                    self.cache.set(cache_key, result)
                    return result
            except Exception as e:
                error_logger.error(f"FTS Search Error: {e}")
            finally:
                release_connection(conn)

        # Fallback to Local SQLite (Basic LIKE search)
        try:
            conn = sqlite_manager.get_connection(authorized=True)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM patients 
                WHERE name LIKE ? OR patient_id LIKE ? OR national_id LIKE ? OR phone LIKE ?
                ORDER BY id DESC LIMIT ?
            """, (f'%{query}%', f'%{query}%', f'%{query}%', f'%{query}%', limit))
            rows = cursor.fetchall()
            sqlite_manager.return_connection(conn)
            result = [dict(row) for row in rows]
            self.cache.set(cache_key, result)
            return result
        except Exception as e:
            self.log_error("SQLite Search Error", e)
            return []

    def get_all_patients(self, limit=50, offset=0, force_sqlite=False):
        """جلب المرضى مع دعم الـ Offline Mode وكاش الـ Keyset
        
        ملاحظة: يتم دمج المرضى المجهولين (Local Only) مع نتائج PostgreSQL
        """
        # cache lookup (key includes offset/limit)
        cache_key = f"all_{offset}_{limit}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached

        result = []
        
        # محاولة جلب البيانات Online أولاً (إلا إذا كان force_sqlite=True)
        # force_sqlite=True يُستخدم بعد عمليات الإضافة/الحذف مباشرةً لأن
        # الكتابات تذهب أولاً لـ SQLite قبل المزامنة مع PostgreSQL
        if self.is_online() and not force_sqlite:
            conn = None
            COLUMNS_SQL = """
                id, patient_id, name, national_id, birth_date,
                age, gender, phone, address, hospital, registration_date,
                COUNT(*) OVER () as total_count
            """
            COLUMNS = ['id', 'patient_id', 'name', 'national_id', 'birth_date',
                       'age', 'gender', 'phone', 'address', 'hospital', 'registration_date', 'total_count']
            try:
                conn = get_connection()
                if conn:
                    if offset == 0: self._page_cache.clear()
                    
                    cursor = conn.cursor()
                    last_id = self._page_cache.get(offset)
                    
                    if last_id is not None:
                        cursor.execute(f"SELECT {COLUMNS_SQL} FROM patients WHERE id < %s ORDER BY id DESC LIMIT %s", (last_id, limit))
                    else:
                        cursor.execute(f"SELECT {COLUMNS_SQL} FROM patients ORDER BY id DESC LIMIT %s", (limit,))
                    
                    rows = cursor.fetchall()
                    cursor.close()
                    result = [{COLUMNS[i]: v for i, v in enumerate(row)} for row in rows]
                    if result:
                        # Limit cache size to prevent memory leaks
                        if len(self._page_cache) >= self._MAX_PAGE_CACHE_SIZE:
                            self._page_cache.clear()
                        self._page_cache[offset + limit] = result[-1]['id']
            except Exception as e:
                error_logger.error(f"PostgreSQL Read Error: {e}")
            finally:
                release_connection(conn)
        
        # جلب المرضى المجهولين من SQLite (Local Only) - دائماً
        try:
            conn = sqlite_manager.get_connection(authorized=True)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            # جلب المرضى المجهولين + أي مريض في طابور المزامنة لم يظهر في PG بعد (Normal patients in sync queue)
            cursor.execute("""
                SELECT *, (SELECT COUNT(*) FROM patients) as total_count 
                FROM patients 
                WHERE patient_id LIKE '%-U-%' 
                   OR is_unknown = 1 
                   OR patient_id IN (SELECT record_id FROM sync_queue WHERE table_name = 'patients' AND status != 'synced')
                ORDER BY id DESC
            """)
            unknown_rows = cursor.fetchall()
            sqlite_manager.return_connection(conn)
            
            # دمج المجهولين مع النتائج
            unknown_patients = [dict(row) for row in unknown_rows]
            # إضافة المجهولين للنتائج (تجنب التكرار إذا كانوا موجودين)
            existing_ids = {p.get('patient_id') for p in result}
            for unknown in unknown_patients:
                if unknown.get('patient_id') not in existing_ids:
                    result.append(unknown)
            
            # ترتيب حسب id تنازلي
            result.sort(key=lambda x: x.get('id', 0), reverse=True)
            
            # تطبيق pagination
            result = result[offset:offset + limit]
            
        except Exception as e:
            error_logger.error(f"Error fetching unknown patients: {e}")

        # إذا لم يكن هناك نتائج من PostgreSQL، أو كان force_sqlite=True: اقرأ من SQLite مباشرة
        if not result and (not self.is_online() or force_sqlite):
            try:
                last_id = self._page_cache.get(offset)
                conn = sqlite_manager.get_connection(authorized=True)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                if last_id is not None and not force_sqlite:
                    cursor.execute("SELECT *, (SELECT COUNT(*) FROM patients) as total_count FROM patients WHERE id < ? ORDER BY id DESC LIMIT ?", (last_id, limit))
                else:
                    cursor.execute("SELECT *, (SELECT COUNT(*) FROM patients) as total_count FROM patients ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset))
                rows = cursor.fetchall()
                sqlite_manager.return_connection(conn)
                result = [dict(row) for row in rows]
            except Exception as e:
                self.log_error("SQLite Read Error", e)
                return []
        
        self.cache.set(cache_key, result)
        return result
    

    def search_patient_by_id(self, p_id):
        """بحث عن مريض بالرقم الموحد مع كاش محلي
        
        ملاحظة: للمرضى المجهولين، يتم البحث في SQLite دائماً لأنهم Local Only
        """
        # try cache first
        cache_key = f"patient_id_{p_id}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached
        
        # التحقق إذا كان مريض مجهول (يحتوي على U- في الرقم)
        is_unknown_patient = '-U-' in p_id or p_id.startswith('U-')
        
        # للمرضى المجهولين: البحث في SQLite فقط
        if is_unknown_patient:
            try:
                conn = sqlite_manager.get_connection(authorized=True)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM patients WHERE patient_id=?", (p_id,))
                row = cursor.fetchone()
                sqlite_manager.return_connection(conn)
                if row:
                    result = dict(row)
                    self.cache.set(cache_key, result)
                    return result
                return None
            except Exception as e:
                self.log_error("search_patient_by_id (unknown)", e)
                return None
            
        # للمرضى المعروفين: البحث في PostgreSQL أولاً (إذا كان Online)
        if self.is_online():
            conn = None
            try:
                conn = get_connection()
                if conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT * FROM patients WHERE patient_id = %s", (p_id,))
                    row = cursor.fetchone()
                    if row:
                        cols = [d[0] for d in cursor.description]
                        result = {cols[i]: v for i, v in enumerate(row)}
                        self.cache.set(cache_key, result)
                        cursor.close()
                        return result
                    cursor.close()
            except Exception as e:
                error_logger.error(f"Online search_patient_by_id failed: {e}")
            finally:
                release_connection(conn)
            
        # Fallback to Local SQLite للمرضى المعروفين
        try:
            conn = sqlite_manager.get_connection(authorized=True)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM patients WHERE patient_id=?", (p_id,))
            row = cursor.fetchone()
            sqlite_manager.return_connection(conn)
            if row:
                result = dict(row)
                self.cache.set(cache_key, result)
                return result
            return None
        except Exception as e:
            error_logger.error(f"SQLite search_patient_by_id error: {e}")
            return None

    def search_patient_by_national_id(self, n_id):
        """بحث عن مريض بالرقم القومي مع Caching (هام لمنع التكرار)"""
        # Check cache first
        cache_key = f"nid_{n_id}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result
        
        result = None
        if self.is_online():
            conn = None
            try:
                conn = get_connection()
                if conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT * FROM patients WHERE national_id = %s", (n_id,))
                    row = cursor.fetchone()
                    cols = [d[0] for d in cursor.description]
                    cursor.close()
                    if row:
                        result = {cols[i]: v for i, v in enumerate(row)}
                        self.cache.set(cache_key, result)
                        return result
            except Exception as e:
                system_logger.warning(f"PG search by national_id failed: {e}")
            finally:
                release_connection(conn)

        try:
            conn = sqlite_manager.get_connection(authorized=True)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM patients WHERE national_id=?", (n_id,))
            row = cursor.fetchone()
            sqlite_manager.return_connection(conn)
            result = dict(row) if row else None
            if result:
                self.cache.set(cache_key, result)
            return result
        except Exception as e:
            error_logger.error(f"SQLite search by national_id error: {e}")
            return None
    def log_error(self, ctx, e):
        """تسجيل الأخطاء بمركزية"""
        error_logger.error(f"Error in {ctx}: {e}")
        print(f"❌ خطأ في {ctx}: {e}")

    def parse_egyptian_national_id(self, national_id):
        """استخراج البيانات من الرقم القومي المصري مع تحقق كامل (تاريخ، جنس، محافظة)"""
        return NationalIDParser.parse(national_id)

    def check_recent_visit(self, p_id):
        """التحقق مما إذا كان المريض قد زار أي مستشفى في آخر 24 ساعة"""
        try:
            conn = sqlite_manager.get_connection(authorized=True)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # البحث عن أحدث زيارة في آخر 24 ساعة
            one_day_ago = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute("""
                SELECT hospital_name, admission_time 
                FROM visits 
                WHERE patient_id = ? AND admission_time >= ?
                ORDER BY admission_time DESC LIMIT 1
            """, (p_id, one_day_ago))
            
            row = cursor.fetchone()
            sqlite_manager.return_connection(conn)
            
            if row:
                return dict(row)
            return None
        except Exception as e:
            error_logger.error(f"Error checking recent visit: {e}")
            return None

    def generate_immutable_patient_id(self, patient_data: dict) -> str:
        """
        Generate immutable patient ID based on national_id + birth_date.
        
        Format: P-{hash(national_id + birth_date)}
        
        This ID is:
        - Unique
        - Immutable
        - Non-guessable
        - Prevents duplicates
        
        Args:
            patient_data: Dictionary containing at least national_id and birth_date
            
        Returns:
            Generated patient_id string
        """
        national_id = patient_data.get('national_id', '').strip()
        birth_date = patient_data.get('birth_date', '').strip()
        
        if not national_id:
            # Fallback for unknown patients
            hospital_code = patient_data.get('hospital', 'UNK')
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
            unique_str = f"{hospital_code}-U-{timestamp}"
            logger.warning(f"Generating unknown patient ID: {unique_str}")
        else:
            unique_str = f"{national_id}-{birth_date}"
        
        # Generate hash
        hash_input = unique_str.encode('utf-8')
        hash_result = hashlib.sha256(hash_input).hexdigest()[:12].upper()
        
        patient_id = f"P-{hash_result}"
        
        system_logger.info(f"Generated immutable patient_id: {patient_id}")
        return patient_id

    def add_patient(self, data, user_id="system", callback=None):
        """
        إضافة مريض جديد مع البصمة والمزامنة والبيانات الطبية
        
        CRITICAL UPDATE: Now uses centralized write queue for thread-safe operations.
        Includes callback for UI notification.
        """
        # CRITICAL: Generate immutable patient_id if not provided
        if 'patient_id' not in data or not data.get('patient_id'):
            data['patient_id'] = self.generate_immutable_patient_id(data)
            system_logger.info(f"Generated new immutable patient_id: {data['patient_id']}")
        
        # Add fingerprint and metadata
        data['fingerprint'] = self.generate_fingerprint(data['name'], data['birth_date'], data['national_id'])
        
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data.setdefault('created_by', user_id)
        data.setdefault('created_at', now)
        data.setdefault('updated_at', now)
        data.setdefault('version', 1)
        
        # Ensure medical fields exist
        medical_fields = ['blood_type', 'chronic_diseases', 'chronic_medications', 'allergies', 'medical_notes']
        for field in medical_fields:
            data.setdefault(field, '')
        
        # Check for potential duplicates (read operation - OK to do outside queue)
        possible_duplicates = []
        if data.get('national_id'):
            try:
                with self.sqlite_manager.cursor() as (cursor, conn):
                    cursor.execute(
                        "SELECT patient_id, name, national_id, birth_date FROM patients WHERE national_id=?",
                        (data['national_id'],)
                    )
                    rows = cursor.fetchall()
                    possible_duplicates.extend(rows)
            except Exception as e:
                error_logger.error(f"Error checking potential duplicate patients: {e}")
        
        duplication_warnings = []
        for row in possible_duplicates:
            try:
                p_id, p_name, p_nid, p_bd = row
                duplication_warnings.append(
                    f"مريض محتمل مكرر: {p_name} ({p_nid}) تاريخ ميلاد {p_bd} - رقم موحد {p_id}"
                )
            except Exception:
                continue
        
        # CRITICAL: Use write queue for thread-safe insert
        def on_patient_added(result, error=None):
            """Callback after patient is added to database."""
            if error:
                error_logger.error(f"Failed to add patient {data['patient_id']}: {error}")
            else:
                system_logger.info(f"✅ Patient added successfully: {data['patient_id']}")
                
                # Create initial visit after patient is added
                admission_data = {
                    'entry_type': data.get('entry_type', 'عيادات'),
                    'department': data.get('current_complaint', ''),
                    'chief_complaint': data.get('current_complaint', ''),
                    'chronic_diseases': data.get('chronic_diseases', ''),
                    'chronic_medications': data.get('chronic_medications', ''),
                    'allergies': data.get('allergies', ''),
                    'notes': ''
                }
                self.create_visit(
                    data['patient_id'], 
                    data.get('hospital', ''), 
                    HOSPITALS.get(data.get('hospital', ''), data.get('hospital', '')),
                    admission_data,
                    user_id=user_id
                )
                
                # Trigger sync
                self.trigger_sync('add_patient', 'patients', data['patient_id'], data)
            
            # Trigger caller callback if provided
            if callback:
                try:
                    # Pass duplication_warnings to the callback
                    callback(bool(result), error, duplication_warnings)
                except Exception as e:
                    error_logger.error(f"Error in add_patient callback: {e}")
        
        # Create a clean dictionary for the patients table to avoid schema mismatch
        patient_data_for_db = dict(data)
        patient_data_for_db.pop('entry_type', None)
        
        # Enqueue the write operation
        request = WriteRequest(
            operation=WriteOperation.UPSERT,
            table='patients',
            data=patient_data_for_db,
            priority=10,  # High priority for patient creation
            callback=on_patient_added
        )
        
        success = self.write_queue_manager.enqueue_write(request)
        
        if duplication_warnings:
            system_logger.warning(
                f"Possible duplicate patients detected for national_id={data.get('national_id','')}: "
                + " | ".join(duplication_warnings)
            )
            
        return success, duplication_warnings

    def add_unknown_patient(self, hospital_code, user_id="system"):
        """
        إضافة مريض مجهول الهوية - باستخدام طابور العمليات (Write Queue)
        
        - يتم توليد patient_id بالشكل: HOSPITAL-U-XXXXXX
        - يتم توليد اسم تلقائي: "مجهول X"
        - is_unknown = 1
        """
        try:
            # 1) توليد بيانات المريض (عمليات قراءة - مسموح بها)
            patient_id = self.generate_patient_id(hospital_code, is_unknown=True)
            
            # نحسب عدد المجهولين الحاليين
            with self.sqlite_manager.cursor() as (cursor, conn):
                cursor.execute("""
                    SELECT COUNT(*) FROM patients 
                    WHERE hospital = ? AND patient_id LIKE '%-U-%'
                """, (hospital_code,))
                count = cursor.fetchone()[0]
                unknown_index = count + 1
                name = f"مجهول {unknown_index}"
            
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            unique_fingerprint = hashlib.sha256(f"{patient_id}{now}".encode()).hexdigest()
            
            data = {
                'patient_id': patient_id, 'name': name, 'national_id': None,
                'birth_date': '', 'phone': '', 'address': '', 'hospital': hospital_code,
                'registration_date': now, 'fingerprint': unique_fingerprint,
                'blood_type': '', 'chronic_diseases': '', 'chronic_medications': '',
                'allergies': '', 'medical_notes': '', 'current_complaint': '',
                'is_unknown': 1, 'created_by': user_id, 'created_at': now,
                'updated_at': now, 'version': 1
            }
            
            # 2) إرسال الطلب لطابور العمليات
            request = WriteRequest(
                operation=WriteOperation.INSERT,
                table='patients',
                data=data,
                priority=10
            )
            success = self.write_queue_manager.enqueue_write(request)
            
            # 3) إضافة الزيارة فوراً بعد التأكد من إضافة المريض (كطلب منفصل بنفس الأولوية)
            if success:
                visit_uuid = str(uuid.uuid4())
                visit_data = {
                    'patient_id': patient_id, 'hospital_code': hospital_code,
                    'hospital_name': HOSPITALS.get(hospital_code, hospital_code),
                    'entry_type': 'طوارئ', 'chief_complaint': 'مريض مجهول الهوية',
                    'admission_time': now, 'visit_uuid': visit_uuid,
                    'created_by': user_id, 'created_at': now
                }
                visit_request = WriteRequest(
                    operation=WriteOperation.INSERT,
                    table='visits',
                    data=visit_data,
                    priority=9
                )
                self.write_queue_manager.enqueue_write(visit_request)
                
            return success, patient_id, name
        except Exception as e:
            error_logger.error(f"Error adding unknown patient (queued): {e}")
            return False, None, str(e)

    def _create_visit_local_only(self, patient_id, hospital_code, hospital_name, admission_data, user_id="system"):
        """
        إنشاء زيارة في SQLite فقط (بدون مزامنة مع PostgreSQL)
        للاستخدام مع المرضى المجهولين
        """
        try:
            conn = sqlite_manager.get_connection(authorized=True)
            cursor = conn.cursor()
            
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            cursor.execute("""
                INSERT INTO visits 
                (patient_id, hospital_code, hospital_name, entry_type, admission_department,
                 chief_complaint, chronic_diseases, chronic_medications, allergies, admission_notes,
                 admission_time, created_by, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                patient_id, hospital_code, hospital_name,
                admission_data.get('entry_type', 'عيادات'),
                admission_data.get('department', ''),
                admission_data.get('chief_complaint', ''),
                admission_data.get('chronic_diseases', ''),
                admission_data.get('chronic_medications', ''),
                admission_data.get('allergies', ''),
                admission_data.get('admission_notes', ''),
                now, user_id, now
            ))
            
            conn.commit()
            sqlite_manager.return_connection(conn)
            
            return True
        except Exception as e:
            error_logger.error(f"Error creating local-only visit: {e}")
            return False

    def identify_unknown_patient(self, unknown_patient_id, real_name, national_id, user_id="system"):
        """
        التحقق من هوية مريض مجهول ودمجه مع سجل موجود أو تحديثه
        
        CASE 1: الرقم القومي غير موجود → تحديث نفس السجل
        CASE 2: الرقم القومي موجود → Soft Merge (نقل الزيارات)
        
        Returns: (success, message, new_patient_id)
        """
        with sqlite_write_lock:
            try:
                conn = sqlite_manager.get_connection(authorized=True)
                cursor = conn.cursor()
                
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # التحقق من وجود المريض المجهول
                cursor.execute("""
                    SELECT patient_id, name, is_unknown FROM patients 
                    WHERE patient_id = ? AND is_unknown = 1
                """, (unknown_patient_id,))
                unknown_row = cursor.fetchone()
                
                if not unknown_row:
                    return False, "المريض غير موجود أو ليس مجهول الهوية", None
                
                # البحث عن مريض بنفس الرقم القومي
                cursor.execute("""
                    SELECT patient_id, name FROM patients 
                    WHERE national_id = ? AND is_unknown = 0
                """, (national_id,))
                existing_row = cursor.fetchone()
                
                if existing_row:
                    # CASE 2: Soft Merge - نسخ الزيارات للمريض الموجود (تبقى في المجهول أيضاً)
                    real_patient_id = existing_row[0]
                    
                    # نسخ الزيارات للمريض الموجود (الزيارات تبقى في ملف المجهول أيضاً)
                    # نجلب الزيارات أولاً ثم ننسخها واحدة بواحدة مع UUID جديد
                    cursor.execute("SELECT * FROM visits WHERE patient_id = ?", (unknown_patient_id,))
                    visits_to_copy = cursor.fetchall()
                    visits_copied = 0
                    
                    for visit in visits_to_copy:
                        new_uuid = str(uuid.uuid4())
                        cursor.execute("""
                            INSERT INTO visits 
                            (patient_id, hospital_code, hospital_name, entry_type, admission_time, 
                             admission_department, chief_complaint, initial_diagnosis, chronic_diseases,
                             chronic_medications, allergies, admission_notes, discharge_time,
                             discharge_doctor, final_diagnosis, discharge_medications, surgery_performed,
                             surgery_name, visit_uuid, created_by, created_at, updated_at)
                            VALUES (?, ?, ?, ?, datetime(?, '+1 second'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            real_patient_id, visit['hospital_code'], visit['hospital_name'], 
                            visit['entry_type'], visit['admission_time'],
                            visit['admission_department'], visit['chief_complaint'], 
                            visit['initial_diagnosis'], visit['chronic_diseases'],
                            visit['chronic_medications'], visit['allergies'], 
                            visit['admission_notes'], visit['discharge_time'],
                            visit['discharge_doctor'], visit['final_diagnosis'], 
                            visit['discharge_medications'], visit['surgery_performed'],
                            visit['surgery_name'], new_uuid, 
                            visit['created_by'], visit['created_at'], visit['updated_at']
                        ))
                        visits_copied += 1
                    
                    # تحديث سجل المجهول
                    cursor.execute("""
                        UPDATE patients 
                        SET status = 'merged',
                            merged_into = ?,
                            identified_at = ?,
                            is_unknown = 0
                        WHERE patient_id = ?
                    """, (real_patient_id, now, unknown_patient_id))
                    
                    conn.commit()
                    sqlite_manager.return_connection(conn)
                    
                    # مسح الكاش لتحديث البيانات
                    self.cache.invalidate(f"patient_id_{unknown_patient_id}")
                    self.cache.invalidate()
                    
                    system_logger.info(
                        f"Unknown patient {unknown_patient_id} merged into {real_patient_id}. "
                        f"Visits copied: {visits_copied}"
                    )
                    
                    return True, f"تم دمج المريض مع {existing_row[1]} ({real_patient_id})", real_patient_id
                
                else:
                    # CASE 1: إنشاء مريض جديد برقم موحد طبيعي (بدون U-)
                    # توليد رقم موحد جديد للمريض
                    hospital_code = unknown_row[0].split('-U-')[0] if '-U-' in unknown_patient_id else 'GEN'
                    new_patient_id = self.generate_patient_id(hospital_code)
                    
                    # توليد بصمة فريدة جديدة للمريض الجديد
                    new_fingerprint = hashlib.sha256(f"{new_patient_id}{now}".encode()).hexdigest()
                    
                    # إنشاء المريض الجديد في SQLite
                    cursor.execute("""
                        INSERT INTO patients 
                        (patient_id, name, national_id, birth_date, age, gender, phone, address, hospital, 
                         registration_date, fingerprint, blood_type, chronic_diseases, 
                         chronic_medications, allergies, medical_notes, current_complaint,
                         is_unknown, created_by, created_at, updated_at, version)
                        SELECT ?, ?, ?, birth_date, age, gender, phone, address, hospital, 
                               registration_date, ?, blood_type, chronic_diseases, 
                               chronic_medications, allergies, medical_notes, current_complaint,
                               0, created_by, created_at, ?, version
                        FROM patients WHERE patient_id = ?
                    """, (new_patient_id, real_name, national_id, new_fingerprint, now, unknown_patient_id))
                    
                    # نسخ الزيارات للمريض الجديد (الزيارات تبقى في ملف المجهول أيضاً)
                    # نجلب الزيارات أولاً ثم ننسخها واحدة بواحدة مع UUID جديد
                    cursor.execute("SELECT * FROM visits WHERE patient_id = ?", (unknown_patient_id,))
                    visits_to_copy = cursor.fetchall()
                    visits_copied = 0
                    
                    for visit in visits_to_copy:
                        new_uuid = str(uuid.uuid4())
                        cursor.execute("""
                            INSERT INTO visits 
                            (patient_id, hospital_code, hospital_name, entry_type, admission_time, 
                             admission_department, chief_complaint, initial_diagnosis, chronic_diseases,
                             chronic_medications, allergies, admission_notes, discharge_time,
                             discharge_doctor, final_diagnosis, discharge_medications, surgery_performed,
                             surgery_name, visit_uuid, created_by, created_at, updated_at)
                            VALUES (?, ?, ?, ?, datetime(?, '+1 second'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            new_patient_id, visit['hospital_code'], visit['hospital_name'], 
                            visit['entry_type'], visit['admission_time'],
                            visit['admission_department'], visit['chief_complaint'], 
                            visit['initial_diagnosis'], visit['chronic_diseases'],
                            visit['chronic_medications'], visit['allergies'], 
                            visit['admission_notes'], visit['discharge_time'],
                            visit['discharge_doctor'], visit['final_diagnosis'], 
                            visit['discharge_medications'], visit['surgery_performed'],
                            visit['surgery_name'], new_uuid, 
                            visit['created_by'], visit['created_at'], visit['updated_at']
                        ))
                        visits_copied += 1
                    
                    # تحديث سجل المجهول ليشير للمريض الجديد
                    cursor.execute("""
                        UPDATE patients 
                        SET status = 'merged',
                            merged_into = ?,
                            identified_at = ?,
                            is_unknown = 0
                        WHERE patient_id = ?
                    """, (new_patient_id, now, unknown_patient_id))
                    
                    conn.commit()
                    sqlite_manager.return_connection(conn)
                    
                    # مسح الكاش لتحديث البيانات
                    self.cache.invalidate(f"patient_id_{unknown_patient_id}")
                    self.cache.invalidate(f"patient_id_{new_patient_id}")
                    self.cache.invalidate()
                    
                    # مزامنة المريض الجديد إلى PostgreSQL
                    if self.is_online():
                        try:
                            pg_conn = get_connection()
                            if pg_conn:
                                pg_cursor = pg_conn.cursor()
                                # جلب بيانات المريض الجديد
                                patient_data = self.search_patient_by_id(new_patient_id)
                                if patient_data:
                                    # إدراج في PostgreSQL
                                    pg_cursor.execute("""
                                        INSERT INTO patients 
                                        (patient_id, name, national_id, birth_date, age, gender, phone, address, hospital, 
                                         registration_date, created_by, created_at, updated_at)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    """, (
                                        patient_data['patient_id'], patient_data['name'], 
                                        patient_data['national_id'], patient_data.get('birth_date', ''),
                                        patient_data.get('age'), patient_data.get('gender'),
                                        patient_data.get('phone', ''), patient_data.get('address', ''),
                                        patient_data.get('hospital', ''), patient_data.get('registration_date', now),
                                        patient_data.get('created_by', user_id), 
                                        patient_data.get('created_at', now), now
                                    ))
                                    pg_conn.commit()
                                    pg_cursor.close()
                                    system_logger.info(f"New patient synced to PostgreSQL: {new_patient_id}")
                                release_connection(pg_conn)
                        except Exception as e:
                            error_logger.error(f"Failed to sync new patient to PostgreSQL: {e}")
                            # إضافة لطابور المزامنة
                            self.add_to_sync_queue('add_patient', {'patient_id': new_patient_id})
                    else:
                        # إضافة لطابور المزامنة إذا كان Offline
                        self.add_to_sync_queue('add_patient', {'patient_id': new_patient_id})
                    
                    system_logger.info(f"Unknown patient {unknown_patient_id} converted to new patient {new_patient_id}")
                    
                    return True, f"✅ تم إنشاء مريض جديد: {real_name}\n🔢 الرقم الموحد: {new_patient_id}", new_patient_id
                
            except Exception as e:
                error_logger.error(f"Error identifying unknown patient: {e}")
                return False, f"خطأ: {str(e)}", None

    def check_recent_visit_hospital(self, p_id, hospital_code):
        """التحقق مما إذا كان المريض قد زار هذا المستشفى مسبقاً"""
        try:
            conn = sqlite_manager.get_connection(authorized=True)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM visits WHERE patient_id=? AND hospital_code=?", (p_id, hospital_code))
            count = cursor.fetchone()[0]
            sqlite_manager.return_connection(conn)
            return count > 0
        except Exception as e:
            error_logger.error(f"Error checking recent visit by hospital: {e}")
            return False

    def check_permission(self, p_id, user_info, operation='edit'):
        """التحقق من صلاحية التعديل/الحذف (24 ساعة + نفس المستشفى)"""
        if not user_info:
            return True, "" # للتوافق مع العمليات النظامية
        
        if user_info.get('role') == 'admin':
            return True, ""
            
        # إذا كانت العملية هي تسجيل زيارة فقط، فهي مسموحة دائماً في منطق HIS الموحد
        if operation == 'create_visit':
            return True, ""
            
        patient = self.search_patient_by_id(p_id)
        if not patient:
            return False, "السجل غير موجود."
            
        # 1. التحقق من المستشفى أو وجود زيارة سابقة (للتعديل)
        has_visit = self.check_recent_visit_hospital(p_id, user_info.get('hospital_code'))
        is_original_hosp = patient.get('hospital') == user_info.get('hospital_code')
        
        if not is_original_hosp and not has_visit:
            return False, "يمكن تعديل الملف الطبي فقط إذا زار المريض المستشفى الحالية."
            
        # 2. التحقق من الوقت (24 ساعة) للتعديلات الأساسية
        try:
            reg_date_str = patient.get('registration_date')
            if reg_date_str:
                # محاولة تجربة التنسيق الكامل أولاً
                try:
                    reg_date = datetime.strptime(reg_date_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    reg_date = datetime.strptime(reg_date_str, "%Y-%m-%d")
                    
                diff = datetime.now() - reg_date
                if diff.total_seconds() > 24 * 3600:
                    return False, "لا يمكن تعديل أو حذف السجل بعد مرور 24 ساعة على تسجيله."
        except Exception as e:
            error_logger.error(f"Error checking time permission: {e}")
            
        return True, ""

    def update_patient(self, p_id, data, user_id="system", user_info=None, callback=None):
        """
        تحديث بيانات مريض مع نظام الصلاحيات والـ Audit Log
        
        CRITICAL UPDATE: Now uses centralized write queue for thread-safe operations.
        Includes callback for UI notification.
        """
        # التحقق من الصلاحيات
        if user_info:
            allowed, msg = self.check_permission(p_id, user_info)
            if not allowed:
                self.log_audit(user_info.get('username', 'system'), 'update_denied', 'patients', p_id)
                return False, msg
                
        # تحديث البصمة عند تغيير البيانات الأساسية
        data['fingerprint'] = self.generate_fingerprint(data['name'], data['birth_date'], data['national_id'])
        data.setdefault('updated_at', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        # CRITICAL: Use write queue for thread-safe update
        def on_patient_updated(result, error=None):
            """Callback after patient is updated in database."""
            if error:
                error_logger.error(f"Failed to update patient {p_id}: {error}")
            else:
                system_logger.info(f"✅ Patient updated successfully: {p_id}")
                
                # Invalidate cache
                self.cache.invalidate(f"patient:{p_id}")
                self.cache.invalidate()  # Invalidate all search results
                
                # Sync update call
                self.trigger_sync('update_patient', 'patients', p_id, {'id': p_id, 'data': update_data})
            
            # Trigger caller callback if provided
            if callback:
                try:
                    callback(bool(result), error)
                except Exception as e:
                    error_logger.error(f"Error in update_patient callback: {e}")
        
        # Prepare update data (only changed fields + patient_id)
        update_data = {
            'patient_id': p_id,
            'name': data.get('name'),
            'national_id': data.get('national_id'),
            'birth_date': data.get('birth_date'),
            'age': data.get('age'),
            'gender': data.get('gender'),
            'phone': data.get('phone'),
            'address': data.get('address'),
            'fingerprint': data.get('fingerprint'),
            'blood_type': data.get('blood_type', ''),
            'chronic_diseases': data.get('chronic_diseases', ''),
            'chronic_medications': data.get('chronic_medications', ''),
            'allergies': data.get('allergies', ''),
            'medical_notes': data.get('medical_notes', ''),
            'current_complaint': data.get('current_complaint', ''),
            'updated_at': data.get('updated_at')
        }
        
        # Enqueue the update operation
        request = WriteRequest(
            operation=WriteOperation.UPDATE,
            table='patients',
            data=update_data,
            where_clause='patient_id = ?',
            where_params=(p_id,),
            priority=5,  # Medium priority for updates
            callback=on_patient_updated
        )
        
        success = self.write_queue_manager.enqueue_write(request)
        if not success:
            error_logger.error(f"Failed to enqueue update for patient {p_id}")
            return False, "Failed to queue update operation"
            
        return True, ""

    def delete_patient(self, p_id, user_id="system", user_info=None, callback=None):
        """
        حذف مريض مع نظام الصلاحيات والـ Audit Log
        
        CRITICAL UPDATE: Now uses centralized write queue for thread-safe operations.
        Includes callback for UI notification.
        """
        # التحقق من الصلاحيات
        if user_info:
            allowed, msg = self.check_permission(p_id, user_info)
            if not allowed:
                self.log_audit(user_info.get('username', 'system'), 'delete_denied', 'patients', p_id)
                return False, msg
        
        # CRITICAL: Use write queue for thread-safe delete
        def on_patient_deleted(result, error=None):
            """Callback after patient is deleted from database."""
            if error:
                error_logger.error(f"Failed to delete patient {p_id}: {error}")
            else:
                system_logger.info(f"✅ Patient deleted successfully: {p_id}")
                # Invalidate cache
                self.cache.invalidate()
                # Sync delete call
                self.trigger_sync('delete_patient', 'patients', p_id, {'id': p_id})
            
            # Trigger caller callback if provided
            if callback:
                try:
                    callback(bool(result), error)
                except Exception as e:
                    error_logger.error(f"Error in delete_patient callback: {e}")
        
        # Enqueue the delete operation
        request = WriteRequest(
            operation=WriteOperation.DELETE,
            table='patients',
            data={},  # No data needed for DELETE
            where_clause='patient_id = ?',
            where_params=(p_id,),
            priority=8,  # High priority for deletes
            callback=on_patient_deleted
        )
        
        success = self.write_queue_manager.enqueue_write(request)
        if not success:
            error_logger.error(f"Failed to enqueue delete for patient {p_id}")
            return False, "Failed to queue delete operation"
        
        return True, ""

    # الأعمدة المسموح بالبحث فيها (حماية من SQL Injection)
    _ALLOWED_DISTINCT_COLUMNS = {
        'patients': {'name', 'national_id', 'phone', 'address', 'hospital', 'blood_type',
                     'chronic_diseases', 'chronic_medications', 'allergies', 'governorate', 'gender',
                     'medical_notes', 'current_complaint'},
        'visits':   {'hospital_code', 'hospital_name', 'entry_type', 'admission_department',
                     'chief_complaint', 'initial_diagnosis', 'final_diagnosis', 'discharge_status',
                     'discharge_doctor', 'created_by'},
    }

    def get_distinct_values(self, column, table='patients', limit=100):
        """جلب قيم فريدة لعمود معين لدعم الإكمال التلقائي"""
        # Bug fix: التحقق من العمود والجدول لمنع SQL Injection
        allowed_cols = self._ALLOWED_DISTINCT_COLUMNS.get(table, set())
        if column not in allowed_cols:
            error_logger.warning(f"get_distinct_values: rejected column='{column}' table='{table}'")
            return []

        # نفضل الجلب من PostgreSQL إذا كان متصلاً للحصول على أحدث البيانات العالمية
        if self.is_online():
            conn = None
            try:
                conn = get_connection()
                if conn:
                    cursor = conn.cursor()
                    # استخدام قائمة بيضاء للعمود والجدول (آمنة بعد التحقق أعلاه)
                    cursor.execute(
                        f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL AND {column} != '' LIMIT %s",
                        (limit,)
                    )
                    results = [row[0] for row in cursor.fetchall()]
                    cursor.close()
                    return results
            except Exception as e:
                error_logger.error(f"Error getting distinct values from PostgreSQL: {e}")
                raise
            finally:
                if conn: release_connection(conn)
        
        # Fallback to SQLite
        conn = None
        try:
            conn = sqlite_manager.get_connection(authorized=True)
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL AND {column} != '' LIMIT ?",
                (limit,)
            )
            results = [row[0] for row in cursor.fetchall()]
            return results
        except Exception as e:
            error_logger.error(f"Error getting distinct values for {column}: {e}")
            return []
        finally:
            if conn:
                sqlite_manager.return_connection(conn)

    def add_visit_history(self, data, user_id="system"):
        """
        إضافة سجل زيارة جديد مع معالجة كاملة للحقول
        
        CRITICAL UPDATE: Now uses centralized write queue for thread-safe operations.
        Includes system_state.mode guard for PostgreSQL operations.
        """
        try:
            # Prepare complete visit data
            visit_uuid = str(uuid.uuid4())
            visit_data = {
                'patient_id': data.get('patient_id', ''),
                'hospital_code': data.get('hospital', data.get('hospital_code', '')),
                'hospital_name': data.get('hospital_name', ''),
                'entry_type': data.get('entry_type', 'Clinic'),
                'admission_time': data.get('admission_time', data.get('date', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))),
                'admission_department': data.get('department', data.get('admission_department', '')),
                'chief_complaint': data.get('chief_complaint', ''),
                'initial_diagnosis': data.get('diagnosis', data.get('initial_diagnosis', '')),
                'chronic_diseases': data.get('chronic_diseases', ''),
                'chronic_medications': data.get('medications', data.get('chronic_medications', '')),
                'allergies': data.get('allergies', ''),
                'admission_notes': data.get('notes', data.get('admission_notes', '')),
                'visit_uuid': visit_uuid,
                'created_by': user_id,
                'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'updated_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'discharge_status': 'خروج تحسن'
            }
            
            # Validate required data
            if not visit_data['patient_id']:
                return False, "معرف المريض مفقود"
            if not visit_data['hospital_name']:
                return False, "اسم المستشفى مفقود"
            
            # Check if patient is unknown (should not sync)
            patient_id = visit_data['patient_id']
            is_unknown_patient = '-U-' in patient_id or patient_id.startswith('U-')
            
            # CRITICAL: Use write queue for thread-safe insert
            def on_visit_added(result, error=None):
                """Callback after visit is added to database."""
                if error:
                    error_logger.error(f"Failed to add visit for patient {patient_id}: {error}")
                    return
                
                visit_id = result if result else visit_uuid
                system_logger.info(f"✅ Visit added successfully: {visit_id}")
                
                # Sync to PostgreSQL if online and known patient (with system_state.mode guard)
                if not is_unknown_patient and system_state.mode == "ONLINE" and self.is_online():
                    try:
                        conn_pg = get_connection()
                        if conn_pg:
                            cursor = conn_pg.cursor()
                            cursor.execute("""
                                INSERT INTO visits (patient_id, hospital_code, hospital_name, entry_type, admission_time, 
                                    admission_department, chief_complaint, initial_diagnosis, chronic_diseases, 
                                    chronic_medications, allergies, admission_notes, discharge_status, visit_uuid, created_by, created_at, updated_at)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (visit_uuid) DO NOTHING
                            """, (visit_data['patient_id'], visit_data['hospital_code'], visit_data['hospital_name'],
                                  visit_data['entry_type'], visit_data['admission_time'], visit_data['admission_department'],
                                  visit_data['chief_complaint'], visit_data['initial_diagnosis'], visit_data['chronic_diseases'],
                                  visit_data['chronic_medications'], visit_data['allergies'], visit_data['admission_notes'],
                                  'خروج تحسن', visit_data['visit_uuid'], visit_data['created_by'], visit_data['created_at'], visit_data['updated_at']))
                            conn_pg.commit()
                            cursor.close()
                            self.log_audit(user_id, 'add_visit', 'visits', str(visit_id))
                            release_connection(conn_pg)
                            system_logger.info(f"PostgreSQL sync completed for visit {visit_id}")
                    except Exception as e:
                        error_logger.error(f"PostgreSQL visit sync failed: {e}")
                        # Will be retried by sync queue
                        self.trigger_sync('add_visit', 'visits', str(visit_id), visit_data)
                else:
                    # Queue for later sync if offline or unknown patient
                    if not is_unknown_patient:
                        self.trigger_sync('add_visit', 'visits', str(visit_id), visit_data)
                    else:
                        system_logger.info(f"Visit for unknown patient {patient_id} created locally only (no sync)")
            
            # Enqueue the visit insert operation
            request = WriteRequest(
                operation=WriteOperation.INSERT,
                table='visits',
                data=visit_data,
                priority=8,  # High priority for visits
                callback=on_visit_added
            )
            
            success = self.write_queue_manager.enqueue_write(request)
            
            if not success:
                error_logger.error(f"Failed to enqueue visit for patient {patient_id}")
                return False, "فشل إضافة الزيارة للطابور"
            
            # Return immediately - actual insert happens async in write queue
            return True, "تم إضافة الزيارة بنجاح"
            
        except Exception as e:
            self.log_error("add_visit_history", e)
            return False, f"خطأ في الحفظ: {str(e)}"

    def create_visit(self, patient_id, hospital_code, hospital_name, admission_data, user_id="system"):
        """إنشاء سجل زيارة جديد مع تجنب التكرار (UNIQUE constraint check)
        
        FIX #3: استخدام sqlite_write_lock لمنع database is locked
        """
        admission_time = admission_data.get('admission_time', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        # التحقق من وجود زيارة سابقة في نفس الوقت (منع التكرار)
        try:
            conn_check = sqlite_manager.get_connection(authorized=True)
            cursor_check = conn_check.cursor()
            cursor_check.execute(
                "SELECT visit_id FROM visits WHERE patient_id=? AND admission_time=? AND hospital_code=?",
                (patient_id, admission_time, hospital_code)
            )
            existing = cursor_check.fetchone()
            sqlite_manager.return_connection(conn_check)
            if existing:
                system_logger.warning(f"Duplicate visit attempt for patient {patient_id} at {admission_time}")
                return None
        except Exception as e:
            error_logger.error(f"Error checking visit uniqueness: {e}")
        
        # FIX #4: Generate unique visit_uuid for conflict resolution
        visit_uuid = str(uuid.uuid4())
        visit_data = {
            'patient_id': patient_id,
            'hospital_code': hospital_code,
            'hospital_name': hospital_name,
            'entry_type': admission_data.get('entry_type', 'Clinic'),
            'admission_time': admission_time,
            'admission_department': admission_data.get('department', ''),
            'chief_complaint': admission_data.get('chief_complaint', ''),
            'initial_diagnosis': admission_data.get('initial_diagnosis', ''),
            'chronic_diseases': admission_data.get('chronic_diseases', ''),
            'chronic_medications': admission_data.get('chronic_medications', ''),
            'allergies': admission_data.get('allergies', ''),
            'admission_notes': admission_data.get('notes', ''),
            'visit_uuid': visit_uuid,
            'discharge_time': None,
            'discharge_doctor': None,
            'discharge_status': 'خروج تحسن',
            'final_diagnosis': None,
            'discharge_medications': None,
            'surgery_performed': 0,
            'surgery_name': None,
            'created_by': user_id,
            'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'updated_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # FIX #3: استخدام sqlite_write_lock لضمان عدم حدوث race conditions
        with sqlite_write_lock:
            try:
                conn_l = sqlite_manager.get_connection(authorized=True)
                c_l = conn_l.cursor()
                c_l.execute("""
                    INSERT INTO visits (patient_id, hospital_code, hospital_name, entry_type, admission_time, 
                        admission_department, chief_complaint, initial_diagnosis, chronic_diseases, 
                        chronic_medications, allergies, admission_notes, surgery_performed, visit_uuid, created_by, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (visit_data['patient_id'], visit_data['hospital_code'], visit_data['hospital_name'],
                      visit_data['entry_type'], visit_data['admission_time'], visit_data['admission_department'],
                      visit_data['chief_complaint'], visit_data['initial_diagnosis'], visit_data['chronic_diseases'],
                      visit_data['chronic_medications'], visit_data['allergies'], visit_data['admission_notes'],
                      visit_data['surgery_performed'], visit_data['visit_uuid'], visit_data['created_by'], visit_data['created_at'], visit_data['updated_at']))
                conn_l.commit()
                visit_id = c_l.lastrowid
                sqlite_manager.return_connection(conn_l)
            except Exception as e:
                error_logger.error(f"Local storage create_visit error: {e}")
                return None

        # التحقق إذا كان المريض مجهول (لمنع المزامنة)
        is_unknown_patient = '-U-' in patient_id or patient_id.startswith('U-')
        
        # Queue for sync (فقط للمرضى المعروفين)
        if not is_unknown_patient:
            # trigger_sync will handle immediate sync if online
            self.trigger_sync('add_visit', 'visits', str(visit_id), visit_data)
        else:
            system_logger.info(f"Visit for unknown patient {patient_id} created locally only (no sync)")
        return visit_id

    def get_visits(self, patient_id, limit=50):
        """جلب جميع زيارات المريض مع دعم الـ Offline والمجهولين"""
        all_visits = []
        
        # جلب الزيارات من PostgreSQL إذا كان متصلاً (للمرضى المعروفين)
        is_unknown_patient = '-U-' in patient_id or patient_id.startswith('U-')
        
        if not is_unknown_patient and self.is_online():
            conn = None
            try:
                conn = get_connection()
                if conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT visit_id, patient_id, hospital_code, hospital_name, entry_type, admission_time, 
                            admission_department, chief_complaint, initial_diagnosis, admission_notes,
                            discharge_time, discharge_doctor, final_diagnosis, discharge_medications,
                            surgery_performed, surgery_name, created_by, created_at, updated_at
                        FROM visits WHERE patient_id = %s 
                        ORDER BY admission_time DESC 
                        LIMIT %s
                    """, (patient_id, limit))
                    cols = ['visit_id', 'patient_id', 'hospital_code', 'hospital_name', 'entry_type', 'admission_time', 
                            'admission_department', 'chief_complaint', 'initial_diagnosis', 'admission_notes',
                            'discharge_time', 'discharge_doctor', 'final_diagnosis', 'discharge_medications',
                            'surgery_performed', 'surgery_name', 'created_by', 'created_at', 'updated_at']
                    rows = cursor.fetchall()
                    cursor.close()
                    all_visits = [{cols[i]: v for i, v in enumerate(row)} for row in rows]
            except Exception as e:
                error_logger.error(f"PG Get Visits Error: {e}")
            finally:
                release_connection(conn)

        # جلب الزيارات من SQLite دائماً (للجميع: معروفين ومجهولين)
        try:
            conn = sqlite_manager.get_connection(authorized=True)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM visits WHERE patient_id=? ORDER BY admission_time DESC LIMIT ?", (patient_id, limit))
            rows = cursor.fetchall()
            sqlite_manager.return_connection(conn)
            sqlite_visits = [dict(row) for row in rows]
            
            # دمج الزيارات من SQLite مع النتائج (تجنب التكرار باستخدام visit_uuid)
            existing_uuids = {v.get('visit_uuid') for v in all_visits if v.get('visit_uuid')}
            for visit in sqlite_visits:
                if visit.get('visit_uuid') not in existing_uuids:
                    all_visits.append(visit)
            
            # إعادة ترتيب حسب admission_time تنازلي
            all_visits.sort(key=lambda x: x.get('admission_time', ''), reverse=True)
            
            return all_visits[:limit]
        except Exception as e:
            error_logger.error(f"SQLite Get Visits Error: {e}")
            return all_visits[:limit]

    def update_visit_discharge(self, visit_id, discharge_data, user_id="system"):
        """تحديث بيانات الخروج (Discharge) للزيارة
        
        CRITICAL UPDATE: Now uses centralized write queue for thread-safe operations.
        """
        update_data = {
            'discharge_time': discharge_data.get('discharge_time'),
            'discharge_doctor': discharge_data.get('discharge_doctor'),
            'discharge_department': discharge_data.get('discharge_department'),
            'discharge_status': discharge_data.get('discharge_status', 'خروج تحسن'),
            'final_diagnosis': discharge_data.get('final_diagnosis'),
            'discharge_medications': discharge_data.get('discharge_medications'),
            'surgery_performed': discharge_data.get('surgery_performed', 0),
            'surgery_name': discharge_data.get('surgery_name'),
            'transfer_hospital': discharge_data.get('transfer_hospital'),
            'transfer_department': discharge_data.get('transfer_department'),
            'data_entry_personnel': discharge_data.get('data_entry_personnel'),
            'updated_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        def on_discharge_updated(result, error=None):
            if error:
                error_logger.error(f"Failed to update visit discharge {visit_id}: {error}")
            else:
                system_logger.info(f"✅ Visit discharge updated successfully: {visit_id}")
                sync_payload = dict(update_data)
                sync_payload['visit_id'] = visit_id
                self.trigger_sync('update_visit_discharge', 'visits', str(visit_id), sync_payload)

        request = WriteRequest(
            operation=WriteOperation.UPDATE,
            table='visits',
            data=update_data,
            where_clause='visit_id = ?',
            where_params=(visit_id,),
            priority=5,
            callback=on_discharge_updated
        )
        self.write_queue_manager.enqueue_write(request)
        return True

    def update_visit_admission(self, visit_id, admission_data, user_id="system"):
        """تحديث بيانات الدخول للزيارة (بدون بيانات الخروج)
        
        CRITICAL UPDATE: Now uses centralized write queue for thread-safe operations.
        """
        update_data = {
            'entry_type': admission_data.get('entry_type'),
            'admission_time': admission_data.get('admission_time'),
            'admission_department': admission_data.get('admission_department'),
            'chief_complaint': admission_data.get('chief_complaint'),
            'initial_diagnosis': admission_data.get('initial_diagnosis'),
            'chronic_diseases': admission_data.get('chronic_diseases'),
            'chronic_medications': admission_data.get('chronic_medications'),
            'allergies': admission_data.get('allergies'),
            'admission_notes': admission_data.get('admission_notes'),
            'transfer_to_inpatient': admission_data.get('transfer_to_inpatient', 0),
            'transfer_reason': admission_data.get('transfer_reason'),
            'transfer_department': admission_data.get('transfer_department'),
            'data_entry_personnel': admission_data.get('data_entry_personnel'),
            'updated_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        def on_admission_updated(result, error=None):
            if error:
                error_logger.error(f"Failed to update visit admission {visit_id}: {error}")
            else:
                system_logger.info(f"✅ Visit admission updated successfully: {visit_id}")
                sync_payload = dict(update_data)
                sync_payload['visit_id'] = visit_id
                self.trigger_sync('update_visit_admission', 'visits', str(visit_id), sync_payload)

        request = WriteRequest(
            operation=WriteOperation.UPDATE,
            table='visits',
            data=update_data,
            where_clause='visit_id = ?',
            where_params=(visit_id,),
            priority=5,
            callback=on_admission_updated
        )
        self.write_queue_manager.enqueue_write(request)
        return True

    def authenticate_user(self, username, password):
        """التحقق من بيانات تسجيل الدخول باستخدام bcrypt فقط (بدون plain text fallback)"""
        # التحقق من حالة الحظر أولاً
        is_locked, remaining_time = login_protection.is_locked(username)
        if is_locked:
            error_logger.warning(f"User {username} is locked. Remaining time: {remaining_time}s")
            return None  # FIX #1: Return None only
        
        conn = None
        user = None
        login_attempt_log = f"Login attempt for user: {username}"
        
        try:
            # محاولة الاتصال بـ PostgreSQL أولاً
            try:
                conn = get_connection()
                if conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        SELECT username, password, hospital_code, role 
                        FROM users 
                        WHERE username=%s
                        LIMIT 1
                    ''', (username,))
                    
                    user = cursor.fetchone()
                    cursor.close()
                    
                    if user:
                        user_data = {'username': user[0], 'password_hash': user[1], 'hospital_code': user[2], 'role': user[3]}
                        # التحقق من كلمة المرور - تجربة bcrypt أولاً
                        if user_data['password_hash'].startswith('$2'):  # bcrypt hash
                            if PasswordHasher.verify_password(password, user_data['password_hash']):
                                columns = ['username', 'hospital_code', 'role']
                                data = dict(zip(columns, [user_data['username'], user_data['hospital_code'], user_data['role']]))
                                data.setdefault('role', 'hospital')
                                data.setdefault('hospital_code', 'ADMIN')
                                system_logger.info(f"{login_attempt_log} - SUCCESS (PostgreSQL)")
                                self.log_audit(username, 'login', 'users', username)
                                # تسجيل المحاولة الناجحة ومسح سجل الحظر
                                login_protection.record_attempt(username, success=True)
                                return data
                        else:  # Legacy plaintext - DEPRECATED (migration only)
                            if password == user_data['password_hash']:
                                # تحديث كلمة المرور إلى bcrypt
                                hashed = PasswordHasher.hash_password(password)
                                if hashed:
                                    try:
                                        cursor = conn.cursor()
                                        cursor.execute("UPDATE users SET password=%s WHERE username=%s", (hashed, username))
                                        conn.commit()
                                    except Exception as e:
                                        system_logger.warning(f"Failed to update password hash: {e}")
                                columns = ['username', 'hospital_code', 'role']
                                data = dict(zip(columns, [user_data['username'], user_data['hospital_code'], user_data['role']]))
                                data.setdefault('role', 'hospital')
                                data.setdefault('hospital_code', 'ADMIN')
                                system_logger.info(f"{login_attempt_log} - SUCCESS (PostgreSQL, migrated to bcrypt)")
                                self.log_audit(username, 'login', 'users', username)
                                # تسجيل المحاولة الناجحة ومسح سجل الحظر
                                login_protection.record_attempt(username, success=True)
                                return data
            except Exception as e:
                error_logger.warning(f"PostgreSQL authentication error: {e}")
            finally:
                release_connection(conn)
            
            # Fallback إلى SQLite المحلي
            try:
                conn = sqlite_manager.get_connection(authorized=True)
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT username, password, hospital_code, role 
                    FROM users 
                    WHERE username=?
                    LIMIT 1
                ''', (username,))
                
                user = cursor.fetchone()
                
                if user:
                    user_data = {'username': user[0], 'password_hash': user[1], 'hospital_code': user[2], 'role': user[3]}
                    # التحقق من كلمة المرور
                    if user_data['password_hash'].startswith('$2'):  # bcrypt hash
                        if PasswordHasher.verify_password(password, user_data['password_hash']):
                            columns = ['username', 'hospital_code', 'role']
                            data = dict(zip(columns, [user_data['username'], user_data['hospital_code'], user_data['role']]))
                            data.setdefault('role', 'hospital')
                            data.setdefault('hospital_code', 'ADMIN')
                            system_logger.info(f"{login_attempt_log} - SUCCESS (SQLite)")
                            sqlite_manager.return_connection(conn)
                            return data
                    else:  # plaintext للتوافقية العكسية
                        if password == user_data['password_hash']:
                            # تحديث كلمة المرور إلى bcrypt
                            hashed = PasswordHasher.hash_password(password)
                            if hashed:
                                try:
                                    cursor.execute("UPDATE users SET password=? WHERE username=?", (hashed, username))
                                    conn.commit()
                                except Exception as e:
                                    system_logger.warning(f"Failed to update password hash: {e}")
                            columns = ['username', 'hospital_code', 'role']
                            data = dict(zip(columns, [user_data['username'], user_data['hospital_code'], user_data['role']]))
                            data.setdefault('role', 'hospital')
                            data.setdefault('hospital_code', 'ADMIN')
                            system_logger.info(f"{login_attempt_log} - SUCCESS (SQLite, migrated to bcrypt)")
                            sqlite_manager.return_connection(conn)
                            return data
                sqlite_manager.return_connection(conn)
            except Exception as e:
                error_logger.error(f"SQLite authentication error: {e}")
            
            # تسجيل المحاولة الفاشلة
            login_protection.record_attempt(username, success=False)
            system_logger.warning(f"{login_attempt_log} - FAILED: Invalid credentials")
        except Exception as e:
            # تسجيل المحاولة الفاشلة في حالة خطأ غير متوقع
            login_protection.record_attempt(username, success=False)
            error_logger.error(f"Authentication error: {e}")
        
        return None

    def get_all_users(self):
        """جلب جميع المستخدمين من النظام (مع Fallback إلى SQLite عند الوضع Offline)"""
        # محاولة جلب البيانات من PostgreSQL أولاً
        if self.is_online():
            conn = None
            try:
                conn = get_connection()
                if conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT username, hospital_code, role FROM users ORDER BY role DESC, username")
                    rows = cursor.fetchall()
                    columns = ['username', 'hospital_code', 'role']
                    users = [{columns[i]: v for i, v in enumerate(row)} for row in rows]
                    cursor.close()
                    return users
            except Exception as e:
                error_logger.error(f"PostgreSQL get_all_users error: {e}")
            finally:
                release_connection(conn)

        # Fallback إلى SQLite المحلي عند انعدام الاتصال
        try:
            conn_sq = sqlite_manager.get_connection(authorized=True)
            conn_sq.row_factory = sqlite3.Row
            cursor_sq = conn_sq.cursor()
            cursor_sq.execute("SELECT username, hospital_code, role FROM users ORDER BY role DESC, username")
            rows = cursor_sq.fetchall()
            result = [dict(row) for row in rows]
            sqlite_manager.return_connection(conn_sq)
            return result
        except Exception as e:
            self.log_error("جلب المستخدمين (SQLite)", e)
            return []

# تم حذف فئة ActionButtonsDelegate بناءً على قرار الاعتماد على القائمة المنبثقة


class SimilarPatientDialog(QDialog):
    """نافذة منبثقة لعرض المرضى المشابهين"""
    
    def __init__(self, similar_patients, parent=None):
        super().__init__(parent)
        self.setWindowTitle("⚠️ تنبيه - مرضى مشابهون")
        self.setMinimumWidth(800)
        self.setMinimumHeight(400)
        self.setLayoutDirection(Qt.RightToLeft)
        
        self.setStyleSheet("""
            QDialog {
                background-color: #F8FAFC;
            }
            QLabel {
                color: #1E293B;
            }
            QTableWidget {
                background-color: white;
                border: none;
                border-radius: 10px;
                gridline-color: transparent;
                font-size: 14px;
            }
            QTableWidget::item {
                padding: 12px 16px;
                border-bottom: 1px solid #F1F5F9;
            }
            QTableWidget::item:selected {
                background-color: #EFF6FF;
                color: #1E40AF;
            }
            QHeaderView::section {
                background-color: #F1F5F9;
                color: #475569;
                padding: 14px 16px;
                font-size: 14px;
                font-weight: bold;
                border: none;
                border-bottom: 2px solid #E2E8F0;
            }
        """)
        
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        layout.setContentsMargins(25, 25, 25, 25)
        
        # بطاقة التحذير
        warning_card = QFrame()
        warning_card.setStyleSheet("""
            QFrame {
                background-color: #FFFBEB;
                border: 2px solid #FCD34D;
                border-radius: 12px;
                padding: 5px;
            }
        """)
        warning_layout = QHBoxLayout(warning_card)
        warning_layout.setContentsMargins(20, 15, 20, 15)
        
        icon_label = QLabel("⚠️")
        icon_label.setStyleSheet("font-size: 32px; background: transparent; border: none;")
        warning_layout.addWidget(icon_label)
        
        text_layout = QVBoxLayout()
        title = QLabel("تم العثور على مرضى مشابهين!")
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: #92400E; background: transparent; border: none;")
        
        subtitle = QLabel("يرجى المراجعة الدقيقة للتأكد من عدم تكرار البيانات قبل الإضافة. اضغط «استخدام» لاستيراد بيانات مريض موجود.")
        subtitle.setStyleSheet("font-size: 13px; color: #78350F; background: transparent; border: none;")
        subtitle.setWordWrap(True)
        
        text_layout.addWidget(title)
        text_layout.addWidget(subtitle)
        warning_layout.addLayout(text_layout)
        warning_layout.addStretch()
        
        layout.addWidget(warning_card)
        
        # إطار الجدول
        table_frame = QFrame()
        table_frame.setStyleSheet("""
            QFrame {
                background-color: white;
                border-radius: 12px;
                border: 1px solid #E2E8F0;
            }
        """)
        table_frame_layout = QVBoxLayout(table_frame)
        table_frame_layout.setContentsMargins(0, 0, 0, 0)
        table_frame_layout.setSpacing(0)
        
        # الجدول
        self.table = QTableWidget()
        headers = ["الرقم الموحد", "الاسم الكامل", "الرقم القومي", "الإجراء"]
        self.table.setColumnCount(len(headers))
        self.table.setHorizontalHeaderLabels(headers)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setShowGrid(False)
        self.table.verticalHeader().setVisible(False)
        self.table.verticalHeader().setDefaultSectionSize(60)
        self.table.setAlternatingRowColors(True)
        
        table_frame_layout.addWidget(self.table)
        layout.addWidget(table_frame)
        
        # أزرار
        button_box = QHBoxLayout()
        close_btn = QPushButton("✖  إغلاق")
        close_btn.setStyleSheet("""
            QPushButton {
                background-color: #F1F5F9;
                color: #475569;
                border: 1px solid #CBD5E1;
                border-radius: 8px;
                padding: 10px 25px;
                font-size: 14px;
                font-weight: bold;
                min-width: 120px;
            }
            QPushButton:hover {
                background-color: #E2E8F0;
                color: #1E293B;
            }
        """)
        close_btn.clicked.connect(self.reject)
        button_box.addStretch()
        button_box.addWidget(close_btn)
        layout.addLayout(button_box)
        
        self.populate_table(similar_patients)
    
    def populate_table(self, patients):
        self.table.setRowCount(len(patients))
        for row, p in enumerate(patients):
            # الرقم الموحد
            id_item = QTableWidgetItem(p['patient_id'])
            id_item.setFont(QFont("Arial", 12, QFont.Bold))
            id_item.setForeground(QColor("#3B82F6"))
            self.table.setItem(row, 0, id_item)
            
            # الاسم
            name_item = QTableWidgetItem(p['name'])
            name_item.setFont(QFont("Arial", 13, QFont.Bold))
            name_item.setForeground(QColor("#1E293B"))
            self.table.setItem(row, 1, name_item)
            
            # الرقم القومي
            nid_item = QTableWidgetItem(p['national_id'])
            nid_item.setFont(QFont("Arial", 12))
            nid_item.setForeground(QColor("#64748B"))
            self.table.setItem(row, 2, nid_item)
            
            use_btn = QPushButton("استخدام")
            use_btn.setCursor(Qt.PointingHandCursor)
            use_btn.setStyleSheet("""
                QPushButton {
                    background-color: #3B82F6;
                    color: white;
                    border-radius: 6px;
                    padding: 6px 15px;
                    font-weight: bold;
                }
                QPushButton:hover { background-color: #2563EB; }
            """)
            use_btn.clicked.connect(lambda chk=False, p=p: self.use_patient(p))
            self.table.setCellWidget(row, 3, use_btn)
            
        # ضبط عرض الأعمدة
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.Fixed)
        self.table.setColumnWidth(3, 150)

    def use_patient(self, patient):
        """تعبئة بيانات المريض المختار في النافذة الرئيسية"""
        parent = self.parent()
        if parent and hasattr(parent, 'entries'):
            parent.entries['name'].setText(patient.get('name', ''))
            parent.entries['national_id'].setText(patient.get('national_id', ''))
            parent.entries['patient_id'].setText(patient.get('patient_id', ''))
            parent.extract_birth_date()
        self.accept()

class AddVisitDialog(QDialog):
    """نافذة إضافة زيارة جديدة يدوياً بتصميم HIS احترافي"""
    def __init__(self, hospitals, patient_id, parent=None):
        super().__init__(parent)
        self.hospitals = hospitals
        self.patient_id = patient_id
        self.setWindowTitle("➕ تسجيل زيارة طبية جديدة")
        self.setFixedSize(500, 650)
        self.setLayoutDirection(Qt.RightToLeft)
        self.setStyleSheet("""
            QDialog { background-color: #F2F2F7; }
            QLabel { color: #1D1D1F; font-weight: 500; font-size: 15px; }
            QLineEdit, QComboBox, QDateEdit, QTextEdit {
                padding: 12px 16px;
                border: 1px solid #D2D2D7;
                border-radius: 12px;
                background-color: #FFFFFF;
                font-size: 15px;
                color: #1D1D1F;
            }
            QLineEdit:focus, QComboBox:focus, QDateEdit:focus, QTextEdit:focus {
                border-color: #007AFF;
                background-color: #F8FBFF;
            }
            QComboBox::drop-down { border: none; }
            QComboBox::down-arrow { image: none; border: none; }
        """)
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # 1. Header
        header = QFrame()
        header.setStyleSheet("background-color: #FFFFFF; border-bottom: 1px solid #D2D2D7; border-radius: 16px 16px 0 0;")
        header.setFixedHeight(80)
        h_layout = QHBoxLayout(header)
        title = QLabel("📝 تسجيل زيارة طبية")
        title.setStyleSheet("color: #1D1D1F; font-size: 20px; font-weight: 700;")
        h_layout.addWidget(title)
        h_layout.addStretch()
        main_layout.addWidget(header)

        # 2. Form Container
        form_parent = QWidget()
        form_layout = QVBoxLayout(form_parent)
        form_layout.setContentsMargins(30, 20, 30, 20)
        form_layout.setSpacing(20)

        grid = QGridLayout()
        grid.setSpacing(15)

        # المستشفى
        grid.addWidget(QLabel("🏥 المستشفى المضيف:"), 0, 0)
        self.hosp_combo = QComboBox()
        for code, name in self.hospitals.items():
            self.hosp_combo.addItem(name, code)
        grid.addWidget(self.hosp_combo, 0, 1)

        # التاريخ
        grid.addWidget(QLabel("📅 تاريخ الزيارة:"), 1, 0)
        self.date_input = QDateEdit()
        self.date_input.setCalendarPopup(True)
        self.date_input.setDate(QDate.currentDate())
        self.date_input.setDisplayFormat("yyyy-MM-dd")
        grid.addWidget(self.date_input, 1, 1)

        # القسم
        grid.addWidget(QLabel("🏢 القسم الطبي:"), 2, 0)
        self.dept_input = QLineEdit()
        self.dept_input.setPlaceholderText("مثال: الباطنة، الطوارئ...")
        grid.addWidget(self.dept_input, 2, 1)

        # الطبيب
        grid.addWidget(QLabel("👨‍⚕️ الطبيب المعالج:"), 3, 0)
        self.doctor_input = QLineEdit()
        self.doctor_input.setPlaceholderText("اسم الطبيب بالكامل")
        grid.addWidget(self.doctor_input, 3, 1)

        # نوع الدخول
        grid.addWidget(QLabel("📋 تصنيف الزيارة:"), 4, 0)
        self.type_combo = QComboBox()
        self.type_combo.addItem("عيادات خارجية", "Clinic")
        self.type_combo.addItem("طوارئ وحوادث", "Emergency")
        self.type_combo.addItem("دخول داخلي", "Inpatient")
        grid.addWidget(self.type_combo, 4, 1)

        # التشخيص
        grid.addWidget(QLabel("🩺 التشخيص المبدئي:"), 5, 0)
        self.diag_input = QLineEdit()
        self.diag_input.setPlaceholderText("أدخل التشخيص هنا")
        grid.addWidget(self.diag_input, 5, 1)

        # الأدوية
        grid.addWidget(QLabel("💊 الأدوية الموصوفة:"), 6, 0)
        self.meds_input = QLineEdit()
        self.meds_input.setPlaceholderText("الأدوية والجرعات")
        grid.addWidget(self.meds_input, 6, 1)

        # العلامات الحيوية
        grid.addWidget(QLabel("🌡️ العلامات الحيوية:"), 7, 0)
        vitals_layout = QHBoxLayout()
        vitals_layout.setSpacing(10) # مسافة بين حقول العلامات الحيوية
        
        self.bp_input = QLineEdit()
        self.bp_input.setPlaceholderText("ضغط الدم (مثال: 120/80)")
        self.bp_input.setMinimumWidth(120)
        
        self.pulse_input = QLineEdit()
        self.pulse_input.setPlaceholderText("النبض")
        self.pulse_input.setMinimumWidth(80)
        
        self.temp_input = QLineEdit()
        self.temp_input.setPlaceholderText("الحرارة")
        self.temp_input.setMinimumWidth(80)
        
        vitals_layout.addWidget(self.bp_input)
        vitals_layout.addWidget(self.pulse_input)
        vitals_layout.addWidget(self.temp_input)
        grid.addLayout(vitals_layout, 7, 1)

        # ضبط عرض العمود الثاني ليكون أوسع
        grid.setColumnStretch(1, 1)

        form_layout.addLayout(grid)

        # ملاحظات
        form_layout.addWidget(QLabel("📝 تفاصيل وملاحظات إضافية:"))
        self.notes_input = QTextEdit()
        self.notes_input.setPlaceholderText("اكتب أي تفاصيل طبية هنا...")
        self.notes_input.setMinimumHeight(100)
        form_layout.addWidget(self.notes_input)

        main_layout.addWidget(form_parent)

        # 3. Actions
        actions = QFrame()
        actions.setStyleSheet("background-color: #f1f5f9; border-top: 1px solid #e2e8f0;")
        a_layout = QHBoxLayout(actions)
        a_layout.setContentsMargins(30, 15, 30, 15)
        a_layout.setSpacing(15)

        save_btn = QPushButton("💾 حفظ الزيارة")
        save_btn.setCursor(Qt.PointingHandCursor)
        save_btn.setStyleSheet("""
            QPushButton { background-color: #2563eb; color: white; padding: 12px; border-radius: 8px; font-weight: bold; font-size: 15px; }
            QPushButton:hover { background-color: #1d4ed8; }
        """)
        save_btn.clicked.connect(self.handle_save)
        
        cancel_btn = QPushButton("إلغاء الأمر")
        cancel_btn.setCursor(Qt.PointingHandCursor)
        cancel_btn.setStyleSheet("""
            QPushButton { background-color: white; color: #64748b; padding: 12px; border-radius: 8px; border: 1px solid #cbd5e1; font-weight: bold; }
            QPushButton:hover { background-color: #f8fafc; }
        """)
        cancel_btn.clicked.connect(self.reject)
        
        a_layout.addWidget(save_btn, 2)
        a_layout.addWidget(cancel_btn, 1)
        main_layout.addWidget(actions)

    def handle_save(self):
        # التحقق من البيانات
        if not self.dept_input.text().strip() or not self.doctor_input.text().strip():
            QMessageBox.warning(self, "بيانات ناقصة", "يرجى إدخال القسم الطبي واسم الطبيب")
            return

        self.visit_data = {
            'patient_id': self.patient_id,
            'hospital': self.hosp_combo.currentData(),
            'hospital_name': self.hosp_combo.currentText(),
            'entry_type': self.type_combo.currentData(),
            'date': self.date_input.date().toString("yyyy-MM-dd"),
            'doctor': self.doctor_input.text().strip(),
            'department': self.dept_input.text().strip(),
            'diagnosis': self.diag_input.text().strip(),
            'medications': self.meds_input.text().strip(),
            'vitals': f"BP: {self.bp_input.text()} | Pulse: {self.pulse_input.text()} | Temp: {self.temp_input.text()}",
            'notes': self.notes_input.toPlainText().strip()
        }
        self.accept()

class EditMedicalSummaryDialog(QDialog):
    """نافذة تعديل الملخص الطبي من داخل سجل المريض"""
    def __init__(self, db, patient_data, parent=None):
        super().__init__(parent)
        self.db = db
        self.patient_data = patient_data
        self.patient_id = patient_data['patient_id']
        self.setWindowTitle("✏️ تعديل الملخص الطبي")
        self.setFixedSize(500, 450)
        self.setLayoutDirection(Qt.RightToLeft)
        self.setStyleSheet("""
            QDialog { background-color: #f8fafc; }
            QLabel { color: #475569; font-weight: bold; font-size: 13px; }
            QLineEdit {
                padding: 10px; border: 1px solid #cbd5e1;
                border-radius: 8px; background-color: white; font-size: 14px;
            }
            QLineEdit:focus { border: 2px solid #3b82f6; outline: none; }
            QComboBox { padding: 10px; border: 1px solid #cbd5e1; border-radius: 8px; background-color: white; font-size: 14px; }
        """)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)

        form_layout = QFormLayout()
        form_layout.setSpacing(15)

        self.blood_type = QComboBox()
        self.blood_type.addItems(["-", "A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"])
        self.blood_type.setCurrentText(self.patient_data.get('blood_type', '-'))
        form_layout.addRow("🩸 فصيلة الدم:", self.blood_type)

        self.chronic_diseases = QLineEdit(self.patient_data.get('chronic_diseases', ''))
        self.chronic_diseases.setObjectName("chronic_diseases_input")  # FIXED: Set object name for exclusion logic
        form_layout.addRow("📋 أمراض مزمنة:", self.chronic_diseases)
        
        # Attach auto-completion to chronic diseases field
        if 'medical_completer' in globals():
            medical_completer.attach_to_input(self.chronic_diseases, 'chronic_diseases')

        self.chronic_meds = QLineEdit(self.patient_data.get('chronic_medications', ''))
        self.chronic_meds.setObjectName("chronic_medications_input")  # FIXED: Set object name for exclusion logic
        form_layout.addRow("💊 أدوية مزمنة:", self.chronic_meds)
        
        # Attach auto-completion to chronic medications field
        if 'medical_completer' in globals():
            medical_completer.attach_to_input(self.chronic_meds, 'medications')

        self.allergies = QLineEdit(self.patient_data.get('allergies', ''))
        self.allergies.setObjectName("allergies_input")  # FIXED: Set object name for exclusion logic
        form_layout.addRow("⚠️ الحساسية:", self.allergies)
        
        # Attach auto-completion to allergies field
        if 'medical_completer' in globals():
            medical_completer.attach_to_input(self.allergies, 'allergies')

        self.current_complaint = QLineEdit(self.patient_data.get('current_complaint', ''))
        self.current_complaint.setObjectName("current_complaint_input")  # FIXED: Set object name for exclusion logic
        form_layout.addRow("🤒 الشكوى الحالية:", self.current_complaint)
        
        # Attach auto-completion to current complaint field
        if 'medical_completer' in globals():
            medical_completer.attach_to_input(self.current_complaint, 'complaints')

        self.notes = QLineEdit(self.patient_data.get('medical_notes', ''))
        self.notes.setObjectName("medical_notes_input")  # FIXED: Set object name for exclusion logic
        form_layout.addRow("📝 ملاحظات طبية:", self.notes)
        
        # Attach auto-completion to medical notes field
        if 'medical_completer' in globals():
            medical_completer.attach_to_input(self.notes, 'notes')

        layout.addLayout(form_layout)

        # أزرار الإجراءات
        btn_layout = QHBoxLayout()
        save_btn = QPushButton("💾 تحديث الملخص")
        save_btn.setStyleSheet("background-color: #10b981; color: white; padding: 10px; border-radius: 8px; font-weight: bold;")
        save_btn.clicked.connect(self.handle_save)
        
        cancel_btn = QPushButton("إلغاء")
        cancel_btn.setStyleSheet("background-color: white; color: #64748b; padding: 10px; border-radius: 8px; border: 1px solid #cbd5e1; font-weight: bold;")
        cancel_btn.clicked.connect(self.reject)
        
        btn_layout.addWidget(save_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

    def handle_save(self):
        """Save medical summary updates with proper error handling"""
        try:
            # Collect updated medical data
            updated_medical_data = {
                'blood_type': self.blood_type.currentText(),
                'chronic_diseases': self.chronic_diseases.text().strip(),
                'chronic_medications': self.chronic_meds.text().strip(),
                'allergies': self.allergies.text().strip(),
                'current_complaint': self.current_complaint.text().strip(),
                'medical_notes': self.notes.text().strip()
            }
            
            # Merge old data with medical updates to preserve essential fields
            updated_data = self.patient_data.copy()
            updated_data.update(updated_medical_data)
            
            # Ensure all required fields exist (entry_type is ONLY in visits, not patients)
            required_fields = ['name', 'national_id', 'birth_date', 'phone', 'address']
            for field in required_fields:
                if field not in updated_data or not updated_data[field]:
                    updated_data[field] = self.patient_data.get(field, '')
            
            # Extract user information from parent window
            parent_window = self.parent()
            user_id = 'system'
            user_info = None
            
            if parent_window:
                # Try to get user data from parent window
                if hasattr(parent_window, 'user_data'):
                    user_id = parent_window.user_data.get('username', 'system')
                    user_info = parent_window.user_data
                # If parent is a child window, try to get grandparent
                elif hasattr(parent_window, 'parent') and parent_window.parent():
                    grandparent = parent_window.parent()
                    if hasattr(grandparent, 'user_data'):
                        user_id = grandparent.user_data.get('username', 'system')
                        user_info = grandparent.user_data

            # Update the patient data
            success, msg = self.db.update_patient(self.patient_id, updated_data, user_id=user_id, user_info=user_info)
            
            if success:
                QMessageBox.information(self, "نجاح", "✅ تم تحديث الملخص الطبي بنجاح")
                
                # Update parent window if it has refresh method
                if parent_window:
                    self.patient_data.update(updated_medical_data)
                    if hasattr(parent_window, 'refresh_medical_summary'):
                        parent_window.refresh_medical_summary()
                    if hasattr(parent_window, 'patient_data'):
                        parent_window.patient_data.update(updated_medical_data)
                
                self.accept()
            else:
                error_msg = msg if msg else "حدث خطأ غير معروف أثناء التحديث"
                QMessageBox.warning(self, "فشل التحديث", f"❌ لا يمكن تحديث السجل:\n{error_msg}")
                
        except Exception as e:
            error_logger.error(f"Error in EditMedicalSummaryDialog.handle_save: {e}")
            QMessageBox.critical(self, "خطأ", f"❌ حدث خطأ أثناء حفظ البيانات:\n{str(e)}")


class PatientLookupWorker(QObject):
    """
    Background worker for patient existence lookup.
    Runs database query in separate thread to keep UI responsive.
    """
    finished = Signal(bool, dict)  # Emits (exists, patient_data)
    
    def __init__(self, national_id, db_manager):
        super().__init__()
        self.national_id = national_id
        self.db_manager = db_manager
    
    def run(self):
        """Execute database lookup in background thread using DatabaseManager"""
        try:
            # FIXED: Use DatabaseManager to search both PG and SQLite with caching
            patient = self.db_manager.search_patient_by_national_id(self.national_id)
            
            if patient:
                self.finished.emit(True, patient)
            else:
                self.finished.emit(False, {})
        except Exception as e:
            error_logger.debug(f"Error in patient lookup: {e}")
            self.finished.emit(False, {})  # Safe default


class VerifyPatientDialog(QDialog):
    """نافذة التحقق من هوية المريض المجهول"""
    def __init__(self, db, patient_id, parent=None):
        super().__init__(parent)
        self.db = db
        self.patient_id = patient_id
        self.result = None
        
        self.setWindowTitle("✔ التحقق من هوية المريض")
        self.setFixedSize(450, 300)
        self.setLayoutDirection(Qt.RightToLeft)
        self.setStyleSheet("""
            QDialog { background-color: #f8fafc; }
            QLabel { color: #475569; font-weight: bold; font-size: 13px; }
            QLineEdit {
                padding: 12px; border: 1px solid #cbd5e1;
                border-radius: 8px; background-color: white; font-size: 14px;
            }
            QLineEdit:focus { border: 2px solid #3b82f6; outline: none; }
        """)
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(25, 25, 25, 25)
        layout.setSpacing(20)
        
        # عنوان
        title = QLabel("🔍 التحقق من هوية المريض المجهول")
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: #1e293b;")
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)
        
        # وصف
        desc = QLabel("أدخل البيانات الحقيقية للمريض:\n\n"
                     "• إذا كان الرقم القومي غير موجود: سيتم تحديث نفس السجل\n"
                     "• إذا كان الرقم القومي موجود: سيتم دمج الزيارات مع الملف الحقيقي")
        desc.setStyleSheet("font-size: 12px; color: #64748b; font-weight: normal;")
        desc.setWordWrap(True)
        layout.addWidget(desc)
        
        # الاسم الرباعي
        name_layout = QVBoxLayout()
        name_label = QLabel("الاسم الرباعي:")
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("مثال: أحمد محمد علي حسن")
        name_layout.addWidget(name_label)
        name_layout.addWidget(self.name_input)
        layout.addLayout(name_layout)
        
        # الرقم القومي
        nid_layout = QVBoxLayout()
        nid_label = QLabel("الرقم القومي:")
        self.nid_input = QLineEdit()
        self.nid_input.setPlaceholderText("أدخل 14 رقم")
        self.nid_input.setObjectName("national_id_input")  # For exclusion logic
        self.nid_input.setMaxLength(14)  # Limit to 14 digits
        nid_layout.addWidget(nid_label)
        nid_layout.addWidget(self.nid_input)
        layout.addLayout(nid_layout)
        
        # Connect text change signal for auto-focus functionality
        self.nid_input.textChanged.connect(self.handle_nid_input)
        
        # أزرار
        btn_layout = QHBoxLayout()
        
        verify_btn = QPushButton("✔ تأكيد التحقق")
        verify_btn.setStyleSheet("""
            QPushButton {
                background-color: #10b981; color: white; 
                padding: 12px 20px; border-radius: 8px; 
                font-weight: bold; font-size: 14px;
            }
            QPushButton:hover { background-color: #059669; }
        """)
        verify_btn.clicked.connect(self.handle_verify)
        
        cancel_btn = QPushButton("إلغاء")
        cancel_btn.setStyleSheet("""
            QPushButton {
                background-color: white; color: #64748b; 
                padding: 12px 20px; border-radius: 8px; 
                border: 1px solid #cbd5e1; font-weight: bold;
            }
        """)
        cancel_btn.clicked.connect(self.reject)
        
        btn_layout.addWidget(verify_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)
    
    def handle_verify(self):
        name = self.name_input.text().strip()
        national_id = self.nid_input.text().strip()
        
        # التحقق من البيانات
        if not name:
            QMessageBox.warning(self, "خطأ", "الاسم مطلوب")
            return
        
        if len(name.split()) < 4:
            QMessageBox.warning(self, "خطأ", "يجب إدخال الاسم رباعياً على الأقل")
            return
        
        if not national_id:
            QMessageBox.warning(self, "خطأ", "الرقم القومي مطلوب")
            return
        
        if len(national_id) != 14 or not national_id.isdigit():
            QMessageBox.warning(self, "خطأ", "الرقم القومي يجب أن يكون 14 رقماً")
            return
        
        self.result = {
            'name': name,
            'national_id': national_id
        }
        self.accept()
    
    def handle_nid_input(self, text):
        """
        Handle national ID input changes.
        Auto-focus to name field when 14 digits entered and patient not found.
        Uses background thread for non-blocking database lookup.
        """
        # Safety check: Prevent multiple simultaneous lookups - مع التحقق من حذف الكائن C++
        if hasattr(self, 'lookup_thread') and self.lookup_thread is not None:
            try:
                if self.lookup_thread.isRunning():
                    return
            except RuntimeError:
                # Internal C++ object already deleted
                self.lookup_thread = None
        
        # Only process if we have exactly 14 digits
        if len(text) == 14 and text.isdigit():
            # Create background worker for non-blocking lookup
            # Pass self.db to worker for PG + SQLite support
            self.lookup_worker = PatientLookupWorker(text, self.db)
            self.lookup_thread = QThread()
            
            # Move worker to thread
            self.lookup_worker.moveToThread(self.lookup_thread)
            
            # Connect signals
            self.lookup_thread.started.connect(self.lookup_worker.run)
            self.lookup_worker.finished.connect(self.on_lookup_finished)
            
            # Cleanup connections
            self.lookup_worker.finished.connect(self.lookup_thread.quit)
            self.lookup_worker.finished.connect(self.lookup_worker.deleteLater)
            self.lookup_thread.finished.connect(self.lookup_thread.deleteLater)
            
            # Start background thread
            self.lookup_thread.start()
    
    def on_lookup_finished(self, exists, patient_data):
        """
        Callback when patient lookup completes.
        Runs in main UI thread.
        
        Args:
            exists: True if patient found in database, False otherwise
            patient_data: Dictionary containing patient details if exists
        """
        if not exists:
            # Bug fix: Only set focus if current input length is still 14
            # (Prevents focusing if user deleted a digit during lookup)
            if len(self.nid_input.text()) == 14:
                # Small delay to ensure UI is ready
                QTimer.singleShot(50, lambda: self.name_input.setFocus())
    
    def get_result(self):
        return self.result


class PatientHistoryDialog(QDialog):
    """نافذة ملف المريض الاحترافي بتصميم آبل بريميوم"""
    def __init__(self, db, patient_id, hospitals, parent=None):
        super().__init__(parent)
        self.db = db
        self.patient_id = patient_id
        self.hospitals = hospitals
        self.all_history = []
        self.filtered_history = []
        
        # جلب بيانات المريض الأساسية
        self.patient_data = self.db.search_patient_by_id(patient_id)
        
        self.setWindowTitle(f"السجل الطبي الكامل — {self.patient_data['name']}")
        self.setMinimumSize(1100, 850)
        self.setLayoutDirection(Qt.RightToLeft)
        self.setStyleSheet("""
            QDialog { background-color: #F2F2F7; }
            QLabel { color: #1D1D1F; font-family: 'Segoe UI', sans-serif; }
            QScrollArea { background: transparent; border: none; }
            QWidget#scrollContent { background: transparent; }
        """)
        self.init_ui()
        self.load_history()

    def init_ui(self):
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)

        # 1. Premium Header
        header = QFrame()
        header.setFixedHeight(120)
        header.setStyleSheet("background-color: #FFFFFF; border-bottom: 1px solid #D2D2D7;")
        h_layout = QHBoxLayout(header)
        h_layout.setContentsMargins(40, 0, 40, 0)

        info_vbox = QVBoxLayout()
        name_lbl = QLabel(self.patient_data['name'])
        name_lbl.setStyleSheet("font-size: 26px; font-weight: 700;")
        
        sub_info = QLabel(f"ID: {self.patient_id}  •  {self.patient_data['national_id']}")
        sub_info.setStyleSheet("font-size: 14px; color: #86868B; font-weight: 500;")
        
        info_vbox.addWidget(name_lbl)
        info_vbox.addWidget(sub_info)
        h_layout.addLayout(info_vbox)
        h_layout.addStretch()

        add_btn = QPushButton("➕ إضافة زيارة")
        add_btn.setFixedSize(160, 44)
        add_btn.setCursor(Qt.PointingHandCursor)
        add_btn.setStyleSheet("""
            QPushButton {
                background-color: #007AFF;
                color: white;
                border-radius: 22px;
                font-size: 15px;
                font-weight: 600;
            }
            QPushButton:hover { background-color: #0071E3; }
        """)
        add_btn.clicked.connect(self.show_add_visit)
        h_layout.addWidget(add_btn)
        self.main_layout.addWidget(header)

        # 2. Controls Section (Search & Filter)
        controls_frame = QFrame()
        controls_frame.setStyleSheet("background-color: #FFFFFF; border-bottom: 1px solid #E5E5E7;")
        controls_layout = QHBoxLayout(controls_frame)
        controls_layout.setContentsMargins(40, 15, 40, 15)
        controls_layout.setSpacing(15)

        self.history_search = QLineEdit()
        self.history_search.setPlaceholderText("🔍 بحث في السجل الطبي...")
        self.history_search.setFixedHeight(44)
        self.history_search.setStyleSheet("""
            QLineEdit {
                background-color: #F2F2F7;
                border: none;
                border-radius: 12px;
                padding: 0 15px;
                font-size: 15px;
            }
        """)
        self.history_search.textChanged.connect(self.filter_data)
        controls_layout.addWidget(self.history_search, 2)

        self.type_filter = QComboBox()
        self.type_filter.addItems(["كل الأنواع", "Clinic", "Emergency", "Inpatient"])
        self.type_filter.setFixedHeight(44)
        self.type_filter.setStyleSheet("QComboBox { background-color: #F2F2F7; border-radius: 12px; padding: 0 15px; }")
        self.type_filter.currentIndexChanged.connect(self.filter_data)
        controls_layout.addWidget(self.type_filter, 1)

        self.main_layout.addWidget(controls_frame)

        # 3. Content Area (Scrollable Timeline)
        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.content_widget = QWidget()
        self.content_widget.setObjectName("scrollContent")
        self.timeline_layout = QVBoxLayout(self.content_widget)
        self.timeline_layout.setContentsMargins(40, 30, 40, 30)
        self.timeline_layout.setSpacing(20)
        
        self.scroll.setWidget(self.content_widget)
        self.main_layout.addWidget(self.scroll)

    def load_history(self):
        """جلب بيانات السجل وتحميلها في الذاكرة"""
        self.all_history = self.db.get_visits(self.patient_id, limit=200)
        self.filtered_history = self.all_history.copy()
        self.display_history()

    def filter_data(self):
        """تصفية البيانات بناءً على البحث والنوع"""
        search_text = self.history_search.text().lower()
        type_text = self.type_filter.currentText()
        
        self.filtered_history = []
        for visit in self.all_history:
            matches_search = (
                search_text in str(visit.get('chief_complaint', '')).lower() or
                search_text in str(visit.get('initial_diagnosis', '')).lower() or
                search_text in str(visit.get('hospital_name', '')).lower() or
                search_text in str(visit.get('admission_department', '')).lower()
            )
            
            matches_type = (type_text == "كل الأنواع" or visit.get('entry_type') == type_text)
            
            if matches_search and matches_type:
                self.filtered_history.append(visit)
        
        self.display_history()

    def display_history(self):
        """عرض السجل في الواجهة كبطاقات آبل"""
        # Clear existing
        while self.timeline_layout.count():
            item = self.timeline_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        
        if not self.filtered_history:
            empty_lbl = QLabel("لا توجد سجلات مطابقة للبحث")
            empty_lbl.setStyleSheet("color: #8E8E93; font-size: 16px; margin-top: 50px;")
            empty_lbl.setAlignment(Qt.AlignCenter)
            self.timeline_layout.addWidget(empty_lbl)
            return

        for visit in self.filtered_history:
            card = VisitCard(visit, self.open_visit_details)
            self.timeline_layout.addWidget(card)
            
        self.timeline_layout.addStretch()

    def open_visit_details(self, visit):
        from MAIN import VisitDetailsDialog # Local import to avoid circular dependency
        dialog = VisitDetailsDialog(self.db, self.patient_id, visit, self.hospitals, self)
        if dialog.exec() == QDialog.Accepted:
            self.load_history()

    def show_add_visit(self):
        # Implementation for adding visit
        if hasattr(self.parent(), 'add_new_visit'):
            self.parent().add_new_visit()
            self.load_history()
        else:
            QMessageBox.information(self, "تنبيه", "يرجى استخدام شاشة ملف المريض لإضافة زيارة جديدة.")

    def show_edit_medical_summary(self):
        dialog = EditMedicalSummaryDialog(self.db, self.patient_data, self)
        if dialog.exec() == QDialog.Accepted:
            self.patient_data = self.db.search_patient_by_id(self.patient_id)
            # Potentially refresh parts of the UI if needed

class VisitDetailsDialog(QDialog):
    """نافذة تفاصيل الزيارة الطبية بتصميم آبل بريميوم - نسخة محسنة"""
    def __init__(self, db, patient_id, visit_data, hospitals, parent=None):
        super().__init__(parent)
        self.db = db
        self.patient_id = patient_id
        self.visit_data = visit_data
        self.visit_id = visit_data.get('visit_id')
        self.hospitals = hospitals
        self.is_closed = bool(visit_data.get('discharge_time'))
        
        self.setWindowTitle(f"تفاصيل الزيارة — {self.visit_id}")
        self.setMinimumSize(1100, 850)
        self.setLayoutDirection(Qt.RightToLeft)
        
        self.setStyleSheet("""
            QDialog { background-color: #F2F2F7; }
            QLabel { color: #1D1D1F; font-family: 'Segoe UI', system-ui, sans-serif; font-size: 14px; }
            QTabWidget::pane { border: none; background-color: #F2F2F7; margin-top: -1px; }
            QTabBar::tab {
                background-color: transparent;
                color: #8E8E93;
                padding: 12px 30px;
                font-weight: 600;
                font-size: 15px;
                border-bottom: 2px solid transparent;
            }
            QTabBar::tab:selected {
                color: #007AFF;
                border-bottom: 2px solid #007AFF;
            }
            QTabBar::tab:hover {
                color: #1D1D1F;
            }
            QLineEdit, QTextEdit, QComboBox {
                background-color: #FFFFFF;
                border: 1px solid #D2D2D7;
                border-radius: 10px;
                padding: 10px 15px;
                font-size: 14px;
                color: #1D1D1F;
            }
            QLineEdit:focus, QTextEdit:focus, QComboBox:focus {
                border-color: #007AFF;
                background-color: #FFFFFF;
            }
            QLineEdit:disabled, QTextEdit:disabled, QComboBox:disabled {
                background-color: #FBFBFC;
                color: #86868B;
            }
            QPushButton#primary {
                background-color: #007AFF;
                color: white;
                border-radius: 12px;
                padding: 12px 25px;
                font-weight: 600;
                font-size: 15px;
                border: none;
            }
            QPushButton#primary:hover { background-color: #0062CC; }
            QPushButton#primary:pressed { background-color: #0051A8; }
            
            QPushButton#secondary {
                background-color: #E8E8ED;
                color: #007AFF;
                border-radius: 12px;
                padding: 12px 25px;
                font-weight: 600;
                font-size: 15px;
                border: none;
            }
            QPushButton#secondary:hover { background-color: #D1D1D6; }
            
            QScrollArea { border: none; background-color: transparent; }
            QWidget#scrollContent { background-color: transparent; }
            QCheckBox { font-size: 14px; color: #1D1D1F; }
            QPushButton#close-danger {
                background-color: #FF3B30;
                color: white;
                border-radius: 12px;
                padding: 12px 25px;
                font-weight: 700;
                font-size: 15px;
                border: none;
            }
            QPushButton#close-danger:hover { background-color: #D32F2F; }
        """)
        self.init_ui()

    def init_ui(self):
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)

        # Header
        header = QFrame()
        header.setFixedHeight(100)
        header.setStyleSheet("background-color: #FFFFFF; border-bottom: 1px solid #E5E5EA;")
        h_layout = QHBoxLayout(header)
        h_layout.setContentsMargins(25, 0, 25, 0)

        title_vbox = QVBoxLayout()
        title_vbox.setSpacing(5)
        title_vbox.setAlignment(Qt.AlignVCenter)

        hosp_name = QLabel(f"🏥 {self.visit_data.get('hospital_name', 'مستشفى غير محدد')}")
        hosp_name.setStyleSheet("font-size: 22px; font-weight: 800; color: #1D1D1F;")

        meta = QLabel(f"الزيارة #{self.visit_id}  •  قسم {self.visit_data.get('admission_department', '—')}  •  {self.visit_data.get('admission_time', '')}")
        meta.setStyleSheet("font-size: 13px; color: #8E8E93; font-weight: 500;")

        title_vbox.addWidget(hosp_name)
        title_vbox.addWidget(meta)
        h_layout.addLayout(title_vbox)
        h_layout.addStretch()

        status_color = "#34C759" if self.is_closed else "#FF9500"
        status_bg = "#EBF9EE" if self.is_closed else "#FFF4E5"
        status_frame = QFrame()
        status_frame.setStyleSheet(f"background-color: {status_bg}; border-radius: 14px; border: none;")
        sl = QHBoxLayout(status_frame)
        sl.setContentsMargins(14, 7, 14, 7)
        sl.setSpacing(7)
        dot = QLabel("●")
        dot.setStyleSheet(f"color: {status_color}; font-size: 11px;")
        stxt = QLabel("زيارة مغلقة" if self.is_closed else "قيد المعالجة")
        stxt.setStyleSheet(f"color: {status_color}; font-weight: 700; font-size: 14px;")
        sl.addWidget(dot)
        sl.addWidget(stxt)
        h_layout.addWidget(status_frame, 0, Qt.AlignVCenter)

        self.main_layout.addWidget(header)

        # Tabs
        self.tabs = QTabWidget()
        self.main_layout.addWidget(self.tabs, 1)

        # Admission Tab
        admission_scroll = QScrollArea()
        admission_scroll.setWidgetResizable(True)
        admission_content = QWidget()
        admission_content.setObjectName("scrollContent")
        admission_layout = QVBoxLayout(admission_content)
        admission_layout.setContentsMargins(20, 15, 20, 15)
        admission_layout.setSpacing(10)
        self.create_admission_section(admission_layout)
        admission_scroll.setWidget(admission_content)
        self.tabs.addTab(admission_scroll, "دخول المريض")

        # Discharge Tab
        discharge_scroll = QScrollArea()
        discharge_scroll.setWidgetResizable(True)
        discharge_content = QWidget()
        discharge_content.setObjectName("scrollContent")
        discharge_layout = QVBoxLayout(discharge_content)
        discharge_layout.setContentsMargins(20, 15, 20, 15)
        discharge_layout.setSpacing(10)
        self.create_discharge_section(discharge_layout)
        discharge_scroll.setWidget(discharge_content)
        self.tabs.addTab(discharge_scroll, "خروج المريض")

        # Actions Bar
        actions_frame = QFrame()
        actions_frame.setFixedHeight(75)
        actions_frame.setStyleSheet("background-color: #FFFFFF; border-top: 1px solid #E5E5EA;")
        actions_layout = QHBoxLayout(actions_frame)
        actions_layout.setContentsMargins(25, 0, 25, 0)
        actions_layout.setSpacing(10)

        if not self.is_closed:
            save_btn = QPushButton("✓  حفظ التغييرات")
            save_btn.setObjectName("primary")
            save_btn.setFixedHeight(48)
            save_btn.setMinimumWidth(200)
            save_btn.setStyleSheet("""
                QPushButton {
                    background-color: #007AFF;
                    color: white;
                    border-radius: 14px;
                    padding: 0 30px;
                    font-weight: 700;
                    font-size: 16px;
                    border: none;
                    letter-spacing: 0.3px;
                }
                QPushButton:hover { background-color: #0062CC; }
                QPushButton:pressed { background-color: #004EA8; }
            """)
            save_btn.clicked.connect(self.handle_save)
            actions_layout.addWidget(save_btn)

            close_visit_btn = QPushButton("✕  إغلاق الزيارة نهائياً")
            close_visit_btn.setFixedHeight(48)
            close_visit_btn.setMinimumWidth(200)
            close_visit_btn.setStyleSheet("""
                QPushButton {
                    background-color: #FF3B30;
                    color: white;
                    border-radius: 14px;
                    padding: 0 30px;
                    font-weight: 700;
                    font-size: 16px;
                    border: none;
                }
                QPushButton:hover { background-color: #D32F2F; }
                QPushButton:pressed { background-color: #B71C1C; }
            """)
            close_visit_btn.clicked.connect(self.handle_save_discharge_and_close)
            actions_layout.addWidget(close_visit_btn)

        actions_layout.addStretch()

        exit_btn = QPushButton("إغلاق")
        exit_btn.setFixedHeight(48)
        exit_btn.setStyleSheet("""
            QPushButton {
                background-color: #F2F2F7;
                color: #3C3C43;
                border-radius: 14px;
                padding: 0 25px;
                font-weight: 600;
                font-size: 15px;
                border: none;
            }
            QPushButton:hover { background-color: #E5E5EA; }
        """)
        exit_btn.clicked.connect(self.reject)
        actions_layout.addWidget(exit_btn)

        self.main_layout.addWidget(actions_frame)

        if self.is_closed:
            self.set_read_only()

    def create_card(self, title, icon=""):
        card = QFrame()
        card.setStyleSheet("""
            QFrame {
                background-color: #FFFFFF;
                border-radius: 16px;
                border: none;
            }
        """)
        vbox = QVBoxLayout(card)
        vbox.setContentsMargins(25, 20, 25, 25)
        vbox.setSpacing(18)

        if title:
            header_lbl = QLabel(f"{icon}  {title}")
            header_lbl.setStyleSheet("font-size: 16px; font-weight: 700; color: #3C3C43;")
            vbox.addWidget(header_lbl)

        return card, vbox

    def create_admission_section(self, layout):
        # --- Admission Date (auto, editable) ---
        lbl_date = QLabel("تاريخ الدخول")
        lbl_date.setStyleSheet("font-size: 13px; color: #8E8E93; font-weight: 600; margin-bottom: 4px;")
        default_adm = self.visit_data.get('admission_time') or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.admission_time_edit = QLineEdit(str(default_adm))
        self.admission_time_edit.setFixedHeight(46)
        self.admission_time_edit.setPlaceholderText("YYYY-MM-DD HH:MM:SS")
        layout.addWidget(lbl_date)
        layout.addWidget(self.admission_time_edit)

        # --- Row: Visit type & department ---
        row1 = QHBoxLayout()
        row1.setSpacing(15)

        col1 = QVBoxLayout()
        lbl1 = QLabel("نوع الزيارة")
        lbl1.setStyleSheet("font-size: 13px; color: #8E8E93; font-weight: 600; margin-bottom: 4px;")
        self.entry_type = QComboBox()
        self.entry_type.addItems(["عيادات", "طوارئ", "داخلي"])
        current_type = self.visit_data.get('entry_type', 'عيادات')
        # Map old English values to Arabic
        type_map = {"Clinic": "عيادات", "Emergency": "طوارئ", "Inpatient": "داخلي"}
        self.entry_type.setCurrentText(type_map.get(current_type, current_type))
        self.entry_type.setFixedHeight(46)
        col1.addWidget(lbl1)
        col1.addWidget(self.entry_type)
        row1.addLayout(col1)

        col2 = QVBoxLayout()
        lbl2 = QLabel("القسم / الوحدة")
        lbl2.setStyleSheet("font-size: 13px; color: #8E8E93; font-weight: 600; margin-bottom: 4px;")
        self.admission_department = QLineEdit(self.visit_data.get('admission_department', ''))
        self.admission_department.setFixedHeight(46)
        col2.addWidget(lbl2)
        col2.addWidget(self.admission_department)
        row1.addLayout(col2)

        layout.addLayout(row1)

        # --- Transfer to inpatient (shown for عيادات/طوارئ) ---
        self.transfer_frame = QFrame()
        self.transfer_frame.setStyleSheet("background: transparent; border: none;")
        transfer_v = QVBoxLayout(self.transfer_frame)
        transfer_v.setContentsMargins(0, 0, 0, 0)
        transfer_v.setSpacing(10)

        transfer_row = QHBoxLayout()
        lbl_transfer = QLabel("هل تم تحويل المريض للداخلي؟")
        lbl_transfer.setStyleSheet("font-size: 14px; font-weight: 600; color: #1D1D1F;")
        transfer_row.addWidget(lbl_transfer)
        transfer_row.addStretch()
        self.transfer_check = QCheckBox("نعم")
        self.transfer_check.setStyleSheet("font-size: 14px; font-weight: 600; color: #007AFF;")
        self.transfer_check.setChecked(bool(self.visit_data.get('transferred_to_inpatient')))
        transfer_row.addWidget(self.transfer_check)
        transfer_v.addLayout(transfer_row)

        self.transfer_dept_widget = QWidget()
        tdw_layout = QVBoxLayout(self.transfer_dept_widget)
        tdw_layout.setContentsMargins(0, 0, 0, 0)
        lbl_td = QLabel("القسم المحول إليه في الداخلي")
        lbl_td.setStyleSheet("font-size: 13px; color: #8E8E93; font-weight: 600; margin-bottom: 4px;")
        self.transfer_dept = QLineEdit(self.visit_data.get('transfer_department', ''))
        self.transfer_dept.setPlaceholderText("مثال: العناية المركزة / الجراحة...")
        self.transfer_dept.setFixedHeight(46)
        tdw_layout.addWidget(lbl_td)
        tdw_layout.addWidget(self.transfer_dept)
        self.transfer_dept_widget.setVisible(self.transfer_check.isChecked())
        self.transfer_check.toggled.connect(self.transfer_dept_widget.setVisible)
        transfer_v.addWidget(self.transfer_dept_widget)

        layout.addWidget(self.transfer_frame)

        # Show/hide transfer frame based on visit type
        def update_transfer_visibility(text=None):
            visible = self.entry_type.currentText() in ("عيادات", "طوارئ")
            self.transfer_frame.setVisible(visible)
        self.entry_type.currentTextChanged.connect(update_transfer_visibility)
        update_transfer_visibility()

        # --- Complaint ---
        lbl3 = QLabel("الشكوى الرئيسية")
        lbl3.setStyleSheet("font-size: 13px; color: #8E8E93; font-weight: 600; margin-bottom: 4px;")
        self.chief_complaint = QTextEdit(self.visit_data.get('chief_complaint', ''))
        self.chief_complaint.setPlaceholderText("أدخل شكوى المريض هنا...")
        self.chief_complaint.setFixedHeight(110)
        layout.addWidget(lbl3)
        layout.addWidget(self.chief_complaint)

        # --- Initial Diagnosis ---
        lbl4 = QLabel("التشخيص المبدئي")
        lbl4.setStyleSheet("font-size: 13px; color: #8E8E93; font-weight: 600; margin-bottom: 4px;")
        self.initial_diagnosis = QLineEdit(self.visit_data.get('initial_diagnosis', ''))
        self.initial_diagnosis.setPlaceholderText("التشخيص المتوقع عند الدخول...")
        self.initial_diagnosis.setFixedHeight(46)
        layout.addWidget(lbl4)
        layout.addWidget(self.initial_diagnosis)

        # --- Author ---
        lbl_author = QLabel("اسم الكاتب")
        lbl_author.setStyleSheet("font-size: 13px; color: #8E8E93; font-weight: 600; margin-bottom: 4px;")
        self.admission_author = QLineEdit(self.visit_data.get('admission_author', ''))
        self.admission_author.setPlaceholderText("اسم الموظف أو الطبيب المسجِّل...")
        self.admission_author.setFixedHeight(46)
        layout.addWidget(lbl_author)
        layout.addWidget(self.admission_author)

        layout.addStretch()


    def create_discharge_section(self, layout):
        # --- Discharge Date (auto, editable) ---
        lbl_ddate = QLabel("تاريخ الخروج")
        lbl_ddate.setStyleSheet("font-size: 13px; color: #8E8E93; font-weight: 600; margin-bottom: 4px;")
        default_dis = self.visit_data.get('discharge_time') or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.discharge_time_edit = QLineEdit(str(default_dis))
        self.discharge_time_edit.setFixedHeight(46)
        self.discharge_time_edit.setPlaceholderText("YYYY-MM-DD HH:MM:SS")
        layout.addWidget(lbl_ddate)
        layout.addWidget(self.discharge_time_edit)

        # --- Doctor & Status row ---
        row1 = QHBoxLayout()
        row1.setSpacing(15)

        colA = QVBoxLayout()
        lblA = QLabel("الطبيب المسؤول عن الخروج")
        lblA.setStyleSheet("font-size: 13px; color: #8E8E93; font-weight: 600; margin-bottom: 4px;")
        self.discharge_doctor = QLineEdit(self.visit_data.get('discharge_doctor', ''))
        self.discharge_doctor.setFixedHeight(46)
        colA.addWidget(lblA)
        colA.addWidget(self.discharge_doctor)
        row1.addLayout(colA)

        colB = QVBoxLayout()
        lblB = QLabel("حالة الخروج")
        lblB.setStyleSheet("font-size: 13px; color: #8E8E93; font-weight: 600; margin-bottom: 4px;")
        self.discharge_status = QComboBox()
        self.discharge_status.addItems(["خروج تحسن", "وفاة", "تحويل إلى مستشفى أخرى", "خروج حسب الطلب"])
        # Map old value to new
        status_val = self.visit_data.get('discharge_status', 'خروج تحسن')
        if status_val == 'تحويل ومتابعة':
            status_val = 'تحويل إلى مستشفى أخرى'
        self.discharge_status.setCurrentText(status_val)
        self.discharge_status.setFixedHeight(46)
        colB.addWidget(lblB)
        colB.addWidget(self.discharge_status)
        row1.addLayout(colB)

        layout.addLayout(row1)

        # --- Hospital referral (shown only when 'تحويل') ---
        self.referral_widget = QWidget()
        ref_layout = QVBoxLayout(self.referral_widget)
        ref_layout.setContentsMargins(0, 0, 0, 0)
        lbl_ref = QLabel("المستشفى المحول إليها المريض")
        lbl_ref.setStyleSheet("font-size: 13px; color: #8E8E93; font-weight: 600; margin-bottom: 4px;")
        self.referral_hospital = QComboBox()
        self.referral_hospital.setFixedHeight(46)
        hospital_names = []
        # hospitals can be a list of codes (from parent) or names. 
        # In PatientManagementSystem, self.hospitals is a dict (HOSPITALS).
        if isinstance(self.hospitals, dict):
            hospital_names = sorted(list(self.hospitals.values()))
        elif isinstance(self.hospitals, list):
            for h in self.hospitals:
                if isinstance(h, dict):
                    name = h.get('name', '')
                    if name: hospital_names.append(name)
                elif isinstance(h, str) and h:
                    hospital_names.append(h)
        
        # If still empty, use global HOSPITALS as fallback if reachable or just show codes
        if not hospital_names and 'HOSPITALS' in globals():
            hospital_names = sorted(list(globals()['HOSPITALS'].values()))

        if hospital_names:
            self.referral_hospital.addItems(hospital_names)
        else:
            self.referral_hospital.addItem("لا توجد مستشفيات مسجلة")
        saved_hosp = self.visit_data.get('referral_hospital', '')
        if saved_hosp and saved_hosp in hospital_names:
            self.referral_hospital.setCurrentText(saved_hosp)
        ref_layout.addWidget(lbl_ref)
        ref_layout.addWidget(self.referral_hospital)
        self.referral_widget.setVisible(self.discharge_status.currentText() == "تحويل إلى مستشفى أخرى")
        self.discharge_status.currentTextChanged.connect(
            lambda t: self.referral_widget.setVisible(t == "تحويل إلى مستشفى أخرى")
        )
        layout.addWidget(self.referral_widget)

        # --- Final Diagnosis ---
        lblC = QLabel("التشخيص النهائي")
        lblC.setStyleSheet("font-size: 13px; color: #8E8E93; font-weight: 600; margin-bottom: 4px;")
        self.final_diagnosis = QLineEdit(self.visit_data.get('final_diagnosis', ''))
        self.final_diagnosis.setPlaceholderText("التشخيص المؤكد...")
        self.final_diagnosis.setFixedHeight(46)
        layout.addWidget(lblC)
        layout.addWidget(self.final_diagnosis)

        # --- Medications ---
        lblD = QLabel("الأدوية الموصوفة عند الخروج")
        lblD.setStyleSheet("font-size: 13px; color: #8E8E93; font-weight: 600; margin-bottom: 4px;")
        self.discharge_medications = QTextEdit(self.visit_data.get('discharge_medications', ''))
        self.discharge_medications.setPlaceholderText("أدخل الأدوية والجرعات...")
        self.discharge_medications.setFixedHeight(110)
        layout.addWidget(lblD)
        layout.addWidget(self.discharge_medications)

        # --- Surgery ---
        surg_frame = QFrame()
        surg_frame.setStyleSheet("background-color: transparent; border: none;")
        surg_h = QHBoxLayout(surg_frame)
        surg_h.setContentsMargins(0, 5, 0, 5)
        surg_h.setSpacing(15)

        self.surgery_check = QCheckBox("تم إجراء عملية جراحية")
        self.surgery_check.setStyleSheet("font-size: 14px; font-weight: 600; color: #1D1D1F;")
        self.surgery_check.setChecked(bool(self.visit_data.get('surgery_performed')))
        surg_h.addWidget(self.surgery_check)

        self.surgery_name = QLineEdit(self.visit_data.get('surgery_name', ''))
        self.surgery_name.setPlaceholderText("اسم العملية...")
        self.surgery_name.setFixedHeight(46)
        self.surgery_name.setEnabled(self.surgery_check.isChecked())
        self.surgery_check.toggled.connect(self.surgery_name.setEnabled)
        surg_h.addWidget(self.surgery_name)

        layout.addWidget(surg_frame)

        # --- Author ---
        lbl_dauthor = QLabel("اسم الكاتب")
        lbl_dauthor.setStyleSheet("font-size: 13px; color: #8E8E93; font-weight: 600; margin-bottom: 4px;")
        self.discharge_author = QLineEdit(self.visit_data.get('discharge_author', ''))
        self.discharge_author.setPlaceholderText("اسم الموظف أو الطبيب المسجِّل...")
        self.discharge_author.setFixedHeight(46)
        layout.addWidget(lbl_dauthor)
        layout.addWidget(self.discharge_author)

        layout.addStretch()


    def create_summary_section_enhanced(self, layout):
        """إنشاء ملخص شامل واحترافي للزيارة"""
        # Summary Grid
        summary_grid = QGridLayout()
        summary_grid.setSpacing(20)
        
        # Card 1: Time Info
        time_card = QFrame()
        time_card.setStyleSheet("background-color: #FFFFFF; border: 1px solid #D2D2D7; border-radius: 16px;")
        time_vbox = QVBoxLayout(time_card)
        time_vbox.setContentsMargins(20, 20, 20, 20)
        
        adm_t = self.visit_data.get('admission_time', 'غير محدد')
        dis_t = self.visit_data.get('discharge_time', 'قيد المعالجة')
        
        time_vbox.addWidget(QLabel("🕒 <b>وقت الدخول:</b>"))
        time_vbox.addWidget(QLabel(adm_t))
        time_vbox.addSpacing(10)
        time_vbox.addWidget(QLabel("🚩 <b>وقت الخروج:</b>"))
        time_vbox.addWidget(QLabel(dis_t))
        
        summary_grid.addWidget(time_card, 0, 0)
        
        # Card 2: Diagnosis Summary
        diag_card = QFrame()
        diag_card.setStyleSheet("background-color: #F0F7FF; border: 1px solid #007AFF33; border-radius: 16px;")
        diag_vbox = QVBoxLayout(diag_card)
        diag_vbox.setContentsMargins(20, 20, 20, 20)
        
        diag_vbox.addWidget(QLabel("🔬 <b>التشخيص:</b>"))
        final_d = self.visit_data.get('final_diagnosis') or self.visit_data.get('initial_diagnosis') or "لم يحدد بعد"
        diag_lbl = QLabel(final_d)
        diag_lbl.setWordWrap(True)
        diag_lbl.setStyleSheet("font-size: 16px; font-weight: 700; color: #007AFF;")
        diag_vbox.addWidget(diag_lbl)
        
        summary_grid.addWidget(diag_card, 0, 1)
        
        layout.addLayout(summary_grid)
        
        # Detailed Card
        detail_card, detail_layout = self.create_card("تفاصيل الحالة", "📂")
        
        details = [
            ("الشكوى:", self.visit_data.get('chief_complaint') or "—"),
            ("الأدوية:", self.visit_data.get('discharge_medications') or "—"),
            ("الطبيب م الخروج:", self.visit_data.get('discharge_doctor') or "—"),
            ("العمليات:", self.visit_data.get('surgery_name') if self.visit_data.get('surgery_performed') else "لا يوجد")
        ]
        
        for label, val in details:
            row = QHBoxLayout()
            l = QLabel(f"<b>{label}</b>")
            l.setFixedWidth(120)
            v = QLabel(val)
            v.setWordWrap(True)
            row.addWidget(l)
            row.addWidget(v, 1)
            detail_layout.addLayout(row)
            
        layout.addWidget(detail_card)
        layout.addStretch()

    def set_read_only(self):
        all_widgets = (
            self.findChildren(QLineEdit) +
            self.findChildren(QTextEdit) +
            self.findChildren(QComboBox) +
            self.findChildren(QCheckBox)
        )
        for widget in all_widgets:
            widget.setEnabled(False)
            if isinstance(widget, (QLineEdit, QTextEdit)):
                widget.setReadOnly(True)

    def get_form_data(self):
        d = {
            'entry_type': self.entry_type.currentText(),
            'admission_department': self.admission_department.text().strip(),
            'admission_time': self.admission_time_edit.text().strip(),
            'chief_complaint': self.chief_complaint.toPlainText().strip(),
            'initial_diagnosis': self.initial_diagnosis.text().strip(),
            'admission_author': self.admission_author.text().strip(),
            'discharge_doctor': self.discharge_doctor.text().strip(),
            'discharge_status': self.discharge_status.currentText(),
            'discharge_time': self.discharge_time_edit.text().strip(),
            'final_diagnosis': self.final_diagnosis.text().strip(),
            'discharge_medications': self.discharge_medications.toPlainText().strip(),
            'surgery_performed': 1 if self.surgery_check.isChecked() else 0,
            'surgery_name': self.surgery_name.text().strip() if self.surgery_check.isChecked() else None,
            'discharge_author': self.discharge_author.text().strip(),
        }
        # Transfer to inpatient (for عيادات/طوارئ)
        if hasattr(self, 'transfer_check'):
            d['transferred_to_inpatient'] = 1 if self.transfer_check.isChecked() else 0
            d['transfer_department'] = self.transfer_dept.text().strip() if self.transfer_check.isChecked() else ''
        # Referral hospital (for تحويل إلى مستشفى أخرى)
        if hasattr(self, 'referral_hospital') and self.discharge_status.currentText() == 'تحويل إلى مستشفى أخرى':
            d['referral_hospital'] = self.referral_hospital.currentText()
        else:
            d['referral_hospital'] = ''
        return d

    def handle_save(self):
        data = self.get_form_data()
        user_id = self.parent().user_data.get('username', 'system') if hasattr(self.parent(), 'user_data') else 'system'
        if self.db.update_visit_admission(self.visit_id, data, user_id=user_id):
            QMessageBox.information(self, "نجاح", "تم حفظ البيانات بنجاح")
            self.visit_data.update(data)
        else:
            QMessageBox.warning(self, "خطأ", "فشل حفظ البيانات")

    def handle_save_discharge_and_close(self):
        confirm = QMessageBox.question(self, "تأكيد", "هل أنت متأكد من إغلاق الزيارة؟ لا يمكن تعديلها لاحقاً.", QMessageBox.Yes | QMessageBox.No)
        if confirm == QMessageBox.No:
            return
            
        data = self.get_form_data()
        data['discharge_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        user_id = self.parent().user_data.get('username', 'system') if hasattr(self.parent(), 'user_data') else 'system'
        
        if self.db.update_visit_discharge(self.visit_id, data, user_id=user_id):
            QMessageBox.information(self, "نجاح", "تم إغلاق الزيارة بنجاح")
            self.accept()
        else:
            QMessageBox.warning(self, "خطأ", "فشل إغلاق الزيارة")



class VisitCard(QFrame):
    """بطاقة عرض زيارة المريض بتصميم آبل بريميوم"""
    def __init__(self, visit, open_callback):
        super().__init__()
        self.visit = visit
        self.open_callback = open_callback

        TYPE_COLORS = {
            "عيادات": "#007AFF", # Apple Blue
            "طوارئ": "#FF3B30",  # Apple Red
            "داخلي": "#34C759",  # Apple Green
            "استقبال": "#5856D6" # Apple Purple
        }
        color = TYPE_COLORS.get(visit.get("entry_type", ""), "#8E8E93")

        self.setObjectName("visitCard")
        self.setCursor(Qt.PointingHandCursor)
        self.setFixedHeight(110)
        self.setStyleSheet(f"""
            #visitCard {{
                background-color: #FFFFFF;
                border-radius: 18px;
                border: 1px solid #E5E5E7;
            }}
            #visitCard:hover {{
                background-color: #FAFAFA;
                border: 1px solid #007AFF;
            }}
            QLabel {{
                background: transparent;
                border: none;
            }}
        """)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(15, 12, 15, 12)
        layout.setSpacing(12)

        # Left Accent Bar
        accent = QFrame()
        accent.setFixedWidth(6)
        accent.setStyleSheet(f"background-color: {color}; border-radius: 3px;")
        layout.addWidget(accent)

        # Content Area
        content = QVBoxLayout()
        content.setSpacing(6)

        # Row 1: Type & Date
        row1 = QHBoxLayout()
        type_lbl = QLabel(visit.get('entry_type', 'زيارة'))
        type_lbl.setStyleSheet(f"font-size: 15px; font-weight: 700; color: {color};")
        row1.addWidget(type_lbl)
        
        row1.addStretch()
        
        date_lbl = QLabel(visit.get('admission_time', ''))
        date_lbl.setStyleSheet("font-size: 12px; color: #8E8E93; font-weight: 500;")
        row1.addWidget(date_lbl)
        content.addLayout(row1)

        # Row 2: Hospital & Department
        h_code = visit.get('hospital_code', '')
        h_name = HOSPITALS.get(h_code, visit.get('hospital_name', 'مستشفى'))
        hosp_info = QLabel(f"🏥 {h_name} • {visit.get('admission_department', 'قسم غير محدد')}")
        hosp_info.setStyleSheet("font-size: 14px; color: #1D1D1F; font-weight: 600;")
        content.addWidget(hosp_info)

        # Row 3: Diagnosis / Status
        status_layout = QHBoxLayout()
        if visit.get("discharge_time"):
            status_text = "✓ تم الانتهاء"
            status_color = "#34C759"
        else:
            status_text = "● قيد المعالجة"
            status_color = "#FF9500"
        
        status_lbl = QLabel(status_text)
        status_lbl.setStyleSheet(f"font-size: 13px; font-weight: 700; color: {status_color}; background-color: {status_color}1A; padding: 4px 10px; border-radius: 10px;")
        status_layout.addWidget(status_lbl)
        
        if visit.get('initial_diagnosis'):
            diag_lbl = QLabel(f"🔍 {visit['initial_diagnosis']}")
            diag_lbl.setStyleSheet("font-size: 13px; color: #86868B; margin-left: 10px;")
            diag_lbl.setWordWrap(False)
            status_layout.addWidget(diag_lbl)
            
        status_layout.addStretch()
        content.addLayout(status_layout)

        layout.addLayout(content, 1)

        # Subtle Arrow
        arrow = QLabel("→")
        arrow.setStyleSheet("font-size: 20px; color: #C7C7CC; font-weight: 400;")
        layout.addWidget(arrow)

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.open_callback(self.visit)
        super().mousePressEvent(event)

class PatientProfilePage(QDialog):
    """نافذة ملف المريض الاحترافي بتصميم آبل بريميوم"""
    def __init__(self, db, patient_id, hospitals, parent=None):
        super().__init__(parent)
        self.db = db
        self.patient_id = patient_id
        self.hospitals = hospitals
        self.patient_data = self.db.search_patient_by_id(patient_id)
        
        if self.patient_data is None:
            QMessageBox.warning(self, "خطأ", f"لم يتم العثور على بيانات المريض")
            self.close()
            return
            
        self.visits_list = []
        
        self.setWindowTitle(f"الملف الطبي — {self.patient_data['name']}")
        self.setMinimumSize(1000, 750)
        self.setLayoutDirection(Qt.RightToLeft)
        # Apply Global Style for Apple Look
        self.setStyleSheet("""
            QDialog { 
                background-color: #F2F2F7; 
            } 
            QLabel { 
                color: #1D1D1F; 
                font-family: 'Segoe UI', 'Ubuntu', sans-serif;
            } 
            QScrollArea { 
                background: transparent; 
                border: none; 
            } 
            QWidget#scrollContent { 
                background: transparent; 
            } 
        """)
        self.init_ui()
        self.load_visits()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # ---------------------------------------------------------
        # Header Section (Apple Premium)
        # ---------------------------------------------------------
        header = QFrame()
        header.setObjectName("header")
        header.setFixedHeight(110)
        header.setStyleSheet("""
            background-color: rgba(255, 255, 255, 0.9);
            border-bottom: 1px solid #D2D2D7;
        """)
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(25, 0, 25, 0)

        # Avatar Placeholder (Circle with initials)
        avatar = QLabel(self.patient_data['name'][0].upper())
        avatar.setFixedSize(64, 64)
        avatar.setAlignment(Qt.AlignCenter)
        avatar.setStyleSheet("""
            background-color: #007AFF;
            color: white;
            font-size: 28px;
            font-weight: 700;
            border-radius: 32px;
        """)
        header_layout.addWidget(avatar)
        header_layout.addSpacing(20)

        # Patient Identity Info
        identity_vbox = QVBoxLayout()
        identity_vbox.setSpacing(4)
        identity_vbox.setAlignment(Qt.AlignVCenter)
        
        name_lbl = QLabel(self.patient_data['name'])
        name_lbl.setStyleSheet("font-size: 24px; font-weight: 700; color: #1D1D1F;")
        
        age_str = f"{self.patient_data.get('age', '—')} سنة" if self.patient_data.get('age') else "—"
        meta_info = QLabel(f"ID: {self.patient_id}  •  {self.patient_data['national_id']}  •  {self.patient_data.get('gender', '—')}  •  {age_str}")
        meta_info.setStyleSheet("font-size: 13px; color: #86868B; font-weight: 500;")
        
        identity_vbox.addWidget(name_lbl)
        identity_vbox.addWidget(meta_info)
        header_layout.addLayout(identity_vbox)
        
        header_layout.addStretch()

        # Action Buttons
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(12)
        
        # التحقق إذا كان المريض مجهول (بنمط U- في الرقم) وتم التعرف عليه بالفعل
        is_unknown_patient = '-U-' in self.patient_id or self.patient_id.startswith('U-')
        merged_into = self.patient_data.get('merged_into')
        status = self.patient_data.get('status')
        is_identified = merged_into or status == 'merged'
        
        # زر إضافة زيارة (يظهر فقط إذا لم يكن مجهول تم التعرف عليه)
        if not (is_unknown_patient and is_identified):
            add_btn = QPushButton("➕ زيارة جديدة")
            add_btn.setFixedSize(160, 44)
            add_btn.setCursor(Qt.PointingHandCursor)
            add_btn.setStyleSheet("""
                QPushButton {
                    background-color: #007AFF;
                    color: white;
                    border-radius: 22px;
                    font-size: 15px;
                    font-weight: 600;
                }
                QPushButton:hover {
                    background-color: #0071E3;
                }
            """)
            add_btn.clicked.connect(self.add_new_visit)
            btn_layout.addWidget(add_btn)
        
        # زر التحقق من المريض المجهول (يظهر فقط إذا لم يتم التحقق بعد)
        show_verification_box = False
        if is_unknown_patient:
            if is_identified:
                # تم التحقق بالفعل - سنعرض معلومات المريض الحقيقي في المحتوى وليس في الهيدر
                show_verification_box = True
            else:
                # لم يتم التحقق بعد - إظهار زر التحقق
                verify_btn = QPushButton("✔ تم التحقق من المريض")
                verify_btn.setFixedSize(180, 44)
                verify_btn.setCursor(Qt.PointingHandCursor)
                verify_btn.setStyleSheet("""
                    QPushButton {
                        background-color: #34C759;
                        color: white;
                        border-radius: 22px;
                        font-size: 15px;
                        font-weight: 600;
                    }
                    QPushButton:hover {
                        background-color: #30B350;
                    }
                """)
                verify_btn.clicked.connect(self.verify_unknown_patient)
                btn_layout.addWidget(verify_btn)
        
        header_layout.addLayout(btn_layout)

        main_layout.addWidget(header)

        # ---------------------------------------------------------
        # Content Section (Scrollable)
        # ---------------------------------------------------------
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        content_widget = QWidget()
        content_widget.setObjectName("scrollContent")
        self.content_layout = QVBoxLayout(content_widget)
        self.content_layout.setContentsMargins(25, 20, 25, 20)
        self.content_layout.setSpacing(20)

        # إظهار مربع التحقق من المريض المجهول (إذا تم التعرف عليه)
        if show_verification_box:
            self.show_merged_patient_info(self.content_layout)

        # 1. Medical Summary Card (Apple Style)
        self.create_medical_summary_card()

        # 2. Visits Section
        visits_section_layout = QVBoxLayout()
        visits_section_layout.setSpacing(12)
        
        section_title = QLabel("السجل الطبي والزيارات")
        section_title.setStyleSheet("font-size: 18px; font-weight: 700; color: #1D1D1F;")
        visits_section_layout.addWidget(section_title)

        self.visits_container = QVBoxLayout()
        self.visits_container.setSpacing(15)
        visits_section_layout.addLayout(self.visits_container)
        
        self.content_layout.addLayout(visits_section_layout)

        self.content_layout.addStretch()
        scroll.setWidget(content_widget)
        main_layout.addWidget(scroll)

    def create_medical_summary_card(self):
        card = QFrame()
        card.setStyleSheet("""
            QFrame {
                background-color: #FFFFFF;
                border-radius: 24px;
                border: 1px solid #E5E5E7;
            }
        """)
        layout = QVBoxLayout(card)
        layout.setContentsMargins(20, 15, 20, 15)
        layout.setSpacing(15)

        header_layout = QHBoxLayout()
        title = QLabel("الملخص الطبي الشامل")
        title.setStyleSheet("font-size: 17px; font-weight: 700; color: #1D1D1F;")
        header_layout.addWidget(title)
        header_layout.addStretch()
        
        edit_btn = QPushButton("تعديل")
        edit_btn.setCursor(Qt.PointingHandCursor)
        edit_btn.setStyleSheet("""
            QPushButton {
                color: #007AFF;
                font-size: 14px;
                font-weight: 600;
                background: transparent;
                border: none;
            }
            QPushButton:hover {
                text-decoration: underline;
            }
        """)
        edit_btn.clicked.connect(self.edit_medical_summary)
        header_layout.addWidget(edit_btn)
        layout.addLayout(header_layout)

        # Separator Line
        line = QFrame()
        line.setFixedHeight(1)
        line.setStyleSheet("background-color: #F2F2F7;")
        layout.addWidget(line)

        grid = QGridLayout()
        grid.setSpacing(30)

        medical_fields = [
            ("🩸 فصيلة الدم", self.patient_data.get('blood_type', '—'), "#FF3B30"),
            ("🎂 السن", f"{self.patient_data.get('age', '—')} سنة", "#1D1D1F"),
            ("🦠 الحساسية", self.patient_data.get('allergies', 'لا يوجد'), "#FF3B30"),
            ("🏥 الأمراض المزمنة", self.patient_data.get('chronic_diseases', 'لا يوجد'), "#1D1D1F"),
            ("💊 الأدوية الدائمة", self.patient_data.get('chronic_medications', 'لا يوجد'), "#1D1D1F"),
            ("📝 ملاحظات طبية", self.patient_data.get('medical_notes', 'لا يوجد'), "#8E8E93"),
            ("🩺 الشكوى الحالية", self.patient_data.get('current_complaint', '—'), "#007AFF"),
        ]

        for i, (label, val, color) in enumerate(medical_fields):
            lbl = QLabel(label)
            lbl.setStyleSheet("font-size: 13px; color: #8E8E93; font-weight: 600;")
            
            val_lbl = QLabel(str(val))
            val_lbl.setWordWrap(True)
            val_lbl.setStyleSheet(f"font-size: 15px; color: {color}; font-weight: 500; line-height: 1.4;")
            
            field_vbox = QVBoxLayout()
            field_vbox.setSpacing(8)
            field_vbox.addWidget(lbl)
            field_vbox.addWidget(val_lbl)
            
            grid.addLayout(field_vbox, i // 2, i % 2)

        layout.addLayout(grid)
        self.content_layout.addWidget(card)

    def load_visits(self):
        """تحميل وعرض الزيارات بتصميم القائمة المنظمة"""
        self.visits_list = self.db.get_visits(self.patient_id, limit=50)
        self.display_visits()

    def display_visits(self):
        # Clear existing
        while self.visits_container.count():
            item = self.visits_container.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        
        if not self.visits_list:
            empty_lbl = QLabel("لا توجد سجلات زيارات سابقة لبدء عرضها هنا.")
            empty_lbl.setStyleSheet("color: #8E8E93; font-size: 16px; margin: 20px;")
            empty_lbl.setAlignment(Qt.AlignCenter)
            self.visits_container.addWidget(empty_lbl)
            return

        for visit in self.visits_list:
            card = VisitCard(visit, self.open_visit_details)
            self.visits_container.addWidget(card)

    def open_visit_details(self, visit):
        dialog = VisitDetailsDialog(self.db, self.patient_id, visit, self.hospitals, self)
        if dialog.exec() == QDialog.Accepted:
            self.load_visits()

    def edit_medical_summary(self):
        dialog = EditMedicalSummaryDialog(self.db, self.patient_data, self)
        if dialog.exec() == QDialog.Accepted:
            self.patient_data = self.db.search_patient_by_id(self.patient_id)
            # Refresh specific components instead of full refresh
            self.refresh_medical_summary_ui()

    def refresh_medical_summary_ui(self):
        # Redraw the content area
        for i in reversed(range(self.content_layout.count())): 
            widget = self.content_layout.itemAt(i).widget()
            if widget: widget.deleteLater()
        
        # Reset layout and re-init
        self.create_medical_summary_card()
        # Re-add visits section title and container
        visits_section_layout = QVBoxLayout()
        section_title = QLabel("السجل الطبي والزيارات")
        section_title.setStyleSheet("font-size: 22px; font-weight: 700; color: #1D1D1F;")
        visits_section_layout.addWidget(section_title)
        self.visits_container = QVBoxLayout()
        self.visits_container.setSpacing(15)
        visits_section_layout.addLayout(self.visits_container)
        self.content_layout.addLayout(visits_section_layout)
        self.content_layout.addStretch()
        self.load_visits()

    def add_new_visit(self):
        """إضافة زيارة جديدة بتدفق عمل سلس"""
        new_visit = {
            'visit_id': -1,
            'patient_id': self.patient_id,
            'hospital_code': self.parent().selected_hospital if hasattr(self.parent(), 'selected_hospital') else 'GEN',
            'hospital_name': self.parent().selected_hospital_name if hasattr(self.parent(), 'selected_hospital_name') else '',
            'entry_type': 'Clinic',
            'admission_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'admission_department': '',
            'chief_complaint': '',
            'initial_diagnosis': '',
            'chronic_diseases': '',
            'chronic_medications': '',
            'allergies': '',
            'admission_notes': '',
            'discharge_time': None,
            'discharge_doctor': None,
            'final_diagnosis': None,
            'discharge_medications': None,
            'surgery_performed': 0,
            'surgery_name': None
        }

        user_id = self.parent().user_data.get('username', 'system') if hasattr(self.parent(), 'user_data') else 'system'
        visit_id = self.db.create_visit(self.patient_id, new_visit['hospital_code'], new_visit['hospital_name'], new_visit, user_id=user_id)
        
        if visit_id:
            new_visit['visit_id'] = visit_id
            dialog = VisitDetailsDialog(self.db, self.patient_id, new_visit, self.hospitals, self)
            if dialog.exec() == QDialog.Accepted:
                self.load_visits()
        else:
            QMessageBox.warning(self, "خطأ", "فشل في إنشاء الزيارة")

    def show_merged_patient_info(self, parent_layout):
        """إظهار معلومات المريض الحقيقي بعد التحقق من المجهول"""
        merged_into = self.patient_data.get('merged_into', '')
        identified_at = self.patient_data.get('identified_at', '')
        
        # جلب بيانات المريض الحقيقي
        real_patient = None
        if merged_into:
            real_patient = self.db.search_patient_by_id(merged_into)
        
        # إنشاء GroupBox لعرض المعلومات (أوضح من QFrame)
        info_group = QGroupBox("✅ تم التحقق من هوية المريض")
        info_group.setStyleSheet("""
            QGroupBox {
                background-color: #E8F5E9;
                border: 3px solid #34C759;
                border-radius: 12px;
                font-size: 15px;
                font-weight: 700;
                color: #1B5E20;
                margin-top: 15px;
                padding-top: 15px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 15px;
                padding: 5px 10px;
                background-color: #34C759;
                color: white;
                border-radius: 6px;
            }
        """)
        info_group.setMinimumWidth(500)
        info_group.setMinimumHeight(200)
        
        info_layout = QVBoxLayout(info_group)
        info_layout.setSpacing(12)
        info_layout.setContentsMargins(25, 25, 25, 25)
        
        if real_patient:
            # اسم المريض الحقيقي
            name_text = real_patient.get('name', '—')
            name_lbl = QLabel(f"👤 الاسم: {name_text}")
            name_lbl.setStyleSheet("font-size: 18px; font-weight: 700; color: #0D47A1; background: transparent;")
            name_lbl.setMinimumHeight(30)
            name_lbl.setWordWrap(True)
            info_layout.addWidget(name_lbl)
            
            # الرقم القومي
            national_id = real_patient.get('national_id', '')
            if national_id:
                nid_lbl = QLabel(f"🆔 الرقم القومي: {national_id}")
                nid_lbl.setStyleSheet("font-size: 15px; font-weight: 600; color: #1B5E20; background: transparent;")
                nid_lbl.setMinimumHeight(25)
                info_layout.addWidget(nid_lbl)
            
            # الرقم الموحد
            id_lbl = QLabel(f"🔢 الرقم الموحد: {merged_into}")
            id_lbl.setStyleSheet("font-size: 15px; font-weight: 600; color: #1B5E20; background: transparent;")
            id_lbl.setMinimumHeight(25)
            info_layout.addWidget(id_lbl)
        else:
            # إذا لم نجد بيانات المريض الحقيقي
            warning_lbl = QLabel("⚠️ لم يتم العثور على بيانات المريض")
            warning_lbl.setStyleSheet("font-size: 15px; color: #FF9800; background: transparent;")
            warning_lbl.setMinimumHeight(30)
            info_layout.addWidget(warning_lbl)
            
            if merged_into:
                id_lbl = QLabel(f"🔢 الرقم: {merged_into}")
                id_lbl.setStyleSheet("font-size: 15px; font-weight: 600; color: #1B5E20; background: transparent;")
                id_lbl.setMinimumHeight(25)
                info_layout.addWidget(id_lbl)
        
        # تاريخ التحقق
        if identified_at:
            date_lbl = QLabel(f"📅 تاريخ التحقق: {identified_at}")
            date_lbl.setStyleSheet("font-size: 13px; color: #2E7D32; background: transparent;")
            date_lbl.setMinimumHeight(20)
            info_layout.addWidget(date_lbl)
        
        # زر فتح ملف المريض الحقيقي
        if merged_into:
            open_btn = QPushButton("📂 فتح ملف المريض الحقيقي")
            open_btn.setCursor(Qt.PointingHandCursor)
            open_btn.setMinimumHeight(40)
            open_btn.setStyleSheet("""
                QPushButton {
                    background-color: #34C759;
                    color: white;
                    border-radius: 8px;
                    font-size: 13px;
                    font-weight: 700;
                    padding: 10px 20px;
                    margin-top: 10px;
                }
                QPushButton:hover {
                    background-color: #30B350;
                }
            """)
            open_btn.clicked.connect(lambda: self.open_merged_patient(merged_into))
            info_layout.addWidget(open_btn)
        
        # إضافة للـ layout (الآن في content_layout العمودي)
        parent_layout.addWidget(info_group)
    
    def open_merged_patient(self, patient_id):
        """فتح ملف المريض المدمج"""
        self.accept()  # إغلاق النافذة الحالية
        parent = self.parent()
        if parent and hasattr(parent, 'show_patient_history'):
            parent.show_patient_history(patient_id)

    def verify_unknown_patient(self):
        """فتح نافذة التحقق من هوية المريض المجهول"""
        dialog = VerifyPatientDialog(self.db, self.patient_id, self)
        if dialog.exec() == QDialog.Accepted:
            result = dialog.get_result()
            if result:
                real_name = result['name']
                national_id = result['national_id']
                
                # تنفيذ التحقق/الدمج
                user_id = self.parent().user_data.get('username', 'system') if hasattr(self.parent(), 'user_data') else 'system'
                success, message, new_patient_id = self.db.identify_unknown_patient(
                    self.patient_id, real_name, national_id, user_id=user_id
                )
                
                if success:
                    QMessageBox.information(self, "تم التحقق", message)
                    
                    # تحديث قائمة المرضى في الواجهة الرئيسية
                    parent = self.parent()
                    if parent and hasattr(parent, 'refresh_patients_list'):
                        parent.refresh_patients_list(force_fresh=True)
                    
                    # إغلاق النافذة وتحديث البيانات
                    self.accept()
                    
                    # فتح الملف الجديد إذا تم إنشاء مريض جديد
                    if new_patient_id and new_patient_id != self.patient_id:
                        if parent and hasattr(parent, 'show_patient_history'):
                            parent.show_patient_history(new_patient_id)
                else:
                    QMessageBox.warning(self, "فشل", message)

class DataLoader(QThread):
    """خيط عامل لجلب البيانات في الخلفية لمنع تجمد الواجهة"""
    finished = Signal(list, int) # البيانات، والعدد الإجمالي
    error = Signal(str)

    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.setTerminationEnabled(True)

    def run(self):
        try:
            result = self.func(*self.args, **self.kwargs)
            if isinstance(result, list):
                total = result[0].get('total_count', len(result)) if result else 0
                self.finished.emit(result, total)
            else:
                self.finished.emit([], 0)
        except Exception as e:
            self.error.emit(str(e))
    
    def __del__(self):
        """التأكد من انتظار انتهاء الخيط قبل حذفه"""
        if self.isRunning():
            self.wait(2000)  # انتظر حتى 2 ثانية قبل الحذف



class PatientManagementSystem(QMainWindow):
    def __init__(self):
        super().__init__()
        self.selected_hospital = None
        self.db = DatabaseManager()
        self.current_page = 0
        self.page_size = 100
        
        # استخدام القائمة المركزية للمستشفيات
        self.hospitals = HOSPITALS
        
        self.setWindowTitle("برنامج الرقم الموحد - النسخة الاحترافية")
        self.setMinimumSize(1400, 800)
        self.setLayoutDirection(Qt.RightToLeft)
        
        # تهيئة الأنماط العامة
        self.apply_arabic_style()
        
        # محاولة تسجيل دخول تلقائي
        self.user_data = self.try_auto_login()
        
        if not self.user_data:
            # إذا فشل التلقائي، إظهار نافذة تسجيل الدخول
            login = LoginWindow(self.db)
            if login.exec() == QDialog.Accepted:
                self.user_data = login.user_data
            else:
                # إغلاق البرنامج إذا تم إلغاء تسجيل الدخول
                sys.exit(0)

        # تعيين المستشفى بناءً على المستخدم بأمان
        self.selected_hospital = self.user_data.get('hospital_code', 'ADMIN')
        self.selected_hospital_name = self.hospitals.get(self.selected_hospital, "النظام الرئيسي")
        self.user_role = self.user_data.get('role', 'hospital')
        
        # إنشاء الواجهة الرئيسية
        try:
            self.setup_main_interface()
            
            # FIX: Removed EnhancedStatusBar - moved indicators to header
            # self.setStatusBar(EnhancedStatusBar(self))
            # self.update_status_indicators()
            
            # تعيين الاختصارات العالمية
            self.setup_shortcuts()
            
            # إظهار النافذة
            self.show()
        except Exception as e:
            QMessageBox.critical(None, "خطأ فادح في التشغيل", f"فشل تشغيل الواجهة الرئيسية:\n{str(e)}")
            sys.exit(1)

    def try_auto_login(self):
        """محاولة تسجيل الدخول من ملف الإعدادات - مع Fallback إلى SQLite لتفادي تأخير الاتصال"""
        try:
            if not os.path.exists(SETTINGS_PATH):
                return None

            with open(SETTINGS_PATH, "r", encoding='utf-8') as f:
                settings = json.load(f)
            remember_user = settings.get("remember_user")
            if not remember_user:
                return None

            # محاولة PostgreSQL أولاً بشرط أن يكون الاتصال جاهزاً
            if self.db.is_online():
                conn = None
                try:
                    conn = get_connection()
                    if conn:
                        cursor = conn.cursor()
                        cursor.execute(
                            "SELECT username, hospital_code, role FROM users WHERE username=%s",
                            (remember_user,)
                        )
                        user = cursor.fetchone()
                        cursor.close()
                        if user:
                            columns = ['username', 'hospital_code', 'role']
                            return dict(zip(columns, user))
                except Exception as e:
                    system_logger.warning(f"Auto login PG error: {e}")
                finally:
                    release_connection(conn)

            # Fallback إلى SQLite  عند انقطاع الاتصال (لتفادي التأخير)
            try:
                conn_sq = sqlite_manager.get_connection(authorized=True)
                cur_sq = conn_sq.cursor()
                cur_sq.execute(
                    "SELECT username, hospital_code, role FROM users WHERE username=? LIMIT 1",
                    (remember_user,)
                )
                user = cur_sq.fetchone()
                sqlite_manager.return_connection(conn_sq)
                if user:
                    columns = ['username', 'hospital_code', 'role']
                    return dict(zip(columns, user))
            except Exception as e:
                system_logger.warning(f"Auto login SQLite error: {e}")

        except Exception as e:
            system_logger.warning(f"Auto login failed: {e}")
        return None

    def setup_shortcuts(self):
        """إعداد اختصارات لوحة المفاتيح"""
        # Ctrl + F للبحث
        self.shortcut_search = QShortcut(QKeySequence("Ctrl+F"), self)
        self.shortcut_search.activated.connect(self.focus_search)
        
        # Ctrl + N لإضافة مريض
        self.shortcut_new = QShortcut(QKeySequence("Ctrl+N"), self)
        self.shortcut_new.activated.connect(self.go_to_add_patient)
        
        # Ctrl + E للتصدير
        self.shortcut_export = QShortcut(QKeySequence("Ctrl+E"), self)
        self.shortcut_export.activated.connect(self.export_patients_to_csv)

    def focus_search(self):
        """التركيز على حقل البحث"""
        tabs = self.centralWidget().findChild(QTabWidget)
        if tabs:
            tabs.setCurrentIndex(1) # تبويب القائمة
            if hasattr(self, 'search_entry'):
                self.search_entry.setFocus()
                self.search_entry.selectAll()

    def export_patients_to_csv(self):
        """تصدير قائمة المرضى الحالية إلى ملف CSV (Excel)"""
        try:
            path, _ = QFileDialog.getSaveFileName(self, "تصدير البيانات", "", "Excel Files (*.csv)")
            if not path:
                return
                
            patients = self.db.get_all_patients(limit=1000000) # تصدير كمية كبيرة
            
            with open(path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=['patient_id', 'name', 'national_id', 'birth_date', 'entry_type', 'phone', 'address', 'hospital', 'registration_date'])
                writer.writeheader()
                for p in patients:
                    # تنظيف البيانات قبل الكتابة إذا لزم الأمر
                    row = {k: v for k, v in p.items() if k in writer.fieldnames}
                    writer.writerow(row)
            
            QMessageBox.information(self, "نجاح", f"تم تصدير {len(patients)} سجل بنجاح إلى:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "خطأ", f"فشل التصدير: {str(e)}")

    
    def apply_arabic_style(self):
        """تطبيق التصميم العربي وتنسيق QSS الخارجي"""
        # محاولة تحميل ملف الاستايل الخارجي
        qss_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "style.qss")
        if os.path.exists(qss_path):
            try:
                with open(qss_path, "r", encoding="utf-8") as f:
                    self.setStyleSheet(f.read())
            except Exception as e:
                print(f"خطأ في تحميل QSS: {e}")
        else:
            # استايل احتياطي بسيط في حال عدم وجود الملف
            self.setStyleSheet("""
                QMainWindow { background-color: #F2F2F7; color: #1D1D1F; }
                QLabel { color: #1D1D1F; }
                QTabWidget::pane { background-color: #FFFFFF; border-radius: 12px; }
                QTabBar::tab { background-color: transparent; color: #86868B; padding: 12px 24px; border-radius: 8px; }
                QTabBar::tab:selected { background-color: #007AFF; color: white; }
            """)

    def update_status_indicators(self):
        """
        تحديث مؤشرات الحالة في شريط الحالة
        يتفاعل مع system_state.health و system_state.mode
        """
        try:
            # FIXED: Call local function directly, avoid self-import
            status = get_system_status()
            
            # Update server indicator
            self.statusBar().set_server_status(status)
            
            # Update sync indicator
            if system_state.health.get('sync_ok', False):
                self.statusBar().set_sync_status('IDLE')
            else:
                self.statusBar().set_sync_status('ERROR')
            
            QApplication.processEvents()
        except Exception as e:
            error_logger.error(f"Error updating status indicators: {e}")

    def setup_main_interface(self):
        """إعداد الواجهة الرئيسية"""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # الهيدر الاحترافي
        self.create_modern_header(main_layout)
        
        # إنشاء التبويبات
        self.tabs = QTabWidget()
        self.tabs.currentChanged.connect(self.on_tab_changed)
        main_layout.addWidget(self.tabs)
        
        # 1. إضافة مريض
        self.create_input_tab(self.tabs)
        
        # 2. عرض القائمة
        self.create_display_tab(self.tabs)

        # 3. شاشة المراقبة - تم الإزالة لتبسيط الواجهة
        # self.create_monitoring_tab(self.tabs)  # REMOVED: Moved status indicators to header

        # 4. إدارة المستخدمين (للمدير فقط)
        if self.user_role == 'admin':
            self.create_users_tab(self.tabs)

        # 4. الفوتر الرسمي
        footer_container = QWidget()
        footer_container.setStyleSheet("background-color: #1E293B; border-top-left-radius: 12px; border-top-right-radius: 12px;")
        footer_layout = QHBoxLayout(footer_container)
        footer_layout.setContentsMargins(20, 12, 20, 12)
        
        footer_label = QLabel("✨ تم التطوير والبرمجة عن طريق إدارة سلامة المرضى بكفرالشيخ | 📞 م/ 01068422223")
        footer_label.setStyleSheet("color: white; font-size: 13px; font-weight: 500;")
        footer_label.setAlignment(Qt.AlignCenter)
        
        footer_layout.addWidget(footer_label)
        
        # FIX: Add logout button and time to footer
        footer_right_layout = QHBoxLayout()
        footer_right_layout.setSpacing(15)
        
        # الوقت والتاريخ
        self.time_label = QLabel()
        self.time_label.setStyleSheet("color: #ecf0f1; font-size: 14px; font-weight: 500;")
        footer_right_layout.addWidget(self.time_label)
        
        # زر تسجيل الخروج
        logout_btn = QPushButton("🚪 خروج")
        logout_btn.setCursor(Qt.PointingHandCursor)
        logout_btn.setStyleSheet("QPushButton { background-color: #ef4444; color: white; border: none; border-radius: 6px; padding: 6px 12px; font-weight: bold; font-size: 13px; } QPushButton:hover { background-color: #dc2626; }")
        logout_btn.clicked.connect(self.handle_logout)
        footer_right_layout.addWidget(logout_btn)
        
        footer_layout.addLayout(footer_right_layout)
        
        # FIX #4: Add health status indicator
        self.health_status_label = QLabel()
        self.health_status_label.setAlignment(Qt.AlignRight)
        self.health_status_label.setStyleSheet("color: white; font-size: 13px; font-weight: bold; padding: 5px 15px; border-radius: 8px;")
        self.update_health_status_indicator()  # Update health state display
        footer_layout.addWidget(self.health_status_label)
        
        main_layout.addWidget(footer_container)
        
        # مؤقت لتحديث الحالة والوقت (كل 10 ثوانٍ ليكون متجاوباً)
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_system_status)
        self.timer.start(10000) 
        self.update_system_status()

    def create_monitoring_tab(self, tabs):
        """تبويب شاشة المراقبة - يعرض إحصائيات سريعة"""
        mon_tab = QWidget()
        layout = QVBoxLayout(mon_tab)
        layout.setContentsMargins(30, 30, 30, 30)
        layout.setSpacing(20)

        title = QLabel("📊 لوحة المراقبة")
        title.setStyleSheet("font-size: 24px; font-weight: bold; color: #1e293b;")
        layout.addWidget(title)

        self.monitor_patients_label = QLabel("عدد المرضى: -")
        self.monitor_visits_label = QLabel("عدد الزيارات: -")
        self.monitor_sync_label = QLabel("طابور المزامنة: -")
        self.monitor_online_label = QLabel("حالة الاتصال: ؟")
        for lbl in [self.monitor_patients_label, self.monitor_visits_label, self.monitor_sync_label, self.monitor_online_label]:
            lbl.setStyleSheet("font-size: 18px; margin-top: 10px;")
            layout.addWidget(lbl)

        refresh_btn = QPushButton("تحديث البيانات")
        refresh_btn.clicked.connect(self.refresh_monitoring_stats)
        layout.addWidget(refresh_btn)

        tabs.addTab(mon_tab, "المراقبة")
    
    def update_health_status_indicator(self):
        """
        FIX #4: Update health status indicator in UI
        Shows ONLINE/OFFLINE/CRITICAL based on system_state.health
        """
        if not hasattr(self, 'health_status_label'):
            return
        
        # FIX #4: Use get_system_status() function
        status_text = get_system_status()
        
        if status_text == "ONLINE":
            status_display = "✅ ONLINE"
            status_color = "#27ae60"  # Green
        elif status_text == "OFFLINE":
            status_display = "⚠️ OFFLINE (PostgreSQL)"
            status_color = "#f39c12"  # Orange
        else:  # CRITICAL
            status_display = "❌ CRITICAL"
            status_color = "#c0392b"  # Red
        
        self.health_status_label.setText(f"📊 {status_display}")
        self.health_status_label.setStyleSheet(f"""
            color: white; 
            font-size: 13px; 
            font-weight: bold; 
            padding: 5px 15px; 
            border-radius: 8px;
            background-color: {status_color};
        """)
        
        # Also update header if exists
        if hasattr(self, 'header_health_label'):
            self.header_health_label.setText(status_display)

    def refresh_monitoring_stats(self):
        stats = {
            'patients': self.db.get_total_patients(),
            'visits': self.db.get_total_visits(),
            'sync': self.db.get_sync_queue_count(),
            'online': "Online" if self.db.is_online() else "Offline"
        }
        self.monitor_patients_label.setText(f"عدد المرضى: {stats['patients']}")
        self.monitor_visits_label.setText(f"عدد الزيارات: {stats['visits']}")
        self.monitor_sync_label.setText(f"طابور المزامنة: {stats['sync']}")
        self.monitor_online_label.setText(f"حالة الاتصال: {stats['online']}")

    def create_users_tab(self, tabs):
        """تبويب إدارة المستخدمين الاحترافي (HIS Style)"""
        users_tab = QWidget()
        users_tab_layout = QVBoxLayout(users_tab)
        users_tab_layout.setContentsMargins(30, 30, 30, 30)
        users_tab_layout.setSpacing(20)

        # 1. العنوان وشريط المعلومات
        header_layout = QHBoxLayout()
        title_vbox = QVBoxLayout()
        title = QLabel("👥 إدارة مستخدمي النظام")
        title.setStyleSheet("font-size: 24px; font-weight: bold; color: #1e293b;")
        
        self.user_stats_label = QLabel("عدد المستخدمين: 0")
        self.user_stats_label.setStyleSheet("font-size: 14px; color: #64748b; font-weight: bold;")
        
        title_vbox.addWidget(title)
        title_vbox.addWidget(self.user_stats_label)
        header_layout.addLayout(title_vbox)
        header_layout.addStretch()

        add_user_btn = QPushButton("➕ إضافة مستخدم جديد")
        add_user_btn.setStyleSheet("""
            QPushButton { background-color: #10b981; color: white; padding: 12px 25px; border-radius: 8px; font-weight: bold; font-size: 14px; }
            QPushButton:hover { background-color: #059669; }
        """)
        add_user_btn.clicked.connect(self.show_add_user_dialog)
        header_layout.addWidget(add_user_btn)
        
        users_tab_layout.addLayout(header_layout)

        # 2. شريط البحث
        search_container = QFrame()
        search_container.setStyleSheet("background-color: white; border-radius: 10px; border: 1px solid #e2e8f0;")
        search_layout = QHBoxLayout(search_container)
        search_layout.setContentsMargins(15, 8, 15, 8)
        
        search_icon = QLabel("🔍")
        search_icon.setStyleSheet("font-size: 16px; border: none;")
        search_layout.addWidget(search_icon)
        
        self.user_search_entry = QLineEdit()
        self.user_search_entry.setPlaceholderText("بحث عن مستخدم بالاسم أو الكود...")
        self.user_search_entry.setStyleSheet("border: none; padding: 8px; font-size: 14px;")
        self.user_search_entry.textChanged.connect(self.filter_users_table)
        search_layout.addWidget(self.user_search_entry)
        
        users_tab_layout.addWidget(search_container)

        # 3. جدول المستخدمين
        table_frame = QFrame()
        table_frame.setStyleSheet("background-color: white; border-radius: 12px; border: 1px solid #e2e8f0;")
        table_layout = QVBoxLayout(table_frame)
        table_layout.setContentsMargins(0, 0, 0, 0)

        self.users_table = QTableWidget()
        self.users_table.setColumnCount(5)
        self.users_table.setHorizontalHeaderLabels(["المستخدم", "المستشفى", "الكود", "الصلاحية", "الإجراءات"])
        
        h = self.users_table.horizontalHeader()
        h.setSectionResizeMode(QHeaderView.Stretch)
        h.setFixedHeight(50)
        h.setStyleSheet("QHeaderView::section { background-color: #f8fafc; color: #475569; font-weight: bold; border: none; border-bottom: 2px solid #e2e8f0; }")
        
        self.users_table.setStyleSheet("QTableWidget { border: none; gridline-color: #f1f5f9; } QTableWidget::item { padding: 15px; border-bottom: 1px solid #f1f5f9; } QTableWidget::item:selected { background-color: #eff6ff; color: #2563eb; }")
        self.users_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.users_table.verticalHeader().setVisible(False)
        self.users_table.setMouseTracking(True)
        
        table_layout.addWidget(self.users_table)
        users_tab_layout.addWidget(table_frame)
        
        self.refresh_users_table()
        tabs.addTab(users_tab, "👥 إدارة المستخدمين")

    def filter_users_table(self):
        """تصفية جدول المستخدمين"""
        search_term = self.user_search_entry.text().lower()
        for i in range(self.users_table.rowCount()):
            row_visible = False
            for j in range(self.users_table.columnCount() - 1): # استثناء عمود الإجراءات
                item = self.users_table.item(i, j)
                if item and search_term in item.text().lower():
                    row_visible = True
                    break
            self.users_table.setRowHidden(i, not row_visible)

    def show_add_user_dialog(self):
        """فتح نافذة إضافة مستخدم"""
        dialog = AddUserDialog(self.hospitals, self)
        if dialog.exec() == QDialog.Accepted:
            info = dialog.user_info
            conn = None
            try:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO users (username, password, hospital_code, role)
                    VALUES (%s, %s, %s, %s)
                """, (info['username'], info['password'], info['hospital_code'], info['role']))
                conn.commit()
                cursor.close()
                QMessageBox.information(self, "نجاح", "تمت إضافة المستخدم بنجاح")
                self.refresh_users_table()
            except psycopg2.IntegrityError:
                if conn:
                    conn.rollback()
                QMessageBox.warning(self, "خطأ", "اسم المستخدم موجود مسبقاً")
            except Exception as e:
                if conn:
                    conn.rollback()
                QMessageBox.critical(self, "خطأ", f"فشل الحفظ: {e}")
            finally:
                release_connection(conn)

    def refresh_users_table(self):
        """تحديث جدول المستخدمين وتنسيقه باحترافية"""
        try:
            users = self.db.get_all_users()
            
            if hasattr(self, 'user_stats_label'):
                self.user_stats_label.setText(f"عدد المستخدمين في النظام: {len(users)}")
            self.users_table.setRowCount(0)
            
            for row_idx, user in enumerate(users):
                self.users_table.insertRow(row_idx)
                self.users_table.setRowHeight(row_idx, 60)
                
                # 1. اسم المستخدم
                u_item = QTableWidgetItem(user['username'])
                u_item.setFont(QFont("Arial", 11, QFont.Bold))
                self.users_table.setItem(row_idx, 0, u_item)
                
                # 2. اسم المستشفى
                h_name = self.hospitals.get(user['hospital_code'], "إدارة النظام")
                self.users_table.setItem(row_idx, 1, QTableWidgetItem(h_name))
                
                # 3. الكود
                self.users_table.setItem(row_idx, 2, QTableWidgetItem(user['hospital_code']))
                
                # 4. الصلاحية بتنسيق لوني
                role_text = "مدير النظام" if user['role'] == 'admin' else "مستخدم مستشفى"
                r_item = QTableWidgetItem(role_text)
                if user['role'] == 'admin':
                    r_item.setForeground(QColor("#ef4444"))
                    r_item.setFont(QFont("Arial", 11, QFont.Bold))
                self.users_table.setItem(row_idx, 3, r_item)
                
                # 5. عمود الإجراءات
                actions_wid = QWidget()
                actions_layout = QHBoxLayout(actions_wid)
                actions_layout.setContentsMargins(5, 5, 5, 5)
                actions_layout.setSpacing(8)
                
                # زر تغيير الباسورد
                pwd_btn = QPushButton("🔑")
                pwd_btn.setToolTip("تغيير كلمة المرور")
                pwd_btn.setFixedSize(35, 35)
                pwd_btn.setStyleSheet("QPushButton { background-color: #f59e0b; color: white; border-radius: 5px; } QPushButton:hover { background-color: #d97706; }")
                pwd_btn.clicked.connect(lambda checked, u=user['username']: self.show_change_password_dialog(u))
                actions_layout.addWidget(pwd_btn)

                # زر Reset
                reset_btn = QPushButton("🔄")
                reset_btn.setToolTip("إعادة تعيين (9876543210)")
                reset_btn.setFixedSize(35, 35)
                reset_btn.setStyleSheet("QPushButton { background-color: #3b82f6; color: white; border-radius: 5px; } QPushButton:hover { background-color: #2563eb; }")
                if user['username'] == 'admin':
                    reset_btn.setEnabled(False)
                    reset_btn.setStyleSheet("background-color: #cbd5e1; color: white; border-radius: 5px;")
                reset_btn.clicked.connect(lambda checked, u=user['username']: self.reset_user_password(u))
                actions_layout.addWidget(reset_btn)

                # زر حذف
                del_btn = QPushButton("🗑️")
                del_btn.setToolTip("حذف المستخدم")
                del_btn.setFixedSize(35, 35)
                del_btn.setStyleSheet("QPushButton { background-color: #ef4444; color: white; border-radius: 5px; } QPushButton:hover { background-color: #dc2626; }")
                if user['username'] == 'admin':
                    del_btn.setEnabled(False)
                    del_btn.setStyleSheet("background-color: #cbd5e1; color: white; border-radius: 5px;")
                del_btn.clicked.connect(lambda checked, u=user['username']: self.delete_user(u))
                actions_layout.addWidget(del_btn)
                
                self.users_table.setCellWidget(row_idx, 4, actions_wid)
                
        except Exception as e:
            print(f"Error refreshing users table: {e}")

    def reset_user_password(self, username):
        """إعادة تعيين كلمة المرور للقيمة الافتراضية"""
        if username == 'admin':
            QMessageBox.warning(self, "تحذير", "لا يمكن إعادة تعيين كلمة مرور المدير من هنا")
            return
            
        confirm = QMessageBox.question(self, "تأكيد", f"هل تريد مسح كلمة مرور {username} وإعادتها للافتراضية؟\n(9876543210)", QMessageBox.Yes | QMessageBox.No)
        if confirm == QMessageBox.Yes:
            conn = None
            try:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute("UPDATE users SET password=%s WHERE username=%s", ("9876543210", username))
                conn.commit()
                cursor.close()
                QMessageBox.information(self, "نجاح", "تمت إعادة التعيين بنجاح")
            except Exception as e:
                if conn:
                    conn.rollback()
                QMessageBox.critical(self, "خطأ", f"فشل التعيين: {e}")
            finally:
                release_connection(conn)

    def delete_user(self, username):
        """حذف مستخدم من النظام"""
        if username == 'admin':
            QMessageBox.warning(self, "تنبيه", "لا يمكن حذف حساب المدير")
            return
            
        confirm = QMessageBox.question(self, "تأكيد الحذف", f"هل أنت متأكد من حذف المستخدم '{username}' نهائياً؟", QMessageBox.Yes | QMessageBox.No)
        if confirm == QMessageBox.Yes:
            conn = None
            try:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute("DELETE FROM users WHERE username=%s", (username,))
                conn.commit()
                cursor.close()
                QMessageBox.information(self, "نجاح", f"تم حذف المستخدم {username} بنجاح")
                self.refresh_users_table()
            except Exception as e:
                if conn:
                    conn.rollback()
                QMessageBox.critical(self, "خطأ", f"فشل الحذف: {e}")
            finally:
                release_connection(conn)

    def show_change_password_dialog(self, username):
        """نافذة تغيير كلمة المرور الاحترافية"""
        if username == 'admin' and self.user_data['username'] != 'admin':
            QMessageBox.warning(self, "تنبيه", "لا يمكن تعديل كلمة مرور المدير من هذه الصفحة.")
            return

        dialog = UserPasswordDialog(username, self)
        if dialog.exec() == QDialog.Accepted:
            new_pwd = dialog.password
            conn = None
            try:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute("UPDATE users SET password=%s WHERE username=%s", (new_pwd, username))
                conn.commit()
                cursor.close()
                QMessageBox.information(self, "نجاح", f"تم تغيير كلمة المرور للمستخدم {username} بنجاح")
            except Exception as e:
                if conn:
                    conn.rollback()
                QMessageBox.critical(self, "خطأ", f"فشل تغيير كلمة المرور: {e}")
            finally:
                release_connection(conn)


    def on_tab_changed(self, index):
        """تحديث جدول المستخدمين عند التبديل"""
        if hasattr(self, 'user_role') and self.user_role == 'admin' and index == 2: # تبويب المستخدمين
            self.refresh_users_table()

    def create_modern_header(self, parent_layout):
        """إنشاء الهيدر الاحترافي"""
        header_frame = QFrame()
        header_frame.setObjectName("mainHeader")
        header_frame.setFixedHeight(90)  # Increased from 80 to 90 for better spacing
        header_frame.setStyleSheet("#mainHeader { background-color: #2c3e50; border-bottom: 2px solid #3498db; }")
        
        header_layout = QHBoxLayout(header_frame)
        header_layout.setContentsMargins(30, 0, 30, 0)
        
        # اسم المستشفى الحالي
        hospital_label = QLabel()
        h_name = self.hospitals.get(self.selected_hospital, "النظام الرئيسي")
        username_display = f" ({self.user_data['username']})"
        hospital_label.setText(f"🏥 {h_name}{username_display}")
        hospital_label.setStyleSheet("color: white; font-size: 18px; font-weight: bold;")
        header_layout.addWidget(hospital_label)
        
        # FIX: Status Indicators - Smooth & Clean (No Boxes)
        status_layout = QHBoxLayout()
        status_layout.setSpacing(15)
        status_layout.setContentsMargins(0, 0, 0, 0)
        
        # Connection Status Indicator
        self.lbl_connection_status = QLabel("● متصل")
        self.lbl_connection_status.setStyleSheet("color: #10b981; font-weight: bold; font-size: 14px; background: transparent;")
        self.lbl_connection_status.setToolTip("حالة الاتصال بقاعدة البيانات")
        status_layout.addWidget(self.lbl_connection_status)
        
        # Sync Status Indicator
        self.lbl_sync_status = QLabel("✓ متزامن")
        self.lbl_sync_status.setStyleSheet("color: #3b82f6; font-weight: bold; font-size: 14px; background: transparent;")
        self.lbl_sync_status.setToolTip("حالة مزامنة البيانات")
        status_layout.addWidget(self.lbl_sync_status)
        
        header_layout.addLayout(status_layout)
        
        header_layout.addStretch()
        
        parent_layout.addWidget(header_frame)

    def update_system_status(self):
        """تحديث الوقت وحالة الاتصال والمزامنة بشكل دوري"""
        # 1. تحديث الوقت
        now = datetime.now()
        self.time_label.setText(now.strftime("📅 %Y-%m-%d  🕒 %I:%M:%S %p"))
        
        # 2. تحديث حالة الاتصال (مفصلة)
        is_pg_online = self.db.is_online()  # يتحقق من الإنترنت + PostgreSQL
        # FIXED: Use the centralized is_online() check for consistency
        internet_available = self.db.is_online()
        
        # تسجيل معلومات التصحيح
        system_logger.info(f"Status Update - Internet: {internet_available}, PostgreSQL: {is_pg_online}")
        
        # FIX: Update new header labels instead of status bar
        if is_pg_online:
            self.lbl_connection_status.setText("● متصل (Online)")
            self.lbl_connection_status.setStyleSheet("color: #10b981; font-weight: bold; background: transparent;")  # Green
        elif internet_available:
            # الإنترنت موجود لكن PostgreSQL غير متوفر
            self.lbl_connection_status.setText("🟡 متصل بالإنترنت (PostgreSQL غير مهيأ)")
            self.lbl_connection_status.setStyleSheet("color: #f59e0b; font-weight: bold; background: transparent;")  # Orange
        else:
            # لا إنترنت ولا PostgreSQL
            self.lbl_connection_status.setText("🔴 غير متصل (Offline)")
            self.lbl_connection_status.setStyleSheet("color: #ef4444; font-weight: bold; background: transparent;")  # Red
            
        # 3. تحديث حالة المزامنة
        sync_status = self.db.get_sync_status()
        pending = sync_status['pending']
        failed = sync_status['failed']
        processing = sync_status['processing']
        
        # FIX: Update new sync label in header
        if failed > 0:
            self.lbl_sync_status.setText(f"⚠️ {failed} فشلت, {pending} معلقة")
            self.lbl_sync_status.setStyleSheet("color: #ef4444; font-weight: bold; background: transparent;")  # Red
        elif processing > 0:
            self.lbl_sync_status.setText(f"🔄 {processing} قيد المعالجة")
            self.lbl_sync_status.setStyleSheet("color: #f59e0b; font-weight: bold; background: transparent;")  # Orange
        elif pending > 0:
            self.lbl_sync_status.setText(f"⏳ {pending} معلقة")
            self.lbl_sync_status.setStyleSheet("color: #3b82f6; font-weight: bold; background: transparent;")  # Blue
        else:
            last_sync = sync_status['last_sync']
            if last_sync:
                try:
                    # تحويل الوقت إلى سلسلة نصية بسيطة
                    last_sync_str = last_sync[:19]  # YYYY-MM-DD HH:MM:SS
                    if internet_available and not is_pg_online:
                        self.lbl_sync_status.setText(f"⚠️ محلي، آخر: {last_sync_str}")
                    elif is_pg_online:
                        self.lbl_sync_status.setText(f"✅ آخر: {last_sync_str}")
                    else:
                        self.lbl_sync_status.setText(f"🔴 غير متصل")
                except:
                    if internet_available and not is_pg_online:
                        self.lbl_sync_status.setText(f"⚠️ محلي")
                    elif is_pg_online:
                        self.lbl_sync_status.setText(f"✅ متصل")
                    else:
                        self.lbl_sync_status.setText(f"🔴 غير متصل")
            else:
                if internet_available and not is_pg_online:
                    self.lbl_sync_status.setText(f"⚠️ وضع محلي")
                elif is_pg_online:
                    self.lbl_sync_status.setText(f"✅ متصل")
                else:
                    self.lbl_sync_status.setText(f"🔴 غير متصل")

    def go_to_add_patient(self):
        """التبديل لتبويب إضافة مريض"""
        tabs = self.centralWidget().findChild(QTabWidget)
        if tabs:
            tabs.setCurrentIndex(0)
            self.entries['name'].setFocus()

    def handle_logout(self):
        """تسجيل الخروج وإعادة تشغيل البرنامج"""
        reply = QMessageBox.question(self, "تأكيد", "هل أنت متأكد من تسجيل الخروج؟",
                                   QMessageBox.Yes | QMessageBox.No)
        
        if reply == QMessageBox.Yes:
            try:
                # مسح ملف الإعدادات (تسجيل الدخول التلقائي)
                if os.path.exists(SETTINGS_PATH):
                    os.remove(SETTINGS_PATH)
                
                # إعادة تشغيل التطبيق
                QProcess.startDetached(sys.executable, sys.argv)
                QApplication.quit()
            except Exception as e:
                QMessageBox.critical(self, "خطأ", f"فشل تسجيل الخروج: {e}")


    
    def create_input_tab(self, tabs):
        """إنشاء تبويب إدخال البيانات"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(15)
        
        # بطاقة إدخال البيانات
        input_card = QFrame()
        input_card.setStyleSheet("background-color: white; border-radius: 12px; border: 1px solid #E2E8F0;")
        
        card_layout = QVBoxLayout(input_card)
        card_layout.setContentsMargins(30, 30, 30, 30)
        
        # عنوان البطاقة وملصق التحقق
        title_box = QHBoxLayout()
        title_label = QLabel("📝 تسجيل مريض جديد")
        title_label.setStyleSheet("font-size: 20px; font-weight: bold; color: #1E293B; background: transparent;")
        
        self.validation_label = QLabel("")
        self.validation_label.setStyleSheet("color: #EF4444; font-size: 13px; font-weight: bold; background: transparent;")
        
        title_box.addWidget(title_label)
        title_box.addStretch()
        title_box.addWidget(self.validation_label)
        card_layout.addLayout(title_box)
        
        # شبكة الإدخالات
        grid_layout = QGridLayout()
        grid_layout.setVerticalSpacing(20)
        grid_layout.setHorizontalSpacing(30)
        
        # إنشاء حقول الإدخال
        self.entries = {}
        labels = ["الرقم الموحد:", "الرقم القومي:", "الاسم رباعي:", "السن (سنة):", "نوع الدخول:", "رقم التليفون:", "العنوان:", "مدخل البيانات:"]
        
        for i, label_text in enumerate(labels):
            label = QLabel(label_text)
            label.setStyleSheet("font-size: 16px; font-weight: bold; color: #555;")
            grid_layout.addWidget(label, i, 0)
            
            if label_text == "نوع الدخول:":
                combo = QComboBox()
                combo.addItems(["عيادات", "طوارئ", "داخلي"])
                combo.setStyleSheet("QComboBox { padding: 10px; border: 2px solid #e0e0e0; border-radius: 8px; font-size: 14px; min-width: 300px; }")
                self.entries['type'] = combo
                grid_layout.addWidget(combo, i, 1)

                # إضافة حقول طبية بجانب الحقول العادية أو تحتها
                # سنضيفها في عمود ثالث (كـ Section طبي)
                if i == 0:
                    med_label = QLabel("🩺 البيانات الطبية (EMR):")
                    med_label.setStyleSheet("font-size: 18px; font-weight: bold; color: #2563eb; margin-top: 20px;")
                    grid_layout.addWidget(med_label, 0, 2, 1, 2)
                
                # فصيلة الدم
                grid_layout.addWidget(QLabel("🩸 فصيلة الدم:"), 1, 2)
                bt_combo = QComboBox()
                bt_combo.addItems(["-", "A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"])
                self.entries['blood_type'] = bt_combo
                grid_layout.addWidget(bt_combo, 1, 3)

                # الأمراض المزمنة
                grid_layout.addWidget(QLabel("📋 أمراض مزمنة:"), 2, 2)
                chronic_d = QLineEdit()
                self.entries['chronic_diseases'] = chronic_d
                grid_layout.addWidget(chronic_d, 2, 3)

                # الأدوية المزمنة
                grid_layout.addWidget(QLabel("💊 أدوية مزمنة:"), 3, 2)
                chronic_m = QLineEdit()
                self.entries['chronic_medications'] = chronic_m
                grid_layout.addWidget(chronic_m, 3, 3)

                # الحساسية
                grid_layout.addWidget(QLabel("⚠️ الحساسية:"), 4, 2)
                allergies = QLineEdit()
                self.entries['allergies'] = allergies
                grid_layout.addWidget(allergies, 4, 3)

                # ملاحظات طبية
                grid_layout.addWidget(QLabel("📝 ملاحظات:"), 5, 2)
                med_notes = QLineEdit()
                self.entries['medical_notes'] = med_notes
                grid_layout.addWidget(med_notes, 5, 3)

                # الشكوى الحالية
                grid_layout.addWidget(QLabel("🤒 الشكوى الحالية:"), 6, 2)
                curr_comp = QLineEdit()
                self.entries['current_complaint'] = curr_comp
                grid_layout.addWidget(curr_comp, 6, 3)

                # ملاحظة: تم نقل إعداد المكمل التلقائي لنهاية الدالة
                pass
            elif label_text == "السن (سنة):":
                entry = QLineEdit()
                entry.setPlaceholderText("أدخل السن بالسنوات")
                entry.setStyleSheet("QLineEdit { padding: 10px; border: 2px solid #e0e0e0; border-radius: 8px; font-size: 14px; min-width: 300px; }")
                self.entries['age'] = entry
                grid_layout.addWidget(entry, i, 1)
            elif label_text == "مدخل البيانات:":
                entry = QLineEdit()
                entry.setReadOnly(True)
                # يتم ملء هذا الحقل تلقائياً بمعلومات المستخدم الحالي
                entry.setText(self.user_data.get('username', 'unknown') if hasattr(self, 'user_data') else 'system')
                entry.setStyleSheet("QLineEdit { padding: 10px; border: 2px solid #e0e0e0; border-radius: 8px; font-size: 14px; min-width: 300px; background-color: #f5f5f5; }")
                self.entries['data_entry_personnel'] = entry
                grid_layout.addWidget(entry, i, 1)
            else:
                entry = QLineEdit()
                
                if label_text == "الرقم الموحد:":
                    entry.setReadOnly(True)
                    entry.setPlaceholderText("يتم إنشاؤه تلقائياً")
                    entry.setStyleSheet("QLineEdit { padding: 10px; border: 2px solid #e0e0e0; border-radius: 8px; font-size: 14px; min-width: 300px; background-color: #f5f5f5; color: #1976D2; font-weight: bold; }")
                    self.entries['patient_id'] = entry
                else:
                    entry.setStyleSheet("QLineEdit { padding: 10px; border: 2px solid #e0e0e0; border-radius: 8px; font-size: 14px; min-width: 300px; }")
                    
                    if label_text == "الاسم رباعي:":
                        entry.setPlaceholderText("مثال: أحمد محمد علي حسن")
                        self.entries['name'] = entry
                    elif label_text == "الرقم القومي:":
                        entry.setPlaceholderText("أدخل 14 رقم")
                        self.entries['national_id'] = entry
                        entry.textChanged.connect(self.on_national_id_changed)
                    elif label_text == "رقم التليفون:":
                        entry.setPlaceholderText("أدخل رقم التليفون")
                        self.entries['phone'] = entry
                    elif label_text == "العنوان:":
                        entry.setPlaceholderText("أدخل العنوان بالكامل")
                        self.entries['address'] = entry
                
                grid_layout.addWidget(entry, i, 1)
        
        card_layout.addLayout(grid_layout)
        
        # ربط التحقق التلقائي
        self.entries['name'].textChanged.connect(lambda: self.validate_inputs())
        self.entries['phone'].textChanged.connect(lambda: self.validate_inputs())
        
        card_layout.addSpacing(10)
        
        # إعداد الإكمال التلقائي للحقول الطبية
        self.setup_completers()
        
        # أزرار التحكم
        button_layout = QHBoxLayout()
        button_layout.setSpacing(15)
        
        buttons = [
            ("💾 حفظ المريض", self.save_patient, "#4CAF50", "#388E3C"),
            ("🔄 مسح", self.clear_entries, "#9E9E9E", "#757575")
        ]
        
        for text, func, color, hover_color in buttons:
            btn = QPushButton(text)
            btn.setStyleSheet(f"QPushButton {{ background-color: {color}; color: white; border: none; border-radius: 8px; padding: 12px 25px; font-size: 14px; font-weight: bold; min-width: 100px; }} QPushButton:hover {{ background-color: {hover_color}; }}")
            btn.clicked.connect(func)
            button_layout.addWidget(btn)
        
        # زر إضافة مريض مجهول
        unknown_btn = QPushButton("➕ مريض مجهول")
        unknown_btn.setStyleSheet("QPushButton { background-color: #f59e0b; color: white; border: none; border-radius: 8px; padding: 12px 25px; font-size: 14px; font-weight: bold; min-width: 100px; } QPushButton:hover { background-color: #d97706; }")
        unknown_btn.clicked.connect(self.add_unknown_patient)
        button_layout.addWidget(unknown_btn)
        
        button_layout.addStretch()
        card_layout.addLayout(button_layout)
        
        layout.addWidget(input_card)
        layout.addStretch()
        
        tabs.addTab(tab, "➕ إدخال بيانات")

    def setup_completers(self):
        """إعداد المكمل التلقائي للحقول الطبية بناءً على البيانات السابقة"""
        fields_to_complete = {
            'chronic_diseases': 'chronic_diseases',
            'medical_notes': 'medical_notes',
            'allergies': 'allergies',
            'current_complaint': 'current_complaint'
        }
        
        for entry_key, col_name in fields_to_complete.items():
            if entry_key in self.entries:
                values = self.db.get_distinct_values(col_name)
                if values:
                    completer = QCompleter(values)
                    completer.setCaseSensitivity(Qt.CaseInsensitive)
                    completer.setFilterMode(Qt.MatchContains)
                    self.entries[entry_key].setCompleter(completer)
    
    def create_display_tab(self, tabs):
        """إنشاء تبويب عرض المرضى"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # شريط البحث
        search_frame = QFrame()
        search_frame.setStyleSheet("QFrame { background-color: white; border-radius: 10px; padding: 15px; }")
        
        search_layout = QHBoxLayout(search_frame)
        search_layout.setContentsMargins(15, 5, 15, 5)
        
        search_icon = QLabel("🔍")
        search_icon.setStyleSheet("font-size: 18px; color: #3B82F6;")
        search_layout.addWidget(search_icon)
        
        self.search_entry = QLineEdit()
        self.search_entry.setPlaceholderText("ابحث عن المرضى (الاسم أو الرقم القومي)...")
        self.search_entry.setStyleSheet("QLineEdit { border: none; background: transparent; padding: 10px; font-size: 15px; color: #1E293B; }")
        # Debounce: انتظار 300ms بعد الكتابة قبل تنفيذ البحث لتقليل الضغط على السيرفر
        self._search_timer = QTimer(singleShot=True)
        self._search_timer.timeout.connect(self.filter_patients)
        self.search_entry.textChanged.connect(lambda: self._search_timer.start(300))
        search_layout.addWidget(self.search_entry)
        
        layout.addWidget(search_frame)
        
        # جدول المرضى مع أزرار التحكم
        self.create_patients_table(layout)
        
        # أزرار التنقل بين الصفحات (Pagination)
        pagination_frame = QFrame()
        pagination_frame.setStyleSheet("QFrame { background-color: white; border-radius: 8px; padding: 10px; margin-top: 10px; }")
        
        pagination_layout = QHBoxLayout(pagination_frame)
        pagination_layout.addStretch()
        
        self.prev_btn = QPushButton("→ السابق")
        self.prev_btn.setStyleSheet("QPushButton { background-color: #2196F3; color: white; border: none; border-radius: 5px; padding: 8px 15px; font-size: 14px; min-width: 80px; } QPushButton:hover { background-color: #1976D2; } QPushButton:disabled { background-color: #b0b0b0; }")
        self.prev_btn.clicked.connect(self.previous_page)
        pagination_layout.addWidget(self.prev_btn)
        
        self.page_label = QLabel("الصفحة 1")
        self.page_label.setStyleSheet("font-size: 14px; font-weight: bold; padding: 0 20px; color: #1976D2;")
        pagination_layout.addWidget(self.page_label)
        
        self.next_btn = QPushButton("التالي ←")
        self.next_btn.setStyleSheet("QPushButton { background-color: #2196F3; color: white; border: none; border-radius: 5px; padding: 8px 15px; font-size: 14px; min-width: 80px; } QPushButton:hover { background-color: #1976D2; } QPushButton:disabled { background-color: #b0b0b0; }")
        self.next_btn.clicked.connect(self.next_page)
        pagination_layout.addWidget(self.next_btn)
        
        pagination_layout.addStretch()
        
        layout.addWidget(pagination_frame)
        
        # Load data into table now that all UI elements are initialized
        self.refresh_patients_list()
        
        tabs.addTab(tab, "📋 قائمة المرضى")
    
    def create_patients_table(self, parent_layout):
        """إنشاء جدول المرضى مع نظام Delegate الاحترافي"""
        table_container = QFrame()
        table_container.setStyleSheet("QFrame { background-color: white; border-radius: 12px; border: 1px solid #E2E8F0; }")
        container_layout = QVBoxLayout(table_container)
        container_layout.setContentsMargins(0, 0, 0, 0)
        container_layout.setSpacing(0)

        self.table = QTableWidget()
        headers = ["الرقم الموحد", "الاسم رباعياً", "الرقم القومي", "تاريخ الميلاد", "نوع الدخول", "التليفون", "العنوان", "المستشفى", "تاريخ التسجيل"]
        self.table.setColumnCount(len(headers))
        self.table.setHorizontalHeaderLabels(headers)

        # --- Header احترافي ---
        h = self.table.horizontalHeader()
        h.setFixedHeight(50)
        h.setDefaultAlignment(Qt.AlignCenter)
        h.setStyleSheet("QHeaderView::section { background-color: #F8FAFC; color: #1E293B; padding: 12px 8px; font-size: 14px; font-weight: bold; border: none; border-bottom: 2px solid #CBD5E1; border-right: 1px solid #E2E8F0; } QHeaderView::section:last { border-right: none; }")

        # --- توزيع الأعمدة ---
        h.setSectionResizeMode(0, QHeaderView.ResizeToContents) # الرقم الموحد
        h.setSectionResizeMode(1, QHeaderView.Stretch)           # الاسم
        h.setSectionResizeMode(2, QHeaderView.ResizeToContents) # الرقم القومي
        h.setSectionResizeMode(3, QHeaderView.ResizeToContents) # تاريخ الميلاد
        h.setSectionResizeMode(4, QHeaderView.ResizeToContents) # نوع الدخول
        h.setSectionResizeMode(5, QHeaderView.ResizeToContents) # التليفون
        h.setSectionResizeMode(6, QHeaderView.Stretch)           # العنوان
        h.setSectionResizeMode(7, QHeaderView.ResizeToContents) # المستشفى
        h.setSectionResizeMode(8, QHeaderView.ResizeToContents) # تاريخ التسجيل

        # --- خصائص الجدول ---
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setShowGrid(False)
        self.table.verticalHeader().setVisible(False)
        self.table.verticalHeader().setDefaultSectionSize(70)
        self.table.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.table.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.table.setSortingEnabled(True)
        self.table.setMouseTracking(True) # ضروري للـ hover في Delegate
        
        # تم إزالة عمود التحكم والـ Delegate الخاص به بناءً على طلب المستخدم لتوسيع المساحة
        
        # تفعيل نسخ المحتوى عند الضغط مرتين
        self.table.cellDoubleClicked.connect(self.copy_cell_to_clipboard)

        # تفعيل القائمة المنبثقة (Context Menu) بالزر الأيمن
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)

        container_layout.addWidget(self.table)
        parent_layout.addWidget(table_container)

    def copy_cell_to_clipboard(self, row, col):
        """نسخ محتوى الخلية للحافظة"""
        item = self.table.item(row, col)
        if item:
            text = item.text()
            QApplication.clipboard().setText(text)
            self.statusBar().showMessage(f"تم نسخ: {text}", 2000)

    def show_context_menu(self, pos):
        """عرض قائمة الخيارات عند الضغط بالزر الأيمن بشكل آمن"""
        index = self.table.indexAt(pos)
        if not index.isValid():
            return
            
        row = index.row()
        item_id = self.table.item(row, 0)
        item_nid = self.table.item(row, 2)
        
        if not item_id or not item_nid:
            return
            
        patient_id = item_id.text()
        national_id = item_nid.text()
        
        menu = QMenu(self)
        menu.setStyleSheet("QMenu { background-color: white; border: 1px solid #CBD5E1; border-radius: 8px; padding: 5px; } QMenu::item { padding: 8px 25px; border-radius: 4px; } QMenu::item:selected { background-color: #EFF6FF; color: #2563EB; }")
        
        view_action = menu.addAction("👁️ عرض السجل")
        edit_action = menu.addAction("✏️ تعديل البيانات")
        delete_action = menu.addAction("🗑️ حذف المريض")
        menu.addSeparator()
        copy_action = menu.addAction("📋 نسخ الرقم القومي")
        
        action = menu.exec(self.table.viewport().mapToGlobal(pos))
        
        if action == view_action:
            self.show_patient_history(patient_id)
        elif action == edit_action:
            self.edit_patient_from_table(patient_id)
        elif action == delete_action:
            self.delete_patient_from_table(patient_id)
        elif action == copy_action:
            QApplication.clipboard().setText(national_id)
            self.statusBar().showMessage(f"تم نسخ الرقم القومي: {national_id}", 2000)

    

    def on_national_id_changed(self):
        """البحث التلقائي عند تغيير الرقم القومي مع استخراج آلي للبيانات"""
        national_id = self.entries['national_id'].text()
        
        if len(national_id) == 14 and national_id.isdigit():
            # FIX: Auto-parse Egyptian National ID
            parsed = NationalIDParser.parse(national_id)
            
            if parsed:
                # Auto-fill extracted data
                if 'birth_date' in self.entries:
                    self.entries['birth_date'].setText(parsed['birth_date'])
                if 'age' in self.entries:
                    self.entries['age'].setText(str(parsed['age']))
                
                # Set gender if field exists (assuming index 0=male, 1=female)
                if 'gender' in self.entries:
                    gender_index = 0 if parsed['gender'] == "ذكر" else 1
                    self.entries['gender'].setCurrentIndex(gender_index)
                
                # Optional: Fill governorate if field exists
                if 'governorate' in self.entries:
                    self.entries['governorate'].setText(parsed['governorate'])
                
                # Show success message
                self.statusBar().showMessage(
                    f"✅ تم استخراج البيانات: العمر={parsed['age']}، الجنس={parsed['gender']}، المحافظة={parsed['governorate']}",
                    5000
                )
            
            # استخراج تاريخ الميلاد والتحقق (محلي - سريع)
            self.calculate_age_from_national_id()
            self.validate_inputs()
            
            # منع عمليات البحث المتعددة المتزامنة - مع التحقق من حذف الكائن C++
            if hasattr(self, 'lookup_thread') and self.lookup_thread is not None:
                try:
                    if self.lookup_thread.isRunning():
                        return
                except RuntimeError:
                    # Internal C++ object already deleted
                    self.lookup_thread = None
            
            # بدء البحث في الخلفية (غير محجوز للواجهة)
            # Pass self.db to worker for PG + SQLite support
            self.lookup_worker = PatientLookupWorker(national_id, self.db)
            self.lookup_thread = QThread()
            
            self.lookup_worker.moveToThread(self.lookup_thread)
            
            self.lookup_thread.started.connect(self.lookup_worker.run)
            self.lookup_worker.finished.connect(self.on_lookup_finished_main)
            
            # تنظيف المخلفات
            self.lookup_worker.finished.connect(self.lookup_thread.quit)
            self.lookup_worker.finished.connect(self.lookup_worker.deleteLater)
            self.lookup_thread.finished.connect(self.lookup_thread.deleteLater)
            
            self.lookup_thread.start()
    
    def on_lookup_finished_main(self, exists, patient):
        """التعامل مع نتيجة البحث عن مريض في الواجهة الرئيسية"""
        if exists and patient:
            # تعبئة النموذج ببيانات المريض
            self.entries['patient_id'].setText(patient.get('patient_id', ''))
            self.entries['name'].setText(patient.get('name', ''))
            self.entries['age'].setText(str(patient.get('age', '')))
            
            # تعبئة الحقول الطبية
            mappings = {
                'phone': 'phone', 'address': 'address', 
                'chronic_diseases': 'chronic_diseases',
                'chronic_medications': 'chronic_medications',
                'allergies': 'allergies', 'medical_notes': 'medical_notes',
                'current_complaint': 'current_complaint'
            }
            for entry_key, patient_key in mappings.items():
                if entry_key in self.entries:
                    self.entries[entry_key].setText(str(patient.get(patient_key, '')))
            
            # تعبئة الجنس (assuming index 0=male, 1=female)
            if 'gender' in self.entries:
                gender_val = patient.get('gender', '')
                if gender_val == "ذكر":
                    self.entries['gender'].setCurrentIndex(0)
                elif gender_val == "أنثى":
                    self.entries['gender'].setCurrentIndex(1)

            if 'blood_type' in self.entries:
                self.entries['blood_type'].setCurrentText(patient.get('blood_type', '-'))

            # TWEAK: Show a more prominent warning for existing patients
            QMessageBox.warning(
                self, 
                "مريض موجود مسبقاً", 
                f"⚠️ تنبيه: الرقم القومي ({self.entries['national_id'].text()}) مسجل بالفعل لمريض باسم:\n\n"
                f"👤 {patient.get('name', 'غير معروف')}\n"
                f"📌 الرقم الموحد: {patient.get('patient_id', 'غير معروف')}\n\n"
                "تم تحميل بيانات المريض تلقائياً. يمكنك تحديث البيانات أو إضافة زيارة جديدة."
            )
            self.statusBar().showMessage(f"✅ تم العثور على مريض: {patient.get('name', 'غير معروف')}", 5000)
        else:
            # مريض جديد: تفعيل الـ Auto-Focus لحقل الاسم
            # التأكد من أن الرقم لا يزال 14 رقماً (لم يتم حذفه أثناء البحث)
            national_id = self.entries['national_id'].text().strip()
            if len(national_id) == 14:
                # تأخير بسيط لضمان سلاسة الانتقال
                QTimer.singleShot(50, lambda: self.entries['name'].setFocus())
                
                # التحقق من مرضى مشابهين (نافذة منبثقة)
                self.check_similar_patients_popup()
    
    def check_similar_patients_popup(self):
        """التحقق من المرضى المشابهين وعرضهم في نافذة منبثقة"""
        name = self.entries['name'].text().strip()
        national_id = self.entries['national_id'].text().strip()
        
        if not name and len(national_id) < 10:
            return
        
        # بحث عن مرضى مشابهين في thread منفصل لتجنب تجميد الواجهة
        def search_similar():
            similar = self.db.search_similar_patients(
                name=name if name else None,
                national_id_prefix=national_id[:10] if len(national_id) >= 10 else None
            )
            
            if similar:
                # استخدام Signal بدلاً من Q_ARG التي لا تعمل في PySide6 مع أنواع Python
                self._similar_patients_result = similar
                QMetaObject.invokeMethod(self, "_show_similar_from_thread",
                                        Qt.ConnectionType.QueuedConnection)
        
        thread = threading.Thread(target=search_similar, daemon=True)
        thread.start()
    
    @Slot()
    def _show_similar_from_thread(self):
        """عرض نافذة المرضى المشابهين بأمان من الـ main thread"""
        similar = getattr(self, '_similar_patients_result', [])
        if similar:
            self.show_similar_patients_dialog(similar)
    
    @Slot(list)
    def show_similar_patients_dialog(self, similar_patients):
        """عرض نافذة المرضى المشابهين"""
        dialog = SimilarPatientDialog(similar_patients, self)
        dialog.exec()
    
    def calculate_age_from_national_id(self):
        """حساب السن من الرقم القومي المصري"""
        national_id = self.entries['national_id'].text()
        if len(national_id) == 14 and national_id.isdigit():
            # استخدام دالة الفحص الجديدة من DatabaseManager
            parsed_data = self.db.parse_egyptian_national_id(national_id)
            
            if parsed_data:
                # حساب السن من تاريخ الميلاد
                import datetime
                try:
                    birth_date_str = parsed_data['birth_date']  # Format: "DD/MM/YYYY"
                    birth_date = datetime.datetime.strptime(birth_date_str, "%d/%m/%Y").date()
                    today = datetime.date.today()
                    age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
                    self.entries['age'].setText(str(age))
                except Exception as e:
                    error_logger.error(f"Error calculating age: {e}")
    
    def generate_patient_id(self):
        """توليد رقم مريض موحد فريد يعمل Online/Offline"""
        hospital_code = self.selected_hospital if self.selected_hospital else "GEN"
        return self.db.generate_patient_id(hospital_code)

    def validate_inputs(self):
        """التحقق من صحة البيانات المدخلة (تم إزالة المعاملات الميتة)"""
        name = self.entries['name'].text().strip()
        national_id = self.entries['national_id'].text().strip()
        phone = self.entries['phone'].text().strip()
        
        # 1. التحقق من الرقم القومي (إجباري - التحقق المصري الرسمي)
        if not national_id:
            self.validation_label.setText("⚠ الرقم القومي مطلوب")
            self.validation_label.setStyleSheet("color: #EF4444; font-weight: bold; background: transparent;")
            return False
        
        # استخدام الفحص المحسن من DatabaseManager (يشمل رقم التحقق والتاريخ والمحافظة)
        nid_data = self.db.parse_egyptian_national_id(national_id)
        if not nid_data:
            self.validation_label.setText("⚠ الرقم القومي غير صحيح أو لا يطابق النظام المصري")
            self.validation_label.setStyleSheet("color: #EF4444; font-weight: bold; background: transparent;")
            return False

        # 2. التحقق من الاسم (إجباري - رباعي على الأقل)
        if not name:
            self.validation_label.setText("⚠ الاسم مطلوب")
            self.validation_label.setStyleSheet("color: #EF4444; font-weight: bold; background: transparent;")
            return False
        if len(name.split()) < 4:
            self.validation_label.setText("⚠ يجب إدخال الاسم رباعياً على الأقل")
            self.validation_label.setStyleSheet("color: #EF4444; font-weight: bold; background: transparent;")
            return False

        # 3. التحقق من رقم الهاتف (اختياري)
        if phone:
            valid_prefixes = ('010', '011', '012', '015')
            if not phone.isdigit() or len(phone) != 11 or not phone.startswith(valid_prefixes):
                self.validation_label.setText("⚠ رقم التليفون غير صحيح")
                self.validation_label.setStyleSheet("color: #EF4444; font-weight: bold; background: transparent;")
                return False

        self.validation_label.setText("✅ البيانات جاهزة للحفظ")
        self.validation_label.setStyleSheet("color: #10B981; font-weight: bold; background: transparent;")
        return True
    
    def save_patient(self):
        """حفظ بيانات المريض"""
        if not self.validate_inputs():
            QMessageBox.warning(self, "خطأ في البيانات", self.validation_label.text())
            return

        name = self.entries['name'].text().strip()
        national_id = self.entries['national_id'].text().strip()
        entry_type_ar = self.entries['type'].currentText()
        phone = self.entries['phone'].text().strip()
        address = self.entries['address'].text().strip()
        
        entry_type = TYPE_MAP_EN.get(entry_type_ar, "Clinic")
        
        # استخراج البيانات من الرقم القومي
        parsed_nid = self.db.parse_egyptian_national_id(national_id)
        gender = parsed_nid.get('gender', '') if parsed_nid else ''
        governorate = parsed_nid.get('governorate', '') if parsed_nid else ''
        birth_date = parsed_nid.get('birth_date', '') if parsed_nid else ''
        
        patient_id = self.entries['patient_id'].text().strip()
        
        # إذا كان هناك رقم مريض، فهذا يعني أننا في وضع "التعديل"
        if patient_id and patient_id != "سيظهر هنا...":
            updated_data = {
                'name': name,
                'national_id': national_id,
                'birth_date': birth_date,
                'age': int(self.entries['age'].text()) if self.entries['age'].text().isdigit() else None,
                'entry_type': entry_type,
                'phone': phone,
                'address': address,
                'gender': gender,
                'governorate': governorate,
                'blood_type': self.entries['blood_type'].currentText() if 'blood_type' in self.entries else '',
                'chronic_diseases': self.entries['chronic_diseases'].text().strip() if 'chronic_diseases' in self.entries else '',
                'chronic_medications': self.entries['chronic_medications'].text().strip() if 'chronic_medications' in self.entries else '',
                'allergies': self.entries['allergies'].text().strip() if 'allergies' in self.entries else '',
                'medical_notes': self.entries['medical_notes'].text().strip() if 'medical_notes' in self.entries else '',
                'current_complaint': self.entries['current_complaint'].text().strip() if 'current_complaint' in self.entries else '',
                'data_entry_personnel': self.entries['data_entry_personnel'].text()
            }
            user_id = self.user_data.get('username', 'system')
            success, msg = self.db.update_patient(patient_id, updated_data, user_id=user_id, user_info=self.user_data)
            
            if success:
                # مسح الكاش وتحديث القائمة فوراً
                self.current_page = 0
                self.refresh_patients_list(force_fresh=True)
                
                # إنشاء زيارة جديدة للمريض
                admission_data = {
                    'entry_type': entry_type,
                    'department': '',
                    'chief_complaint': self.entries['current_complaint'].text().strip() if 'current_complaint' in self.entries else '',
                    'initial_diagnosis': '',
                    'chronic_diseases': self.entries['chronic_diseases'].text().strip() if 'chronic_diseases' in self.entries else '',
                    'chronic_medications': self.entries['chronic_medications'].text().strip() if 'chronic_medications' in self.entries else '',
                    'allergies': self.entries['allergies'].text().strip() if 'allergies' in self.entries else '',
                    'notes': ''
                }
                visit_id = self.db.create_visit(patient_id, self.selected_hospital, self.selected_hospital_name, admission_data, user_id=user_id)
                
                if visit_id:
                    # فتح نافذة الزيارة
                    visits = self.db.get_visits(patient_id, limit=1)
                    if visits:
                        visit_data = visits[0]
                        dialog = VisitDetailsDialog(self.db, patient_id, visit_data, self.hospitals, self)
                        if dialog.exec() == QDialog.Accepted:
                            self.load_visits()
                
                self.clear_entries()
                QMessageBox.information(self, "نجاح", "تم تحديث بيانات المريض بنجاح")
            else:
                # الإصلاح: إذا كان مريض من مستشفى أخرى، نسجل له زيارة فقط
                if "الملف الطبي فقط إذا زار المريض المستشفى الحالية" in msg or "مستشفى أخرى" in msg:
                    admission_data = {
                        'entry_type': entry_type,
                        'department': '',
                        'chief_complaint': updated_data['current_complaint'],
                        'initial_diagnosis': '',
                        'chronic_diseases': updated_data['chronic_diseases'],
                        'chronic_medications': updated_data['chronic_medications'],
                        'allergies': updated_data['allergies'],
                        'notes': updated_data['medical_notes']
                    }
                    visit_id = self.db.create_visit(patient_id, self.selected_hospital, self.selected_hospital_name, admission_data, user_id=user_id)
                    if visit_id:
                        # مسح الكاش وتحديث القائمة فوراً
                        self.current_page = 0
                        self.refresh_patients_list(force_fresh=True)
                        self.clear_entries()
                        QMessageBox.information(self, "تم تسجيل زيارة", "✅ تم تسجيل زيارة جديدة بنجاح للمريض (مريض من مستشفى أخرى)")
                        
                        # فتح نافذة الزيارة
                        visits = self.db.get_visits(patient_id, limit=1)
                        if visits:
                            dialog = VisitDetailsDialog(self.db, patient_id, visits[0], self.hospitals, self)
                            dialog.exec()
                    else:
                        QMessageBox.warning(self, "فشل", "فشل تسجيل الزيارة للمريض")
                else:
                    QMessageBox.warning(self, "فشل التعديل", f"لا يمكن تعديل هذا السجل:\n{msg}")
            return
            
        # أما إذا كان الحقل فارغاً، فهذا مريض جديد أو مريض موجود بالرقم القومي
        # التحقق من وجود رقم قومي مطابق
        existing = self.db.search_patient_by_national_id(national_id)
        
        if existing:
            # المريض موجود بالفعل - تسجيل زيارة جديدة وتحديث البيانات إن أمكن
            patient_id = existing['patient_id']
            user_id = self.user_data.get('username', 'system')
            
            # البيانات المطلوب تحديثها (بيانات المريض الأساسية + الطبية العامة)
            updated_data = {
                'name': name,
                'national_id': national_id,
                'birth_date': birth_date,
                'age': int(self.entries['age'].text()) if self.entries['age'].text().isdigit() else None,
                'phone': phone,
                'address': address,
                'gender': gender,
                'governorate': governorate,
                'blood_type': self.entries['blood_type'].currentText() if 'blood_type' in self.entries else '',
                'chronic_diseases': self.entries['chronic_diseases'].text().strip() if 'chronic_diseases' in self.entries else '',
                'chronic_medications': self.entries['chronic_medications'].text().strip() if 'chronic_medications' in self.entries else '',
                'allergies': self.entries['allergies'].text().strip() if 'allergies' in self.entries else '',
                'medical_notes': self.entries['medical_notes'].text().strip() if 'medical_notes' in self.entries else '',
                'current_complaint': self.entries['current_complaint'].text().strip() if 'current_complaint' in self.entries else '',
                'updated_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # 1. محاولة تحديث البيانات الأساسية (باستخدام نظام الصلاحيات المحدث)
            update_success, update_msg = self.db.update_patient(
                patient_id, 
                updated_data, 
                user_id=user_id, 
                user_info=self.user_data
            )
            
            # 2. تسجيل الزيارة (دائماً مسموحة للمريض الموجود)
            admission_data = {
                'entry_type': entry_type,
                'department': self.entries['medical_notes'].text().strip() if 'medical_notes' in self.entries else '', # استخدام قسم أو ملاحظات
                'chief_complaint': self.entries['current_complaint'].text().strip() if 'current_complaint' in self.entries else '',
                'initial_diagnosis': '',
                'chronic_diseases': updated_data['chronic_diseases'],
                'chronic_medications': updated_data['chronic_medications'],
                'allergies': updated_data['allergies'],
                'notes': updated_data['medical_notes']
            }
            
            visit_id = self.db.create_visit(
                patient_id, 
                self.selected_hospital, 
                self.selected_hospital_name, 
                admission_data, 
                user_id=user_id
            )
            
            if visit_id:
                # مسح الكاش وتحديث القائمة فوراً
                self.current_page = 0
                self.refresh_patients_list(force_fresh=True)
                self.clear_entries()
                
                if update_success:
                    QMessageBox.information(self, "نجاح", f"✅ تم تحديث بيانات المريض وتسجيل زيارة جديدة بنجاح.\nالرقم الموحد: {patient_id}")
                else:
                    QMessageBox.warning(self, "تم تسجيل زيارة فقط", 
                        f"✅ تم تسجيل زيارة جديدة بنجاح للمستشفى الحالية.\n\n"
                        f"⚠️ ملاحظة: {update_msg}\n"
                        f"(لا يمكن تعديل البيانات المشتركة إلا من مستشفى التسجيل الأصلية)")
            else:
                QMessageBox.critical(self, "خطأ", "❌ فشل تسجيل الزيارة في قاعدة البيانات.")
            
            return

        # إنشاء مريض جديد كلياً
        patient_id = self.db.generate_patient_id(self.selected_hospital)
        patient_data = {
            'patient_id': patient_id,
            'name': name,
            'national_id': national_id,
            'birth_date': birth_date,
            'age': int(self.entries['age'].text()) if self.entries['age'].text().isdigit() else None,
            'entry_type': entry_type,
            'phone': phone,
            'address': address,
            'gender': gender,
            'governorate': governorate,
            'blood_type': self.entries['blood_type'].currentText() if 'blood_type' in self.entries else '',
            'chronic_diseases': self.entries['chronic_diseases'].text().strip() if 'chronic_diseases' in self.entries else '',
            'chronic_medications': self.entries['chronic_medications'].text().strip() if 'chronic_medications' in self.entries else '',
            'allergies': self.entries['allergies'].text().strip() if 'allergies' in self.entries else '',
            'medical_notes': self.entries['medical_notes'].text().strip() if 'medical_notes' in self.entries else '',
            'current_complaint': self.entries['current_complaint'].text().strip() if 'current_complaint' in self.entries else '',
            'data_entry_personnel': self.entries['data_entry_personnel'].text(),
            'hospital': self.selected_hospital,
            'registration_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        user_id = self.user_data.get('username', 'system')
        
        # Define callback for async UI refresh
        def on_saved(success, error=None, duplication_warnings=None):
            if success:
                warning_text = ""
                if duplication_warnings:
                    warning_text = "⚠ تحذير احتمالية تكرار:\n- " + "\n- ".join(duplication_warnings[:3])
                # Use invokeMethod for thread-safe UI update
                QMetaObject.invokeMethod(self, "_handle_save_completion", 
                                       Qt.QueuedConnection,
                                       Q_ARG(str, patient_id),
                                       Q_ARG(str, warning_text))
            else:
                QMetaObject.invokeMethod(self, "_handle_save_error", 
                                       Qt.QueuedConnection,
                                       Q_ARG(str, str(error) if error else "فشل في حفظ بيانات المريض"))

        self.db.add_patient(patient_data, user_id=user_id, callback=on_saved)
        # We don't return here yet, the success message and refresh happen in _handle_save_completion
        return

    def add_unknown_patient(self):
        """إضافة مريض مجهول الهوية"""
        reply = QMessageBox.question(
            self, 
            "تأكيد", 
            "هل تريد إضافة مريض مجهول الهوية؟\n\n"
            "سيتم إنشاء رقم موحد مؤقت واسم تلقائي (مجهول X)",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            user_id = self.user_data.get('username', 'system')
            success, patient_id, name = self.db.add_unknown_patient(
                self.selected_hospital, 
                user_id=user_id
            )
            
            if success:
                QMessageBox.information(
                    self, 
                    "تم الإنشاء", 
                    f"✅ تم إنشاء مريض مجهول\n"
                    f"🔢 الرقم: {patient_id}\n"
                    f"👤 الاسم: {name}\n\n"
                    f"يمكنك الآن فتح ملف المريض وإضافة زيارة."
                )
                # مسح الكاش وتحديث القائمة فوراً
                self.current_page = 0
                self.refresh_patients_list(force_fresh=True)
                
                # فتح ملف المريض المجهول
                self.show_patient_history(patient_id)
            else:
                QMessageBox.warning(self, "خطأ", f"فشل إنشاء المريض: {name}")

    def edit_patient_from_table(self, patient_id):
        """تعديل بيانات المريض من الجدول"""
        patient = self.db.search_patient_by_id(patient_id)
        if patient is None:
            QMessageBox.warning(self, "خطأ", f"لم يتم العثور على بيانات المريض: {patient_id}")
            return
        self.entries['patient_id'].setText(patient.get('patient_id', ''))
        self.entries['name'].setText(patient.get('name', ''))
        self.entries['national_id'].setText(patient.get('national_id', ''))
        
        # ملاحظة: entry_type يجب أن يتم تحديده بواسطة المستخدم في الواجهة، وليس من بيانات المريض
        self.entries['phone'].setText(patient.get('phone', ''))
        self.entries['address'].setText(patient.get('address', ''))
        
        # التبديل إلى تبويب الإدخال
        tabs = self.centralWidget().findChild(QTabWidget)
        if tabs:
            tabs.setCurrentIndex(0)  # تبويب الإدخال
        
        QMessageBox.information(self, "تعديل", "يمكنك تعديل البيانات ثم الضغط على حفظ")
    
    def delete_patient_from_table(self, patient_id):
        """حذف مريض من الجدول"""
        reply = QMessageBox.question(self, "تأكيد الحذف", 
            f"هل أنت متأكد من حذف هذا المريض؟\nالرقم: {patient_id}",
            QMessageBox.Yes | QMessageBox.No)
        
        if reply == QMessageBox.Yes:
            user_id = self.user_data.get('username', 'system')
            
            # Define callback for async UI refresh
            def on_deleted(success, error=None):
                if success:
                    # Use invokeMethod for thread-safe UI update
                    QMetaObject.invokeMethod(self, "_handle_delete_completion", Qt.QueuedConnection)
                else:
                    msg = str(error) if error else "حدث خطأ غير متوقع"
                    QMetaObject.invokeMethod(self, "_handle_delete_error", Qt.QueuedConnection, Q_ARG(str, msg))

            self.db.delete_patient(patient_id, user_id=user_id, user_info=self.user_data, callback=on_deleted)
    
    def show_patient_history(self, patient_id=None):
        """فتح ملف المريض (الملف الطبي)"""
        if patient_id is None:
            btn = self.sender()
            if not btn: return
            patient_id = btn.property("patient_id")
        
        dialog = PatientProfilePage(self.db, patient_id, self.hospitals, self)
        dialog.exec()
                
    def previous_page(self):
        """الانتقال للصفحة السابقة"""
        if self.current_page > 0:
            self.current_page -= 1
            self.refresh_patients_list()
    
    def next_page(self):
        """الانتقال للصفحة التالية"""
        if self.next_btn.isEnabled():
            self.current_page += 1
            self.refresh_patients_list()
    
    def refresh_patients_list(self, force_fresh=False):
        """تحديث قائمة المرضى بشكل غير متزامن - سريع بدون تعليق
        
        Args:
            force_fresh: إذا True، يتم مسح الكاش وجلب البيانات من جديد
                         ويقرأ من SQLite مباشرةً (حيث تذهب الكتابات أولاً)
                         بدلاً من PostgreSQL (الذي قد لم يُزامن بعد)
        """
        try:
            # مسح الكاش إذا كان force_fresh
            if force_fresh:
                self.db.cache.invalidate()
                self.db._page_cache.clear()
            
            offset = self.current_page * self.page_size
            # force_fresh=True → force_sqlite=True لضمان رؤية آخر تعديل فورياً
            patients = self.db.get_all_patients(limit=self.page_size, offset=offset, force_sqlite=force_fresh)
            total_patients = self.db.get_total_patients()
            
            # تحديث الواجهة مباشرة دون خيط
            max_page = (total_patients - 1) // self.page_size if total_patients > 0 else 0
            self.display_patients(patients)
            self.page_label.setText(f"الصفحة {self.current_page + 1}")
            self.prev_btn.setEnabled(self.current_page > 0)
            self.next_btn.setEnabled(self.current_page < max_page)
        except Exception as e:
            error_logger.error(f"Error refreshing patients list: {e}")
            self.statusBar().showMessage(f"❌ خطأ: {e}", 5000)

    @Slot(str, str)
    def _handle_save_completion(self, patient_id, warning_text):
        """تم اكتمال حفظ المريض بنجاح"""
        # مسح الكاش وتحديث القائمة فوراً
        self.current_page = 0
        self.refresh_patients_list(force_fresh=True)
        self.clear_entries()

        msg = f"✅ تم حفظ المريض بنجاح\n🔢 الرقم الموحد: {patient_id}"
        if warning_text:
            msg += "\n\n" + warning_text
        
        QMessageBox.information(self, "نجاح", msg)

    @Slot(str)
    def _handle_save_error(self, error_msg):
        """حدث خطأ أثناء الحفظ"""
        QMessageBox.warning(self, "خطأ", error_msg)

    @Slot()
    def _handle_delete_completion(self):
        """تم اكتمال حذف المريض بنجاح"""
        self.current_page = 0
        self.refresh_patients_list(force_fresh=True)
        QMessageBox.information(self, "نجاح", "تم حذف المريض بنجاح")

    @Slot(str)
    def _handle_delete_error(self, error_msg):
        """حدث خطأ أثناء الحذف"""
        QMessageBox.warning(self, "فشل الحذف", f"لا يمكن حذف هذا السجل:\n{error_msg}")

    @Slot(list, int)
    def on_data_loaded(self, patients, total_patients):
        """يتم استدعاؤها عند انتهاء تحميل البيانات من الخيط الخلفي"""
        max_page = (total_patients - 1) // self.page_size if total_patients > 0 else 0
        
        self.display_patients(patients)
        self.page_label.setText(f"الصفحة {self.current_page + 1}")
        
        # تحديث أزرار التنقل
        self.prev_btn.setEnabled(self.current_page > 0)
        self.next_btn.setEnabled(self.current_page < max_page)
    
    def display_patients(self, patients):
        """عرض المرضى في الجدول بتنسيق HIS احترافي مع أعلى أداء"""
        self.table.setSortingEnabled(False)
        self.table.setUpdatesEnabled(False)
        self.table.blockSignals(True)
        self.table.setRowCount(len(patients))

        bold_font = QFont("Arial", 11, QFont.Bold)
        normal_font = QFont("Arial", 11)

        def make_item(value, font=None, color=None, center=False):
            text = str(value) if value is not None else ''
            if len(text) > 10 and text[4] == '-' and text[7] == '-':
                text = text[:10]
            item = QTableWidgetItem(text)
            item.setTextAlignment(Qt.AlignCenter if center else (Qt.AlignRight | Qt.AlignVCenter))
            item.setToolTip(str(value))
            if font: item.setFont(font)
            if color: item.setForeground(QColor(color))
            return item

        for row, patient in enumerate(patients):
            self.table.setItem(row, 0, make_item(patient.get('patient_id', ''), bold_font, '#2563EB', center=True))
            self.table.setItem(row, 1, make_item(patient.get('name', ''), bold_font, '#0F172A'))
            self.table.setItem(row, 2, make_item(patient.get('national_id', ''), normal_font, '#475569', center=True))
            self.table.setItem(row, 3, make_item(patient.get('birth_date', ''), normal_font, '#64748B', center=True))

            # ملاحظة: entry_type موجود فقط في جدول visits وليس في patients
            self.table.setItem(row, 4, make_item('-', normal_font, '#94A3B8', center=True))

            self.table.setItem(row, 5, make_item(patient.get('phone', ''), normal_font, '#475569', center=True))
            self.table.setItem(row, 6, make_item(patient.get('address', ''), normal_font, '#475569'))
            self.table.setItem(row, 7, make_item(patient.get('hospital', ''), normal_font, '#475569', center=True))
            self.table.setItem(row, 8, make_item(patient.get('registration_date', ''), normal_font, '#94A3B8', center=True))

        self.table.blockSignals(False)
        self.table.setUpdatesEnabled(True)
        self.table.setSortingEnabled(True)
    
    def filter_patients(self):
        # تصفية المرضى بشكل غير متزامن
        search_term = self.search_entry.text().strip()
        
        if not search_term:
            self.refresh_patients_list()
            return

        # إيقاف أي loader سابق إذا كان لا يزال يعمل
        if hasattr(self, 'loader') and self.loader is not None:
            if self.loader.isRunning():
                self.loader.wait(1000)  # انتظر ثانية واحدة كحد أقصى
            self.loader.deleteLater()
        
        # استخدام DataLoader للبحث في الخلفية
        self.loader = DataLoader(self.db.search_patients, search_term)
        self.loader.finished.connect(lambda patients, total: self.display_patients(patients))
        self.loader.error.connect(lambda e: self.statusBar().showMessage(f"❌ خطأ في البحث: {e}", 5000))
        self.loader.start()
    

    
    def clear_entries(self):
        # مسح حقول الإدخال بشكل شامل
        if hasattr(self, 'entries'):
            for key, entry in self.entries.items():
                if isinstance(entry, QLineEdit):
                    entry.clear()
                elif isinstance(entry, QComboBox):
                    entry.setCurrentIndex(0)
            
            if 'patient_id' in self.entries:
                self.entries['patient_id'].setText("سيظهر هنا...")
            
            if hasattr(self, 'validation_label'):
                self.validation_label.setText("")
                self.validation_label.setStyleSheet("color: #EF4444; font-size: 13px; font-weight: bold; background: transparent;")
    



# ==========================================
# Background Threads (Sync & Backup)
# ==========================================

class BackupThread(threading.Thread):
    def __init__(self, db_manager):
        super().__init__(daemon=True)
        self.db = db_manager

    def run(self):
        system_logger.info("Backup Thread Started")
        while system_state.running:  # FIX #3: Use system_state.running flag for graceful shutdown
            try:
                self.db.backup_data()
            except Exception as e:
                error_logger.error(f"Backup Thread Error: {e}")
            # التشغيل كل 24 ساعة
            time.sleep(24 * 3600)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # تعيين الخط العربي بشكل صحيح
    font = QFont("Arial", 11)
    app.setFont(font)
    
    # FIX: Show settings dialog if config missing
    CONFIG_PATH = os.path.join(BASE_DIR, 'config.json')
    if not os.path.exists(CONFIG_PATH):
        system_logger.info("Config file missing, showing SettingsDialog")
        settings = SettingsDialog()
        if settings.exec() == QDialog.Rejected:
            sys.exit(1)
    
    # إنشاء وتشغيل التطبيق
    window = PatientManagementSystem()
    
    # FIX #1: Start background workers with proper tracking
    start_background_threads()
    
    
    backup_thread = BackupThread(window.db)
    backup_thread.start()
    system_state.active_threads.append(backup_thread)  # FIX #1: Track thread
    
    # FIX #5: Add graceful shutdown handler
    import atexit
    def graceful_shutdown():
        """إغلاق جميع الموارد بشكل آمن عند الخروج"""
        shutdown_system()  # FIX #3: Use centralized shutdown function
    
    atexit.register(graceful_shutdown)
    
    sys.exit(app.exec())
