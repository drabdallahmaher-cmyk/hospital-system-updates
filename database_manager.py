"""
Thread-Safe SQLite Connection Manager

CRITICAL: Each thread MUST use its own connection to prevent database locked errors.
This module provides a per-thread connection pool with automatic cleanup.
"""

import sqlite3
import threading
import logging
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Optional, Any, List, Dict

logger = logging.getLogger("system")


def is_write_query(sql: str) -> bool:
    """Detect if a SQL query is a write operation."""
    if not sql or not sql.strip():
        return False
    return sql.strip().split()[0].upper() in {
        "INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "DROP", "ALTER"
    }


class SafeCursor:
    """
    Wrapper for sqlite3.Cursor that enforces write protection and audit logging.
    """
    def __init__(self, cursor: sqlite3.Cursor, connection: 'SafeConnection', from_write_queue: bool = False):
        self._cursor = cursor
        self._connection = connection
        self._from_write_queue = from_write_queue

    def execute(self, sql: str, parameters: tuple = ()):
        # 1. Detect write operations
        is_write = is_write_query(sql)
        
        if is_write and not self._from_write_queue:
            # NO implicit bypass allowed. Direct writes MUST raise.
            sql_log = sql[:50]
            raise PermissionError(f"Direct write prohibited - must use WriteQueueManager: {sql_log}...")

        # 2. Audit Logging (only for actual writes from the queue)
        if is_write and self._from_write_queue:
            self._connection.log_audit(sql, parameters)

        return self._cursor.execute(sql, parameters)

    def fetchone(self): return self._cursor.fetchone()
    def fetchall(self): return self._cursor.fetchall()
    def close(self): self._cursor.close()
    
    @property
    def lastrowid(self): return self._cursor.lastrowid
    @property
    def rowcount(self): return self._cursor.rowcount

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class SafeConnection:
    """
    Wrapper for sqlite3.Connection that prevents unauthorized commits.
    """
    def __init__(self, conn: sqlite3.Connection, sqlite_manager: 'ThreadSafeSQLiteManager', authorized: bool = False):
        self._conn = conn
        self._manager = sqlite_manager
        self._authorized = authorized

    def authorize(self):
        """Explicitly authorize commits for the next operation."""
        self._authorized = True

    def cursor(self, from_write_queue: bool = False) -> SafeCursor:
        # If the connection itself is authorized, treat all its cursors as authorized
        is_authorized = from_write_queue or self._authorized
        return SafeCursor(self._conn.cursor(), self, is_authorized)

    def commit(self):
        if not self._authorized:
            raise PermissionError("Commit only allowed via WriteQueueManager")
        # Reset authorization after commit
        self._authorized = False
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        return self._conn.close()

    def log_audit(self, sql: str, parameters: tuple):
        """Log write operations to a structured audit table."""
        try:
            # Basic parsing for better visibility
            parts = sql.strip().split()
            operation = parts[0].upper() if parts else "UNKNOWN"
            
            # Simple table name extraction (usually second word for INSERT INTO, UPDATE, DELETE FROM)
            table_name = "UNKNOWN"
            if len(parts) > 1:
                if operation in ["UPDATE", "DELETE"]:
                    table_name = parts[1].upper() if operation == "UPDATE" else (parts[2].upper() if len(parts) > 2 else "UNKNOWN")
                elif operation == "INSERT":
                    table_name = parts[2].upper() if len(parts) > 2 else "UNKNOWN"
            
            audit_sql = """
                INSERT INTO audit_log (timestamp, operation, table_name, query, params, source) 
                VALUES (?, ?, ?, ?, ?, ?)
            """
            self._conn.execute(audit_sql, (
                datetime.now().isoformat(),
                operation,
                table_name,
                sql,
                str(parameters),
                "write_queue"
            ))
        except Exception as e:
            logger.error(f"Audit logging failed: {e}")

    def __getattr__(self, name):
        return getattr(self._conn, name)



class ThreadSafeSQLiteManager:
    """
    Thread-safe SQLite connection manager using thread-local storage.
    
    CRITICAL RULES:
    1. Each thread gets its own dedicated connection
    2. Connections are NEVER shared across threads
    3. All writes MUST go through the write queue (not direct calls)
    4. Use context managers for automatic resource cleanup
    """
    
    def __init__(self, db_path: str, max_connections_per_thread: int = 5):
        """
        Initialize thread-safe SQLite manager.
        """
        self.db_path = db_path
        self.max_connections_per_thread = max_connections_per_thread
        self.write_queue: Any = None # For UI thread redirects
        
        # Thread-local storage for connections
        self._local = threading.local()
        
        # Global lock for initialization and cleanup
        self._global_lock = threading.RLock()
        
        # Track all connections for cleanup
        self._all_connections = set()
        
        # Initialize database schema
        self._initialize_database()
        
        logger.info(f"ThreadSafeSQLiteManager initialized: {db_path}")
    
    def _initialize_database(self):
        """Initialize database with optimal settings and schema."""
        conn = self._create_connection() # This returns a raw connection for init
        try:
            # Enable WAL mode
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")
            
            # Create Audit Log table if not exists with improved structure
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    operation TEXT,
                    table_name TEXT,
                    query TEXT,
                    params TEXT,
                    source TEXT
                )
            """)
            
            # Create Dead Letter table if not exists
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dead_letter_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    operation TEXT,
                    table_name TEXT,
                    payload TEXT,
                    error TEXT,
                    retries INTEGER
                )
            """)
            
            conn.commit()
            logger.info("Database initialized with optimized settings and auditing tables")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
        finally:
            # For internal use we return raw connection
            conns = getattr(self._local, 'connections', None)
            if conns is not None:
                conns.append(conn)
            else:
                conn.close()
    
    def _create_connection(self) -> sqlite3.Connection:
        """Create a new SQLite connection with optimized settings."""
        conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,  # We manage thread safety ourselves
            timeout=30.0
        )
        
        # Connection-level optimizations
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-64000")
        
        # Enable row factory for dict-like access
        conn.row_factory = sqlite3.Row
        
        with self._global_lock:
            self._all_connections.add(conn)
        
        return conn
    
    def _get_thread_connections(self) -> list:
        """Get or create connection list for current thread."""
        if not hasattr(self._local, 'connections'):
            setattr(self._local, 'connections', [])
        return getattr(self._local, 'connections')
    
    def get_connection(self, heavy: bool = False, authorized: bool = False) -> SafeConnection:
        """
        Get a safe connection wrapper for the current thread.
        """
        # Ensure current thread is NOT the UI thread for blocking operations
        self._check_ui_thread(heavy=heavy)

        thread_conns = self._get_thread_connections()
        
        if thread_conns:
            conn = thread_conns.pop()
        else:
            conn = self._create_connection()
        
        return SafeConnection(conn, self, authorized=authorized)

    def _check_ui_thread(self, heavy: bool = False):
        """Guard against blocking database operations on the UI thread."""
        try:
            from PySide6.QtWidgets import QApplication
            from PySide6.QtCore import QThread
            if QApplication.instance() and QThread.currentThread() == QApplication.instance().thread():
                if heavy:
                    raise RuntimeError("Blocking HEAVY DB operation on UI thread is not allowed")
                # Removed the continuous warning for normal operations to reduce log noise
                # as most reads currently happen on the UI thread.
        except ImportError:
            pass
    
    def _return_connection_internal(self, conn: Any):
        """Internal method to return connection without thread safety checks."""
        # Prevent "Matryoshka doll" nested wrappers by unwrapping SafeConnection
        if hasattr(conn, '_conn') and isinstance(conn, SafeConnection):
            raw_conn = conn._conn
        else:
            raw_conn = conn

        # Rollback any uncommitted transactions
        try:
            raw_conn.rollback()
        except Exception:
            pass  # Ignore rollback errors
        
        thread_conns = self._get_thread_connections()
        
        if len(thread_conns) < self.max_connections_per_thread:
            thread_conns.append(raw_conn)
        else:
            # Close connection if thread has too many cached
            try:
                raw_conn.close()
            except Exception:
                pass
            with self._global_lock:
                self._all_connections.discard(raw_conn)
    
    def return_connection(self, conn: sqlite3.Connection):
        """
        Return a connection to the thread's pool.
        
        Args:
            conn: The connection to return
            
        CRITICAL: Always call this after finishing with a connection.
        """
        if conn is None:
            return
        
        self._return_connection_internal(conn)
    
    @contextmanager
    def cursor(self, commit: bool = False, from_write_queue: bool = False, heavy: bool = False):
        """
        Context manager for safe cursor usage with automatic cleanup.
        """
        conn = self.get_connection(heavy=heavy)
        cursor = conn.cursor(from_write_queue=from_write_queue)
        try:
            yield cursor, conn
            if commit:
                conn.commit()
        except Exception as e:
            conn.rollback()
            raise
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            self.return_connection(conn)
    
    def execute_read(self, query: str, params: tuple = ()) -> list:
        """
        Execute a read-only query and return results.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of result rows as dictionaries
        """
        with self.cursor() as (cursor, conn):
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    
    def execute_write(self, query: str, params: tuple = ()) -> Optional[int]:
        """
        Execute a write query (INSERT/UPDATE/DELETE).
        
        CRITICAL: For production, all writes should go through WriteQueueManager,
        NOT directly via this method. This is only for initialization/schema ops.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Last row ID for INSERTs, None otherwise
        """
        with self.cursor(commit=True) as (cursor, conn):
            cursor.execute(query, params)
            return cursor.lastrowid
    
    def close_all_connections(self):
        """
        Close all connections (for shutdown).
        
        CRITICAL: Call this only during graceful shutdown.
        """
        with self._global_lock:
            for conn in self._all_connections:
                try:
                    conn.close()
                except Exception:
                    pass
            self._all_connections.clear()
            
            # Clear thread-local connections
            conns = getattr(self._local, 'connections', None)
            if conns is not None:
                conns.clear()
        
        logger.info("All SQLite connections closed")


# Global instance - will be initialized in MAIN.PY
sqlite_manager: Optional[ThreadSafeSQLiteManager] = None


def initialize_sqlite_manager(db_path: str) -> ThreadSafeSQLiteManager:
    """Initialize the global SQLite manager instance."""
    global sqlite_manager
    sqlite_manager = ThreadSafeSQLiteManager(db_path)
    return sqlite_manager


def get_thread_connection(authorized: bool = False) -> sqlite3.Connection:
    """
    Get the current thread's SQLite connection.
    
    CRITICAL: This is the ONLY way to get a connection outside of the manager.
    Each thread MUST use this to get its own connection.
    
    Returns:
        sqlite3.Connection: Thread-local connection
    """
    if sqlite_manager is None:
        raise RuntimeError("SQLite manager not initialized")
    return sqlite_manager.get_connection(authorized=authorized)


def return_thread_connection(conn: sqlite3.Connection):
    """
    Return the thread's connection back to the pool.
    
    CRITICAL: Must be called after finishing with a connection.
    """
    if sqlite_manager is not None:
        sqlite_manager.return_connection(conn)
