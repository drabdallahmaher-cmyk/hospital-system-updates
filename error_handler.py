"""
Unified Database Error Handler

All database errors MUST go through this handler for:
1. Consistent error handling across the application
2. Automatic retry logic with exponential backoff
3. Proper connection cleanup
4. System mode-aware error recovery
"""

import time
import logging
import sqlite3
from typing import Callable, Any, Optional
from functools import wraps

logger = logging.getLogger("error")


class DatabaseErrorHandler:
    """
    Centralized error handler for all database operations.
    
    Features:
    - Exponential backoff retry logic
    - Connection cleanup
    - Mode-aware error handling (ONLINE/OFFLINE/CRITICAL)
    - Structured error logging
    """
    
    # Retry configuration
    MAX_RETRIES_SQLITE = 5
    MAX_RETRIES_POSTGRES = 3
    BASE_DELAY = 1.0  # seconds
    MAX_DELAY = 30.0  # seconds
    
    @classmethod
    def handle_sqlite_errors(cls, func: Callable) -> Callable:
        """
        Decorator for handling SQLite errors with automatic retry.
        
        Usage:
            @DatabaseErrorHandler.handle_sqlite_errors
            def my_database_function(...):
                ...
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            
            for attempt in range(cls.MAX_RETRIES_SQLITE):
                try:
                    return func(*args, **kwargs)
                    
                except sqlite3.OperationalError as e:
                    error_msg = str(e).lower()
                    
                    # Handle database locked errors
                    if 'database is locked' in error_msg or 'locked' in error_msg:
                        delay = min(cls.BASE_DELAY * (2 ** attempt), cls.MAX_DELAY)
                        logger.warning(
                            f"SQLite locked (attempt {attempt + 1}/{cls.MAX_RETRIES_SQLITE}): {e}. "
                            f"Retrying in {delay:.1f}s"
                        )
                        time.sleep(delay)
                        
                        if attempt == cls.MAX_RETRIES_SQLITE - 1:
                            logger.error(f"SQLite locked failed after {cls.MAX_RETRIES_SQLITE} attempts: {e}")
                            raise
                    
                    # Handle other operational errors
                    elif 'no such table' in error_msg:
                        logger.error(f"Missing table: {e}")
                        raise
                    
                    else:
                        logger.error(f"SQLite operational error: {e}")
                        raise
                
                except sqlite3.IntegrityError as e:
                    logger.error(f"Integrity constraint violation: {e}")
                    raise
                
                except sqlite3.DatabaseError as e:
                    logger.error(f"Database error: {e}")
                    last_error = e
                    if attempt < cls.MAX_RETRIES_SQLITE - 1:
                        delay = min(cls.BASE_DELAY * (2 ** attempt), cls.MAX_DELAY)
                        time.sleep(delay)
                    else:
                        raise
                
                except Exception as e:
                    logger.error(f"Unexpected error in SQLite operation: {e}")
                    raise
            
            if last_error:
                raise last_error
        
        return wrapper
    
    @classmethod
    def handle_postgres_errors(cls, func: Callable) -> Callable:
        """
        Decorator for handling PostgreSQL errors with automatic retry.
        
        Usage:
            @DatabaseErrorHandler.handle_postgres_errors
            def my_postgres_function(...):
                ...
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            
            for attempt in range(cls.MAX_RETRIES_POSTGRES):
                try:
                    return func(*args, **kwargs)
                    
                except Exception as e:
                    error_msg = str(e).lower()
                    
                    # Connection errors - retry with backoff
                    if any(x in error_msg for x in ['connection', 'timeout', 'pool']):
                        delay = min(cls.BASE_DELAY * (2 ** attempt), cls.MAX_DELAY)
                        logger.warning(
                            f"PostgreSQL connection error (attempt {attempt + 1}/{cls.MAX_RETRIES_POSTGRES}): {e}. "
                            f"Retrying in {delay:.1f}s"
                        )
                        time.sleep(delay)
                        
                        if attempt == cls.MAX_RETRIES_POSTGRES - 1:
                            logger.error(f"PostgreSQL connection failed after {cls.MAX_RETRIES_POSTGRES} attempts: {e}")
                            raise
                    
                    # Other errors - log and raise immediately
                    else:
                        logger.error(f"PostgreSQL error: {e}")
                        raise
            
            if last_error:
                raise last_error
        
        return wrapper
    
    @staticmethod
    def safe_execute_db_operation(operation_func: Callable, db_type: str = "sqlite", 
                                 *args, **kwargs) -> Any:
        """
        Execute a database operation with unified error handling.
        
        Args:
            operation_func: Function to execute
            db_type: "sqlite" or "postgres"
            *args: Arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Result of the operation
            
        Raises:
            Exception: If operation fails after retries
        """
        conn = None
        cursor = None
        
        try:
            # Get connection based on type
            if 'conn' in kwargs:
                conn = kwargs.pop('conn')
            else:
                # Import here to avoid circular imports
                if db_type == "sqlite":
                    from database_manager import get_thread_connection
                    conn = get_thread_connection()
                elif db_type == "postgres":
                    from MAIN import get_connection
                    conn = get_connection()
                else:
                    raise ValueError(f"Invalid db_type: {db_type}")
            
            if not conn:
                raise Exception(f"Failed to get {db_type} connection")
            
            # Execute operation
            cursor = conn.cursor()
            result = operation_func(cursor, conn, *args, **kwargs)
            
            # Commit for write operations
            if hasattr(operation_func, '__name__') and 'get_' not in operation_func.__name__:
                conn.commit()
            
            return result
            
        except Exception as e:
            # Rollback on error
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            
            logger.error(f"Database operation failed ({db_type}): {e}")
            raise
        
        finally:
            # Cleanup resources
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            
            # Return connection to pool
            if conn:
                try:
                    if db_type == "sqlite":
                        from database_manager import return_thread_connection
                        return_thread_connection(conn)
                    elif db_type == "postgres":
                        from MAIN import release_connection
                        release_connection(conn)
                except Exception:
                    pass
    
    @staticmethod
    def check_system_mode(system_mode: str) -> bool:
        """
        Check if operation is allowed based on system mode.
        
        Args:
            system_mode: Current system mode (ONLINE/OFFLINE/CRITICAL)
            
        Returns:
            True if operation is allowed, False otherwise
            
        Usage:
            if not DatabaseErrorHandler.check_system_mode(SYSTEM_MODE):
                logger.warning("Operation not allowed in current mode")
                return
        """
        # All operations allowed in ONLINE mode
        if system_mode == "ONLINE":
            return True
        
        # Only read operations allowed in OFFLINE mode
        elif system_mode == "OFFLINE":
            return True  # Will be checked at call site
        
        # Critical mode - minimal operations only
        elif system_mode == "CRITICAL":
            return False
        
        return False
    
    @staticmethod
    def log_error_context(context: str, error: Exception, details: Optional[dict] = None):
        """
        Log error with rich context information.
        
        Args:
            context: Where the error occurred (e.g., "add_patient", "sync_visits")
            error: Exception object
            details: Additional context dictionary
        """
        error_msg = {
            'context': context,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
            'thread': __import__('threading').current_thread().name
        }
        
        if details:
            error_msg.update(details)
        
        logger.error(f"ERROR CONTEXT: {error_msg}")


# Convenience decorator for critical sections that need extra protection
def critical_section(lock_object: threading.Lock):
    """
    Decorator for critical sections that need locking.
    
    Usage:
        @critical_section(sqlite_write_lock)
        def critical_operation(...):
            ...
    """
    import threading
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            with lock_object:
                return func(*args, **kwargs)
        return wrapper
    return decorator
