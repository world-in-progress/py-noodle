import os
import time
import uuid
import logging
import sqlite3
import threading
from typing import Literal

logger = logging.getLogger(__name__)

class RWLock:
    def __init__(
        self,
        db_path: str,
        node_key: str,
        lock_type: Literal['r', 'w'],
        timeout: float | None = None,
        retry_interval: float = 1.0
    ):
        if lock_type not in ['r', 'w']:
            raise ValueError("lock_type must be either 'r' for read or 'w' for write")
        
        self.db_path = db_path
        self.node_key = node_key
        self.lock_type = lock_type
        self.retry_interval = retry_interval
        self.timeout = timeout if (timeout is not None and timeout >= 0) else None

        # Generate a unique ID for this special lock instance
        self.lock_id = f'pid_{os.getpid()}-tid_{threading.get_ident()}-{uuid.uuid4().hex}'
        
        self._conn = None
        self._init_db()
        
    def _get_connection(self):
        """Creates a new database connection."""
        return sqlite3.connect(self.db_path)
    
    def _init_db(self):
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS locks (
                    node_key TEXT NOT NULL,
                    lock_type TEXT NOT NULL,
                    lock_id TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
    
    @staticmethod
    def is_node_active(db_path: str, node_key: str) -> bool:
        """Check if a node is currently active."""
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT 1 FROM locks WHERE node_key = ?", (node_key,))
            return cursor.fetchone() is not None
    
    @staticmethod
    def clear_all(db_path: str) -> None:
        """Remove all locks from the database."""
        # If no table, do nothing
        if not os.path.exists(db_path):
            return
        
        with sqlite3.connect(db_path) as conn:
            conn.execute("DELETE FROM locks")
            conn.commit()

    def acquire(self) -> bool:
        """
        Acquires the lock, blocking until it's available or timeout occurs.
        """
        start_time = time.monotonic()
        while (self.timeout is None) or (time.monotonic() - start_time < self.timeout):
            conn = self._get_connection()
            # Use IMMEDIATE transaction to acquire a reserved lock on the database file,
            # preventing other connections from writing to the database.
            try:
                conn.execute('BEGIN IMMEDIATE')
                cursor = conn.cursor()
                
                can_acquire = False
                if self.lock_type == 'w':
                    # For a write lock, no other locks should exist for this resource.
                    cursor.execute('SELECT COUNT(*) FROM locks WHERE node_key = ?', (self.node_key,))
                    if cursor.fetchone()[0] == 0:
                        can_acquire = True
                else: # 'r'
                    # For a read lock, no write locks should exist for this resource.
                    cursor.execute("SELECT COUNT(*) FROM locks WHERE node_key = ? AND lock_type = 'w'", (self.node_key,))
                    if cursor.fetchone()[0] == 0:
                        can_acquire = True
                
                if can_acquire:
                    cursor.execute(
                        'INSERT INTO locks (node_key, lock_type, lock_id) VALUES (?, ?, ?)',
                        (self.node_key, self.lock_type, self.lock_id)
                    )
                    conn.commit()
                    return True
                else:
                    # Could not acquire, rollback and wait.
                    conn.rollback()

            except sqlite3.OperationalError as e:
                # This can happen if another process has an EXCLUSIVE lock (e.g., another BEGIN IMMEDIATE)
                # This is part of the contention mechanism, just rollback and retry.
                conn.rollback()
            finally:
                conn.close()

            # Wait before retrying
            time.sleep(self.retry_interval)
            
        raise TimeoutError(f"Failed to acquire {self.lock_type} lock for resource '{self.resource_name}' within {self.timeout} seconds.")

    def release(self) -> None:
        """Releases the lock."""
        with self._get_connection() as conn:
            try:
                conn.execute('DELETE FROM locks WHERE lock_id = ?', (self.lock_id,))
                conn.commit()
            except Exception as e:
                # Log this error, as failure to release a lock can be critical
                logger.error(f'Error releasing lock {self.lock_id}: {e}')
                
    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()