import os
import time
import uuid
import asyncio
import logging
import sqlite3
import threading
import c_two as cc
from typing import Literal

from ..config import settings
from ..schemas.lock import LockInfo

logger = logging.getLogger(__name__)

class RWLock:
    def __init__(
        self,
        node_key: str,
        access_mode: Literal['lr', 'lw', 'pr', 'pw'],
        timeout: float | None = None,
        retry_interval: float = 1.0
    ):
        if access_mode not in ['lr', 'lw', 'pr', 'pw']:
            raise ValueError("access mode must be either 'lr' for local read, 'lw' for local write, 'pr' for process-level read, or 'pw' for process-level write")

        self.node_key = node_key
        self.access_mode = access_mode
        self.retry_interval = retry_interval
        self.timeout = timeout if (timeout is not None and timeout >= 0) else None
        self.id = f'pid_{os.getpid()}_tid_{threading.get_ident()}_{uuid.uuid4().hex}'
        
    def _get_connection(self):
        """Creates a new database connection."""
        return sqlite3.connect(settings.SQLITE_PATH)
    
    @property
    def access_level(self) -> str:
        return self.access_mode[0]
    
    @property
    def lock_type(self) -> str:
        return self.access_mode[1]
    
    @staticmethod
    def init():
        with sqlite3.connect(settings.SQLITE_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS locks (
                    node_key TEXT NOT NULL,
                    lock_type TEXT NOT NULL,
                    lock_id TEXT PRIMARY KEY,
                    access_level TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
        
        # Clear all existing locks to avoid stale locks
        RWLock.clear_all()
        
    @staticmethod
    def get_lock_type(lock_id: str) -> str | None:
        """Get the lock type ('r' or 'w') for a given lock ID."""
        with sqlite3.connect(settings.SQLITE_PATH) as conn:
            cursor = conn.execute('SELECT lock_type FROM locks WHERE lock_id = ?', (lock_id,))
            row = cursor.fetchone()
            return row[0] if row else None

    @staticmethod
    def is_node_locked(node_key: str) -> bool:
        """Check if a node is currently active."""
        with sqlite3.connect(settings.SQLITE_PATH) as conn:
            cursor = conn.execute('SELECT 1 FROM locks WHERE node_key = ?', (node_key,))
            return cursor.fetchone() is not None
    
    @staticmethod
    def lock_node(node_key: str, lock_type: Literal['r', 'w'], access_level: Literal['l', 'p'], timeout: float | None = None, retry_interval: float = 1.0) -> 'RWLock':
        """Acquire a lock for a node."""
        lock = RWLock(node_key, access_level + lock_type, timeout, retry_interval)
        lock.acquire()
        return lock
    
    @staticmethod
    def unlock_nodes(node_keys: list[str]) -> None:
        """Release the locks for a list of nodes."""
        with sqlite3.connect(settings.SQLITE_PATH) as conn:
            conn.execute('DELETE FROM locks WHERE node_key IN ({})'.format(', '.join('?' for _ in node_keys)), node_keys)
            conn.commit()

    @staticmethod
    def has_lock(lock_id: str) -> bool:
        """Check if a lock with the given ID exists."""
        with sqlite3.connect(settings.SQLITE_PATH) as conn:
            cursor = conn.execute('SELECT 1 FROM locks WHERE lock_id = ?', (lock_id,))
            return cursor.fetchone() is not None
    
    @staticmethod
    def remove_lock(lock_id: str) -> None:
        """Remove a lock with the given ID."""
        with sqlite3.connect(settings.SQLITE_PATH) as conn:
            conn.execute('DELETE FROM locks WHERE lock_id = ?', (lock_id,))
            conn.commit()
    
    @staticmethod
    def release_all_process_servers() -> None:
        """
        Release all process-level locks by shutting down their associated CRM servers.
        
        This function queries the database for all existing locks and specifically handles
        process-level locks (access_level == 'p') by attempting to shutdown their 
        corresponding CRM servers. Local-level locks are ignored in this operation.
        """
        with sqlite3.connect(settings.SQLITE_PATH) as conn:
            cursor = conn.execute('SELECT lock_id, node_key, access_level FROM locks')
            for lock_id, node_key, access_level in cursor.fetchall():
                # For process-level locks, shutdown the CRM server
                if access_level == 'p':
                    server_address = f'memory://{node_key.replace(".", "_")}_{lock_id}'
                    try:
                        if not cc.rpc.Client.shutdown(server_address, -1.0):
                            raise RuntimeError(f'Failed to shutdown CRM server for node {node_key}')
                    except Exception as e:
                        logger.error(f'{e}')
            conn.commit()
    
    @staticmethod
    def clear_all() -> None:
        """Remove all locks from the database."""
        # If no table, do nothing
        if not settings.SQLITE_PATH.exists():
            return

        with sqlite3.connect(settings.SQLITE_PATH) as conn:
            conn.execute('DELETE FROM locks')
            conn.commit()
    
    @staticmethod
    def get_lock_info(lock_id: str) -> LockInfo | None:
        """Get detailed information about a lock."""
        with sqlite3.connect(settings.SQLITE_PATH) as conn:
            cursor = conn.execute('SELECT node_key, lock_type, access_level FROM locks WHERE lock_id = ?', (lock_id,))
            row = cursor.fetchone()
            if row:
                return LockInfo(lock_id=lock_id, node_key=row[0], lock_type=row[1], access_mode=row[2])
            return None

    def acquired(self) -> bool:
        """Checks if the lock is currently acquired."""
        with sqlite3.connect(settings.SQLITE_PATH) as conn:
            cursor = conn.execute('SELECT 1 FROM locks WHERE lock_id = ?', (self.id,))
            return cursor.fetchone() is not None

    def acquire(self) -> None:
        """
        Acquires the lock, blocking until it's available or timeout occurs.
        """
        if self.acquired():
            return  # already acquired
        
        start_time = time.monotonic()
        while (self.timeout is None) or (time.monotonic() - start_time < self.timeout):
            conn = self._get_connection()
            # Use IMMEDIATE transaction to acquire a reserved lock on the database file,
            # Preventing other connections from writing to the database.
            try:
                conn.execute('BEGIN IMMEDIATE')
                cursor = conn.cursor()
                
                can_acquire = False
                if self.lock_type == 'w':
                    # For a write lock, no other locks should exist for this resource
                    cursor.execute('SELECT COUNT(*) FROM locks WHERE node_key = ?', (self.node_key,))
                    if cursor.fetchone()[0] == 0:
                        can_acquire = True
                else: # 'r'
                    # For a read lock, no write locks should exist for this resource
                    cursor.execute("SELECT COUNT(*) FROM locks WHERE node_key = ? AND lock_type = 'w'", (self.node_key,))
                    if cursor.fetchone()[0] == 0:
                        can_acquire = True
                
                if can_acquire:
                    cursor.execute(
                        'INSERT INTO locks (node_key, lock_type, lock_id, access_level) VALUES (?, ?, ?, ?)',
                        (self.node_key, self.lock_type, self.id, self.access_level)
                    )
                    conn.commit()
                    return
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
                conn.execute('DELETE FROM locks WHERE lock_id = ?', (self.id,))
                conn.commit()
            except Exception as e:
                # Log this error, as failure to release a lock can be critical
                logger.error(f'Error releasing lock {self.id}: {e}')

    async def async_acquire(self) -> None:
        """
        Acquires the lock asynchronously, blocking until it's available or timeout occurs.
        """
        if self.acquired():
            return  # already acquired
        
        start_time = time.monotonic()
        while (self.timeout is None) or (time.monotonic() - start_time < self.timeout):
            conn = self._get_connection()
            # Use IMMEDIATE transaction to acquire a reserved lock on the database file,
            # Preventing other connections from writing to the database.
            try:
                conn.execute('BEGIN IMMEDIATE')
                cursor = conn.cursor()

                can_acquire = False
                if self.lock_type == 'w':
                    # For a write lock, no other locks should exist for this resource
                    cursor.execute('SELECT COUNT(*) FROM locks WHERE node_key = ?', (self.node_key,))
                    if cursor.fetchone()[0] == 0:
                        can_acquire = True
                else:  # 'r'
                    # For a read lock, no write locks should exist for this resource
                    cursor.execute("SELECT COUNT(*) FROM locks WHERE node_key = ? AND lock_type = 'w'", (self.node_key,))
                    if cursor.fetchone()[0] == 0:
                        can_acquire = True

                if can_acquire:
                    cursor.execute(
                        'INSERT INTO locks (node_key, lock_type, lock_id, access_level) VALUES (?, ?, ?, ?)',
                        (self.node_key, self.lock_type, self.id, self.access_level)
                    )
                    conn.commit()
                    return
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
            await asyncio.sleep(self.retry_interval)

        raise TimeoutError(f"Failed to acquire {self.lock_type} lock for resource '{self.resource_name}' within {self.timeout} seconds.")