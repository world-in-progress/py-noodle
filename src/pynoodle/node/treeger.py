import json
import sqlite3
import logging
import requests
import c_two as cc
from contextlib import contextmanager
from typing import TypeVar, Literal, Type, Generator

from .lock import RWLock
from ..config import settings
from ..schemas.lock import LockInfo
from ..schemas.node import ResourceNodeInfo
from ..module_cache import ModuleCache, ResourceNodeTemplateModule
from .node import ResourceNodeRecord, IResourceNode, ResourceNode, RemoteResourceNode, RemoteResourceNodeProxy

T = TypeVar('T')
logger = logging.getLogger(__name__)

# DB-related constants
NODE_TABLE = 'node'
NODE_KEY = 'node_key'
PARENT_KEY = 'parent_key'
LAUNCH_PARAMS = 'launch_params'
TEMPLATE_NAME = 'template_name'
ACCESS_INFO = 'access_info' # access info = access address :: remote node key

class Treeger:
    def __init__(self):
        # Get scenario graph
        self.module_cache = ModuleCache()
    
    @staticmethod
    def init():
        # Create the database file if it doesn't exist
        with sqlite3.connect(settings.SQLITE_PATH) as conn:
            # Create the node table
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {NODE_TABLE} (
                    {PARENT_KEY} TEXT,
                    {TEMPLATE_NAME} TEXT,
                    {NODE_KEY} TEXT PRIMARY KEY,
                    {ACCESS_INFO} TEXT DEFAULT NULL,
                    {LAUNCH_PARAMS} TEXT DEFAULT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY ({PARENT_KEY}) REFERENCES {NODE_TABLE} ({NODE_KEY}) ON DELETE CASCADE
                )
            """)
            conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{PARENT_KEY} ON {NODE_TABLE}({PARENT_KEY})')
            conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{TEMPLATE_NAME} ON {NODE_TABLE}({TEMPLATE_NAME})')
            conn.commit()
    
    @contextmanager
    def _connect_db(self):
        """Context manager for database connection."""
        conn = sqlite3.connect(settings.SQLITE_PATH)
        conn.row_factory = sqlite3.Row  # enable column access by name
        conn.execute('PRAGMA foreign_keys = ON;') # enable foreign key support
        try:
            yield conn
        finally:
            conn.close()
    
    def _has_node(self, node_key: str) -> bool:
        """Check if a node exists in the database"""
        with self._connect_db() as conn:
            cursor = conn.execute(f'SELECT 1 FROM {NODE_TABLE} WHERE {NODE_KEY} = ?', (node_key,))
            return cursor.fetchone() is not None

    def _insert_node(
        self, node_key: str,
        parent_key: str | None, template_name: str, launch_params: str,
    ) -> None:
        """Insert a new node into the database"""
        with self._connect_db() as conn:
            # Add node to the node table
            conn.execute(f"""
                INSERT INTO {NODE_TABLE} ({NODE_KEY}, {TEMPLATE_NAME}, {PARENT_KEY}, {LAUNCH_PARAMS})
                VALUES (?, ?, ?, ?)
            """, (
                node_key,
                template_name,
                parent_key if parent_key else None,
                launch_params if launch_params else None
            ))
            conn.commit()

    def _delete_node(self, node_key: str) -> None:
        """Delete a node from the database"""
        with self._connect_db() as conn:
            # Delete node from the node table
            conn.execute(f'DELETE FROM {NODE_TABLE} WHERE {NODE_KEY} = ?', (node_key,))
            conn.commit()
    
    def _get_child_keys(self, parent_key: str) -> list[str]:
        """Get all child node keys for a given parent from databse"""
        with self._connect_db() as conn:
            cursor = conn.execute(f'SELECT {NODE_KEY} FROM {NODE_TABLE} WHERE {PARENT_KEY} = ?', (parent_key,))
            return [row[NODE_KEY] for row in cursor.fetchall()]
    
    def _load_node_record(self, node_key: str, is_cascade: bool) -> ResourceNodeRecord | None:
        """Load a single node from the database"""
        with self._connect_db() as conn:
            cursor = conn.execute(f"""
                SELECT {NODE_KEY}, {TEMPLATE_NAME}, {LAUNCH_PARAMS}, {PARENT_KEY}, {ACCESS_INFO}
                FROM {NODE_TABLE}
                WHERE {NODE_KEY} = ?
            """, (node_key,))
            row = cursor.fetchone()
            if row is None:
                return None
            
            # Get ResourceNode attributes
            node_key = row[NODE_KEY]
            parent_key = row[PARENT_KEY] if row[PARENT_KEY] else None
            access_url = row[ACCESS_INFO] if row[ACCESS_INFO] else None
            launch_params = row[LAUNCH_PARAMS] if row[LAUNCH_PARAMS] else ''
            template = self.module_cache.templates.get(row[TEMPLATE_NAME], None) if row[TEMPLATE_NAME] else None

            # Create ResourceNode record
            node = ResourceNodeRecord(
                node_key=node_key,
                template=template,
                parent_key=parent_key,
                access_info=access_url,
                launch_params=launch_params
            )
            
            # If not cascade, return the node with no children loaded
            if not is_cascade:
                return node
            
            # Get all children of the node
            children = self._get_child_keys(node_key)
            if children:
                with self._connect_db() as conn:
                    cursor = conn.execute(f"""
                        SELECT {NODE_KEY}, {TEMPLATE_NAME}, {ACCESS_INFO} FROM {NODE_TABLE} WHERE {PARENT_KEY} = ?
                    """, (node_key,))
                    child_rows = cursor.fetchall()
                    for child_row in child_rows:
                        child_node = ResourceNodeRecord(
                            node_key=child_row[NODE_KEY],
                            access_info=child_row[ACCESS_INFO] if child_row[ACCESS_INFO] else None,
                            template=self.module_cache.templates.get(child_row[TEMPLATE_NAME], None) if child_row[TEMPLATE_NAME] else None
                        )
                        node.add_child(child_node)

            return node
    
    def proxy_node(
        self,
        node_key: str, node_template_name: str,
        access_address: str, remote_node_key: str
    ) -> tuple[bool, str]:
        # Check if node already exists in db
        if (self._has_node(node_key)):
            logger.debug(f'Node {node_key} already exists, skipping')
            return True, ''

        try:
            # Add node to the resource tree
            parent_key = '.'.join(node_key.split('.')[:-1])
            access_info = f'{access_address}::{remote_node_key}'
            with self._connect_db() as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    INSERT INTO {NODE_TABLE} ({NODE_KEY}, {PARENT_KEY}, {TEMPLATE_NAME}, {ACCESS_INFO}) VALUES (?, ?, ?, ?)
                """, (node_key, parent_key, node_template_name, access_info))
                conn.commit()

            logger.info(f'Successfully proxy node "{node_key}" with ResourceNodeTemplate "{node_template_name}"')
            return True, ''
        
        except Exception as e:
            logger.error(f'Failed to proxy node "{node_key}" with ResourceNodeTemplate "{node_template_name}": {e}')
            return False, str(e)

    def mount(
        self, node_key: str,
        node_template_name: str | None = None, mount_params: any = None
    ) -> tuple[bool, str]:
        # Check if node already exists in db
        if (self._has_node(node_key)):
            logger.debug(f'Node {node_key} already mounted, skipping')
            return True, ''

        # Validate resource SET node
        # If node_template_name is not provided
        # Meaning this is a resource set node (no mount params), not a resource node (with mount params or not)
        # If mount_params are provided for a resource set node, raise a warning and ignore mount_params
        if not node_template_name and mount_params:
            logger.warning(f'Mount parameters are provided for resource set node "{node_key}", ignoring them')

        node_template: ResourceNodeTemplateModule | None = None
        try:
            # Validate resource node template
            if node_template_name:
                node_template = self.module_cache.templates.get(node_template_name, None)
                if node_template is None:
                    raise ValueError(f'ResourceNodeTemplate "{node_template_name}" not found in noodle module cache')
            
            # Validate parent key
            parent_key = '.'.join(node_key.split('.')[:-1])
            parent_key = parent_key if parent_key else None
            if parent_key and not self._has_node(parent_key):
                raise ValueError(f'Parent node "{parent_key}" not found in scene for node "{node_key}"')

            # Call resource node mount hook and get launch parameters
            launch_params_json = None
            if node_template:
                launch_params = node_template.mount(node_key, mount_params)
                if launch_params is not None and not isinstance(launch_params, dict):
                    raise ValueError(f'Launch parameters for node "{node_key}" must be a dictionary if provided')
                # Convert to JSON string
                launch_params_json = json.dumps(launch_params, indent=4) if launch_params else None

            # If all validations pass, insert node into db
            self._insert_node(node_key, parent_key, node_template_name, launch_params_json)

            logger.info(f'Successfully mounted node "{node_key}"')
            return True, ''

        except Exception as e:
            logger.error(f'Failed to mount node "{node_key}": {e}')
            return False, str(e)

    def unmount(self, node_key: str) -> tuple[bool, str]:
        """Unmount a node from the scene"""
        # If the node does not exist, return True, as it is already unmounted
        if not self._has_node(node_key):
            return True, ''

        nodes_to_delete: list[ResourceNodeRecord] = []
        try:
            # Try to unmount node recursively
            node_stack = [node_key]
            while node_stack:
                current_key = node_stack.pop()
                current_node = self._load_node_record(current_key, is_cascade=True)
                
                # Skip proxy nodes
                access_info = current_node.access_info
                is_proxy = access_info is not None and '::' in access_info
                if is_proxy:
                    continue
                
                # If the node is locked, stop unmounting and raise an error
                # For this node is currently connected
                if RWLock.is_node_locked(current_key):
                    message = f'Node "{current_key}" is locked, cannot unmount node "{node_key}" recursively. Unlock node "{current_key}" first, then retry unmounting.'
                    raise ValueError(message)
                else:
                    nodes_to_delete.append(current_node)
                    RWLock.lock_node(current_key, 'w', 'l') # lock the node locally to prevent new connections during unmounting
                
                # If the current node has children (a resource set node or a resource node with children)
                # Add all children to the stack for deletion check
                if current_node.has_children:
                    child_keys = self._get_child_keys(current_key)
                    node_stack.extend(child_keys)

            # Delete picked nodes
            for node in nodes_to_delete:
                # Delete node from db
                self._delete_node(node.node_key)
                # Call unmount hook if applicable
                if node.template is not None:
                    node.template.unmount(node.node_key)
            # Release the locks
            RWLock.unlock_nodes([node.node_key for node in nodes_to_delete])
            
            logger.debug(f'Successfully unmounted node "{node_key}"')
            return True, ''

        except Exception as e:
            logger.error(f'Failed to unmount node "{node_key}": {e}')
            # Release any locks acquired before the error
            if nodes_to_delete:
                RWLock.unlock_nodes([node.node_key for node in nodes_to_delete])
            return False, str(e)

    def _get_node(
        self,
        icrm: Type[T], node_key: str,
        access_mode: Literal['lr', 'lw', 'pr', 'pw'],
        timeout: float | None = None, retry_interval: float = 1.0
    ) -> IResourceNode[T]:
        # Check if icrm_class is valid (must be an ICRM class)
        if icrm and getattr(icrm, 'direction', None) is None:
            raise ValueError(f'Provided icrm_class {icrm.__name__} is not an ICRM class, provide an ICRM class instead')
        
        # If the node exists in a remote Noodle
        # Return as a RemoteResourceNode
        if node_key.startswith('http'):
            return RemoteResourceNode(
                icrm, node_key,
                access_mode, timeout, retry_interval
            )
        
        # Get node record from the resource tree
        # Check if the node exists and not a resource set
        node_record = self._load_node_record(node_key, is_cascade=False)
        if node_record is None:
            raise ValueError(f'Node "{node_key}" not found in noodle resource tree')
        if node_record.template is None:
            raise ValueError(f'Node "{node_key}" is a resource set node, cannot get its service')
        
        # If the node is a proxy of a remote node
        # Return as a RemoteResourceNodeProxy
        if node_record.access_info is not None:
            return RemoteResourceNodeProxy(
                icrm, node_record,
                access_mode, timeout, retry_interval
            )
        
        # If the node is a local node
        # Return as a ResourceNode
        return ResourceNode(
            icrm, node_record,
            access_mode, timeout, retry_interval
        )
    
    @contextmanager
    def connect(
        self,
        icrm: Type[T],
        node_key: str,
        access_mode: Literal['lr', 'lw', 'pr', 'pw'],
        timeout: float | None = None,
        retry_interval: float = 1.0,
        lock_id: str | None = None
    ) -> Generator[T, None, None]:
        """Context manager to connect to a node"""
        icrm_instance: T
        is_remote = node_key.startswith('http') and '::' in node_key
        
        # If lock_id is provided, validate the lock exists and yield the ICRM instance directly
        if lock_id:
            server_address = ''
            if is_remote:
                access_address, remote_node_key = node_key.split('::', 1)
                lock_info_api = f'{access_address}/noodle/lock/?lock_id={lock_id}'
                
                response = requests.get(lock_info_api)
                if response.status_code == 404:
                    raise ValueError(f'Lock {lock_id} not found for node {node_key}')
                elif response.status_code != 200:
                    raise RuntimeError(f'Failed to validate lock for remote CRM server: {response.text}')
                else:
                    lock_info = LockInfo(**response.json())
                    if lock_info.node_key != remote_node_key:
                        raise ValueError(f'Lock {lock_id} does not belong to node {node_key}')
                    if access_mode[1] == 'w' and lock_info.lock_type == 'r':
                        raise ValueError(f'Lock {lock_id} access mode "{lock_info.lock_type}" does not have write permission')
                    
                    server_address = f'{access_address}/noodle/proxy/?node_key={remote_node_key}&lock_id={lock_id}'
            else:
                lock_info = RWLock.get_lock_info(lock_id)
                if not lock_info or lock_info.node_key != node_key:
                    raise ValueError(f'Lock {lock_id} not found for node {node_key}')
                if access_mode[1] == 'w' and lock_info.lock_type == 'r':
                    raise ValueError(f'Lock {lock_id} access mode "{lock_info.lock_type}" does not have write permission')
                
                server_address = 'memory://' + node_key.replace('.', '_') + f'_{lock_id}'
            
            # Generate an ICRM instance related to the node CRM
            icrm_instance = icrm()
            client = cc.rpc.Client(server_address)
            icrm_instance.client = client
        
        else:
            # Get the node 
            node = self._get_node(icrm, node_key, access_mode, timeout, retry_interval)
            if node is None:
                raise ValueError(f'Node "{node_key}" not found or inaccessible')
            
            icrm_instance = node.crm
            
        try:
            
            yield icrm_instance

        finally:
            # If lock_id is not provided, meaning the connection is temporary
            # Release the lock and terminate the node CRM server if applicable
            if not lock_id:
                node.terminate()
    
    def get_node_info(self, node_key: str, child_start_index: int = 0, child_end_index: int | None = None) -> ResourceNodeInfo | None:
        # Check if node exists in the scene
        if not self._has_node(node_key):
            return None
        
        # Load node from the database
        node_record = self._load_node_record(node_key, is_cascade=True)

        child_start_index = min(child_start_index, len(node_record.children))
        child_end_index = len(node_record.children) if child_end_index is None else min(child_end_index, len(node_record.children))

        # Get info of child nodes
        children_info: list[ResourceNodeInfo] = []
        for child in node_record.children[child_start_index:child_end_index]:
            children_info.append(
                ResourceNodeInfo(
                    node_key=child.node_key,
                    access_info=child.access_info,
                    template_name=child.template.name if child.template else None,
                    children=None   # do not focus on children info of children
                )
            )
        
        return ResourceNodeInfo(
            node_key=node_record.node_key,
            access_info=node_record.access_info,
            template_name=node_record.template.name if node_record.template else None,
            children=children_info if children_info else None
        )
    
    def link(
        self,
        icrm: Type[T], node_key: str, access_mode: Literal['r', 'w'],
        timeout: float | None = None, retry_interval: float = 1.0
    ) -> str | None:
        """
        Link to a resource node in Noodle resource tree.
        Returns the lock ID if successful, None otherwise.
        
        Notice: link operation always uses process-level access for the node connection must be long-lived
        """
        # Get the node
        node = self._get_node(icrm, node_key, 'p' + access_mode, timeout, retry_interval)
        return node.lock_id
    
    def access(
        self,
        icrm_class: Type[T], node_key: str, lock_id: str
    ) -> T:
        node_server_address = ''
        is_remote = node_key.startswith('http') and '::' in node_key
        
        # Check if the lock exists
        does_lock_exist = False
        if is_remote:
            access_address, remote_node_key = node_key.split('::', 1)
            lock_info_api = f'{access_address}/noodle/lock/?lock_id={lock_id}'
            node_server_address = f'{access_address}/noodle/proxy/?node_key={remote_node_key}&lock_id={lock_id}'
            
            response = requests.get(lock_info_api)
            if response.status_code == 404:
                does_lock_exist = False
            elif response.status_code != 200:
                raise RuntimeError(f'Failed to validate lock for remote CRM server: {response.text}')
            else:
                lock_info = LockInfo(**response.json())
                if lock_info.node_key != remote_node_key:
                    does_lock_exist = False
                does_lock_exist = True
        else:
            does_lock_exist = RWLock.has_lock(lock_id)
            node_server_address = 'memory://' + node_key.replace('.', '_') + f'_{lock_id}'
            
        if not does_lock_exist:
            raise ValueError(f'Lock {lock_id} not found for node {node_key}')
        
        
        # Generate an ICRM instance related to the node CRM
        icrm = icrm_class()
        client = cc.rpc.Client(node_server_address)
        icrm.client = client
        return icrm
    
    def unlink(self, node_key: str, lock_id: str) -> tuple[bool, str | None]:
        error: str | None = None
        node_server_address = ''
        is_remote = node_key.startswith('http') and '::' in node_key
        
        # Check if the lock exists
        does_lock_exist = False
        if is_remote:
            access_address, remote_node_key = node_key.split('::', 1)
            lock_info_api = f'{access_address}/noodle/lock/?lock_id={lock_id}'
            node_server_address = f'{access_address}/noodle/proxy/?node_key={remote_node_key}&lock_id={lock_id}'
            
            response = requests.get(lock_info_api)
            if response.status_code == 404:
                does_lock_exist = False
            elif response.status_code != 200:
                raise RuntimeError(f'Failed to validate lock for remote CRM server: {response.text}')
            else:
                lock_info = LockInfo(**response.json())
                if lock_info.node_key != remote_node_key:
                    does_lock_exist = False
                does_lock_exist = True
        else:
            does_lock_exist = RWLock.has_lock(lock_id)
            node_server_address = 'memory://' + node_key.replace('.', '_') + f'_{lock_id}'

        if not does_lock_exist:
            error = f'Lock {lock_id} not found for node {node_key}'
            return False, error
        
        # Deactivate the node CRM server
        if is_remote:
            try:
                response = requests.delete(node_server_address)
                if response.status_code != 200:
                    raise RuntimeError(f'HTTP {response.status_code}: {response.text}')
                else:
                    return True, error
            except Exception as e:
                error = f'Error deactivating remote node server {node_key}: {e}'
                return False, error
        else:
            cc.rpc.Client.shutdown(node_server_address, -1.0)
            
            # Remove the lock
            RWLock.remove_lock(lock_id)
            return True, error