import json
import socket
import sqlite3
import logging
import requests
from contextlib import contextmanager
from typing import TypeVar, Literal, Type, Generator

from .lock import RWLock
from ..config import settings
from ..scenario import Scenario
from ..schemas.scene import SceneNodeInfo
from ..schemas.dependencies import DependencyRequest
from .scene_node import SceneNode, SceneNodeProxy, SceneNodeRecord

T = TypeVar('T')
logger = logging.getLogger(__name__)

# DB-related constants
SCENE_TABLE = 'scene'
NODE_KEY = 'node_key'
PARENT_KEY = 'parent_key'
LAUNCH_PARAMS = 'launch_params'
SCENARIO_NODE_NAME = 'scenario_node_name'
ACCESS_INFO = 'access_info' # access URL :: remote node key

DEPENDENCY_TABLE = 'dependency'
DEPENDENT_KEY = 'dependent_key'

class Treeger:
    def __init__(self):
        # Get scenario graph
        self.scenario = Scenario()
        
        # Init server URL
        port = settings.SERVER_PORT
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        self.url = f'http://{ip}:{port}'
        
        # Initialize scene db
        self._init_scene()
            
    def _init_scene(self):
        # Create the database file if it doesn't exist
        with sqlite3.connect(settings.SQLITE_PATH) as conn:
            # Create the scene table
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCENE_TABLE} (
                    {PARENT_KEY} TEXT,
                    {SCENARIO_NODE_NAME} TEXT,
                    {NODE_KEY} TEXT PRIMARY KEY,
                    {ACCESS_INFO} TEXT DEFAULT NULL,
                    {LAUNCH_PARAMS} TEXT DEFAULT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY ({PARENT_KEY}) REFERENCES {SCENE_TABLE} ({NODE_KEY}) ON DELETE CASCADE
                )
            """)
            conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{PARENT_KEY} ON {SCENE_TABLE}({PARENT_KEY})')
            conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{SCENARIO_NODE_NAME} ON {SCENE_TABLE}({SCENARIO_NODE_NAME})')
            
            # Create the dependency table
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {DEPENDENCY_TABLE} (
                    {NODE_KEY} TEXT NOT NULL,
                    {DEPENDENT_KEY} TEXT NOT NULL,
                    PRIMARY KEY ({NODE_KEY}, {DEPENDENT_KEY})
                )
            """)
            
            conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{DEPENDENT_KEY} ON {DEPENDENCY_TABLE}({DEPENDENT_KEY})')
            
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
            cursor = conn.execute(f'SELECT 1 FROM {SCENE_TABLE} WHERE {NODE_KEY} = ?', (node_key,))
            return cursor.fetchone() is not None

    def _insert_node(self, node_key: str, scenario_node_name: str, launch_params: str, parent_key: str | None, dependent_node_keys_or_infos: list[str]) -> None:
        """Insert a new node into the database"""
        with self._connect_db() as conn:
            # Add node to the scene table
            conn.execute(f"""
                INSERT INTO {SCENE_TABLE} ({NODE_KEY}, {SCENARIO_NODE_NAME}, {LAUNCH_PARAMS}, {PARENT_KEY})
                VALUES (?, ?, ?, ?)
            """, (
                node_key,
                scenario_node_name,
                launch_params if launch_params else None,
                parent_key if parent_key else None
            ))
            
            # Add dependencies to the dependency table
            for dep_key in dependent_node_keys_or_infos:
                # is_remote = dep_key.startswith('http')
                # if is_remote:
                #     continue
                conn.execute(f"""
                    INSERT OR IGNORE INTO {DEPENDENCY_TABLE} ({NODE_KEY}, {DEPENDENT_KEY})
                    VALUES (?, ?)
                """, (dep_key, node_key))
            
            conn.commit()

    def _delete_node(self, node_key: str) -> None:
        """Delete a node from the database"""
        with self._connect_db() as conn:
            # Delete node from the scene table
            conn.execute(f'DELETE FROM {SCENE_TABLE} WHERE {NODE_KEY} = ?', (node_key,))
            
            # Delete dependencies from the dependency table
            conn.execute(f'DELETE FROM {DEPENDENCY_TABLE} WHERE {NODE_KEY} = ?', (node_key,))
            conn.commit()
    
    def _get_child_keys(self, parent_key: str) -> list[str]:
        """Get all child node keys for a given parent from databse"""
        with self._connect_db() as conn:
            cursor = conn.execute(f'SELECT {NODE_KEY} FROM {SCENE_TABLE} WHERE {PARENT_KEY} = ?', (parent_key,))
            return [row[NODE_KEY] for row in cursor.fetchall()]
    
    def _load_node(self, node_key: str, is_cascade: bool) -> SceneNodeRecord | None:
        """Load a single node from the database"""
        with self._connect_db() as conn:
            cursor = conn.execute(f"""
                SELECT {NODE_KEY}, {SCENARIO_NODE_NAME}, {LAUNCH_PARAMS}, {PARENT_KEY}, {ACCESS_INFO}
                FROM {SCENE_TABLE}
                WHERE {NODE_KEY} = ?
            """, (node_key,))
            row = cursor.fetchone()
            if row is None:
                return None
            
            # Get SceneNode attributes
            node_key = row[NODE_KEY]
            access_url = row[ACCESS_INFO] if row[ACCESS_INFO] else None
            parent_key = row[PARENT_KEY] if row[PARENT_KEY] else None
            launch_params = row[LAUNCH_PARAMS] if row[LAUNCH_PARAMS] else ''
            scenario_node = self.scenario[row[SCENARIO_NODE_NAME]] if row[SCENARIO_NODE_NAME] else None
            if scenario_node is None:
                logger.error(f'Scenario node {row[SCENARIO_NODE_NAME]} not found in scenario graph')
                return None
            
            # Create SceneNode instance
            node = SceneNodeRecord(
                node_key=node_key,
                parent_key=parent_key,
                access_info=access_url,
                scenario_node=scenario_node,
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
                        SELECT {NODE_KEY}, {SCENARIO_NODE_NAME}, {ACCESS_INFO} FROM {SCENE_TABLE} WHERE {PARENT_KEY} = ?
                    """, (node_key,))
                    child_rows = cursor.fetchall()
                    for child_row in child_rows:
                        child_node = SceneNodeRecord(
                            node_key=child_row[NODE_KEY],
                            access_info=child_row[ACCESS_INFO] if child_row[ACCESS_INFO] else None,
                            scenario_node=self.scenario[child_row[SCENARIO_NODE_NAME]] if child_row[SCENARIO_NODE_NAME] else None
                        )
                        node.add_child(child_node)

            return node

    def _is_node_dependency(self, node_key: str) -> bool:
        """Check if a node is depended on by other node"""
        with self._connect_db() as conn:
            cursor = conn.execute(f'SELECT 1 FROM {DEPENDENCY_TABLE} WHERE {NODE_KEY} = ?', (node_key,))
            return cursor.fetchone() is not None
    
    def add_dependency(self, node_key: str, dependent_node_key: str) -> None:
        with self._connect_db() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                INSERT INTO {DEPENDENCY_TABLE} ({NODE_KEY}, {DEPENDENT_KEY}) VALUES (?, ?)
            """, (node_key, dependent_node_key))
            conn.commit()
    
    def remove_dependency(self, node_key: str, dependent_node_key: str) -> None:
        with self._connect_db() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                DELETE FROM {DEPENDENCY_TABLE} WHERE {NODE_KEY} = ? AND {DEPENDENT_KEY} = ?
            """, (node_key, dependent_node_key))
            conn.commit()
    
    def import_node(
        self,
        node_key: str, scenario_node_name: str,
        server_url: str, remote_node_key: str
    ) -> None:
        parent_key = '.'.join(node_key.split('.')[:-1])
        access_info = f'{server_url}::{remote_node_key}'
        with self._connect_db() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                INSERT INTO {SCENE_TABLE} ({NODE_KEY}, {PARENT_KEY}, {SCENARIO_NODE_NAME}, {ACCESS_INFO}) VALUES (?, ?, ?, ?)
            """, (node_key, parent_key, scenario_node_name, access_info))
            conn.commit()

        # Add dependency relation to the remote Noodle
        req = DependencyRequest(
            method='ADD',
            node_key=remote_node_key,
            dependent_key=node_key,
            dependent_url=self.url,
        )
        response = requests.post(f'{server_url}/noodle/dependencies/', json=req.model_dump())
        if response.status_code != 200:
            logger.error(f'Failed to add dependency to remote noodle: {response.text}')
            raise requests.RequestException(f'Failed to add dependency: {response.text}')

    def mount_node(
        self,
        node_key: str, scenario_node_name: str = '',
        launch_params: any = None, dependent_node_keys_or_infos: list[str] = []
    ) -> None:
        # Check if node already exists in db
        if (self._has_node(node_key)):
            logger.debug(f'Node {node_key} already mounted, skipping')
            return
        
        # Validate scenario node name
        # - If scenario_node_name is not provided, meaning this is a resource set node, not a resource node
        if not scenario_node_name and launch_params:
            logger.warning(f'Launch parameters provided for resource set node "{node_key}", skipping')

        # - Validate scenario_node_name
        if scenario_node_name:
            scenario_node = self.scenario[scenario_node_name]
            if scenario_node is None:
                raise ValueError(f'Scenario node {scenario_node_name} not found in scenario graph')
            
            # -- Check if dependencies are valid
            dep_map: dict[str, bool] = {dep.name: False for dep in scenario_node.dependencies}
            
            # --- Check if all dependencies are provided
            if len(dependent_node_keys_or_infos) != len(dep_map):
                raise ValueError(f'Node {scenario_node_name} has {len(scenario_node.dependencies)} dependencies, but {len(dependent_node_keys_or_infos)} provided')
            
            for dep_key in dependent_node_keys_or_infos:
                if dep_key.startswith('http'):
                    # If the dependency is a remote node, check if it exists in the remote noodle
                    try:
                        # Fetch the node info from the remote noodle
                        access_url, dep_key = dep_key.split('::')
                        response = requests.get(f'{access_url}/noodle/scene/', params={'node_key': dep_key})
                        
                        # Check if the response is successful
                        if response.status_code != 200:
                            raise ValueError(f'Dependency node {dep_key} not found in remote noodle {access_url}')
                        
                        # Parse the response to get the node info
                        dep_node_info = SceneNodeInfo.model_validate(response.json())
                        
                        # Check if the dependency node exists in the local scenario graph
                        if dep_node_info.scenario_node_name not in dep_map:
                            raise ValueError(f'Dependency node {dep_node_info.scenario_node_name} not found in local scenario graph for node {scenario_node_name}')

                        # If the dependency node has no scenario node name (a resource-set node), raise an error
                        if dep_node_info.scenario_node_name is None:
                            raise ValueError(f'Dependency node {dep_key} has no scenario node name in remote noodle {access_url}')
                        
                        dep_map[dep_node_info.scenario_node_name] = True
                        
                        # Add dependency to the remote Noodle
                        req = DependencyRequest(
                            method='ADD',
                            node_key=dep_key,
                            dependent_key=node_key,
                            dependent_url=self.url,
                        )
                        response = requests.post(f'{access_url}/noodle/dependencies/', json=req.model_dump())
                        if response.status_code != 200:
                            logger.error(f'Failed to add dependency to remote noodle: {response.text}')
                            raise requests.RequestException(f'Failed to add dependency: {response.text}')
                        
                    except requests.RequestException as e:
                        raise ValueError(f'Failed to fetch dependency node {dep_key} from remote noodle {access_url}: {e}')
                else:
                    # If the dependency is a node key, check if it exists in the scene
                    if not self._has_node(dep_key):
                        raise ValueError(f'Dependency node {dep_key} not found in scene for node {node_key}')
                    
                    # Load the node record to check if it has a scenario node
                    dep_node_record = self._load_node(dep_key, is_cascade=False)
                    if dep_node_record.scenario_node is None:
                        raise ValueError(f'Dependency node {dep_key} is a resource set node, not a resource node')
                    
                    dep_map[dep_node_record.scenario_node.name] = True
                
            if not all(dep_map.values()):
                missing_deps = [name for name, exists in dep_map.items() if not exists]
                raise ValueError(f'Missing dependencies for node {scenario_node_name}: {", ".join(missing_deps)}')
        
        # Validate parent key
        parent_key = '.'.join(node_key.split('.')[:-1])
        if parent_key and not self._has_node(parent_key):
            raise ValueError(f'Parent node "{parent_key}" not found in scene for node "{node_key}"')

        # If all validations pass, insert node into db
        self._insert_node(node_key, scenario_node_name if scenario_node_name else None, json.dumps(launch_params, indent=4) if launch_params else None, parent_key if parent_key else None, dependent_node_keys_or_infos)

        logger.info(f'Successfully mounted node "{node_key}" for scenario "{scenario_node_name}"')

    def unmount_node(self, node_key: str) -> None:
        """Unmount a node from the scene"""
        if not self._has_node(node_key):
            logger.warning(f'Node "{node_key}" not found in scene, cannot unmount')
            return
        
        # Try to unmount node recursively
        nodes_count = 0
        node_stack = [node_key]
        nodes_to_delete: list[str] = []
        while node_stack:
            nodes_count += 1
            current_key = node_stack.pop()
            current_node = self._load_node(current_key, is_cascade=False)
            
            # If the node is depended, skip it
            if self._is_node_dependency(current_key):
                continue
            # If the node is active, skip it
            if RWLock.is_node_active(current_key):
                continue
            else:
                nodes_to_delete.append(current_key)
            
            # If the current node has children (a resource set node)
            # Add all children to the stack for deletion check
            if current_node.is_set:
                child_keys = self._get_child_keys(current_key)
                node_stack.extend(child_keys)
        
        if len(nodes_to_delete) != nodes_count:
            logger.warning(
                f'Failed to unmount node "{node_key}": '
                f'it is depended by {nodes_count - len(nodes_to_delete)} node(s). '
                f'Remove dependencies first, then retry unmounting.'
            )
            return

        # Delete the node from the database
        node_record = self._load_node(node_key, is_cascade=False)
        if node_record.access_info is not None:
            # If the node has an access URL, it is a remote node
            # The dependency needs to be removed from the remote noodle
            server_url, remote_node_key = node_record.access_info.split('::')
            req = DependencyRequest(
                method='REMOVE',
                node_key=remote_node_key,
                dependent_key=node_key,
                dependent_url=self.url,
            )
            response = requests.post(f'{server_url}/noodle/dependencies/', json=req.model_dump())
            if response.status_code != 200:
                logger.error(f'Failed to remove dependency from remote noodle: {response.text}')
                raise requests.RequestException(f'Failed to remove dependency: {response.text}')
        else:
            # If the node is a local node
            # Just delete it from the database
            self._delete_node(node_key)
        
        logger.info(f'Successfully unmounted node "{node_key}"')

    def get_node(
        self,
        icrm_class: Type[T],
        node_key: str,
        access_mode: Literal['lr', 'lw', 'pr', 'pw'],
        timeout: float | None = None,
        retry_interval: float = 1.0
    ) -> SceneNode[T] | SceneNodeProxy[T]:
        is_remote = node_key.startswith('http')
        if is_remote:
            return SceneNodeProxy(
                icrm_class, node_key,
                access_mode, timeout, retry_interval
            )
        
        node_record = self._load_node(node_key, is_cascade=False)
        if node_record is None:
            raise ValueError(f'Node "{node_key}" not found in scene tree')
        if node_record.scenario_node is None:
            raise ValueError(f'Node "{node_key}" is a resource set node, cannot get its service')
        
        return SceneNode(
            icrm_class, node_record,
            access_mode, timeout, retry_interval
        )
    
    @contextmanager
    def connect_node(
        self,
        icrm_class: Type[T],
        node_key: str,
        access_mode: Literal['lr', 'lw', 'pr', 'pw'],
        timeout: float | None = None,
        retry_interval: float = 1.0
    ) -> Generator[SceneNode[T] | SceneNodeProxy[T], None, None]:
        """Context manager to connect to a node"""
        node = self.get_node(icrm_class, node_key, access_mode, timeout, retry_interval)
        try:
            yield node
        finally:
            node.terminate()
    
    def get_node_info(self, node_key: str, child_start_index: int = 0, child_end_index: int | None = None) -> SceneNodeInfo | None:
        # Check if node exists in the scene
        if not self._has_node(node_key):
            return None
        
        # Load the node from the database
        node_record = self._load_node(node_key, is_cascade=True)
            
        child_start_index = min(child_start_index, len(node_record.children))
        child_end_index = len(node_record.children) if child_end_index is None else min(child_end_index, len(node_record.children))

        # Get info of child nodes
        children_info: list[SceneNodeInfo] = []
        for child in node_record.children[child_start_index:child_end_index]:
            children_info.append(
                SceneNodeInfo(
                    node_key=child.node_key,
                    access_info=child.access_info,
                    scenario_node_name=child.scenario_node.name if child.scenario_node else None,
                    children=None   # do not focus on children info of children
                )
            )
        
        return SceneNodeInfo(
            node_key=node_record.node_key,
            access_info=node_record.access_info,
            scenario_node_name=node_record.scenario_node.name if node_record.scenario_node else None,
            children=children_info if children_info else None
        )