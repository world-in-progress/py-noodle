import sqlite3
import logging
import subprocess
import c_two as cc
from dataclasses import dataclass
from typing import TypeVar, Literal
from contextlib import contextmanager

from .lock import RWLock
from ..scenario import Scenario
from .scene_node import SceneNode, SceneNodeRecord

T = TypeVar('T')
logger = logging.getLogger(__name__)

# DB-related constants
SCENE_TABLE = 'scene'
NODE_KEY = 'node_key'
PARENT_KEY = 'parent_key'
LAUNCH_PARAMS = 'launch_params'
SCENARIO_NODE_NAME = 'scenario_node_name'
READING_COUNT = 'reading_count'

DEPENDENCY_TABLE = 'dependency'
DEPENDENT_KEY = 'dependent_key'

@dataclass
class ProcessInfo:
    address: str
    start_time: float = 0.0
    scenario_node_name: str = ''
    process: subprocess.Popen | None = None

class Treeger:
    def __init__(self, scenario: Scenario):
        # Get scenario graph
        self.scenario = scenario
        self.scene_path = self.scenario.scene_path
        
        # Initialize scene db
        self._init_scene()
            
    def _init_scene(self):
        # Create database directory if it doesn't exist
        self.scene_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create the database file if it doesn't exist
        with sqlite3.connect(self.scene_path) as conn:
            # Create the scene table
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCENE_TABLE} (
                    {PARENT_KEY} TEXT,
                    {LAUNCH_PARAMS} TEXT,
                    {NODE_KEY} TEXT PRIMARY KEY,
                    {SCENARIO_NODE_NAME} TEXT NOT NULL,
                    {READING_COUNT} INTEGER NOT NULL DEFAULT 0,
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
                    PRIMARY KEY ({NODE_KEY}, {DEPENDENT_KEY}),
                    FOREIGN KEY ({NODE_KEY}) REFERENCES {SCENE_TABLE} ({NODE_KEY}) ON DELETE CASCADE,
                    FOREIGN KEY ({DEPENDENT_KEY}) REFERENCES {SCENE_TABLE} ({NODE_KEY}) ON DELETE CASCADE
                )
            """)
            
            conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{DEPENDENT_KEY} ON {DEPENDENCY_TABLE}({DEPENDENT_KEY})')
            
            conn.commit()
    
    @contextmanager
    def _connect_db(self):
        """Context manager for database connection."""
        conn = sqlite3.connect(self.scene_path)
        conn.row_factory = sqlite3.Row  # enable column access by name
        try:
            yield conn
        finally:
            conn.close()
    
    def _node_exists_in_db(self, node_key: str) -> bool:
        """Check if a node exists in the database"""
        with self._connect_db() as conn:
            cursor = conn.execute(f'SELECT 1 FROM {SCENE_TABLE} WHERE {NODE_KEY} = ?', (node_key,))
            return cursor.fetchone() is not None

    def _insert_node_to_db(self, node_key: str, scenario_node_name: str, launch_params: str, parent_key: str | None, dependent_node_keys: list[str]) -> None:
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
            for dep_key in dependent_node_keys:
                conn.execute(f"""
                    INSERT OR IGNORE INTO {DEPENDENCY_TABLE} ({NODE_KEY}, {DEPENDENT_KEY})
                    VALUES (?, ?)
                """, (node_key, dep_key))
            
            conn.commit()
    
    def _get_child_keys_from_db(self, parent_key: str) -> list[str]:
        """Get all child node keys for a given parent from databse"""
        with self._connect_db() as conn:
            cursor = conn.execute(f'SELECT {NODE_KEY} FROM {SCENE_TABLE} WHERE {PARENT_KEY} = ?', (parent_key,))
            return [row[NODE_KEY] for row in cursor.fetchall()]

    def _delete_node_from_db(self, node_key: str) -> None:
        """Delete a node from the database"""
        with self._connect_db() as conn:
            # Only delete node from the scene table
            # Dependencies are automatically deleted due to ON DELETE CASCADE in the dependency table
            conn.execute(f'DELETE FROM {SCENE_TABLE} WHERE {NODE_KEY} = ?', (node_key,))
            conn.commit()
    
    def _load_node_from_db(self, node_key: str, is_cascade: bool) -> SceneNodeRecord | None:
        """Load a single node from the database"""
        with self._connect_db() as conn:
            cursor = conn.execute(f"""
                SELECT {NODE_KEY}, {SCENARIO_NODE_NAME}, {LAUNCH_PARAMS}, {PARENT_KEY}
                FROM {SCENE_TABLE}
                WHERE {NODE_KEY} = ?
            """, (node_key,))
            row = cursor.fetchone()
            if row is None:
                return None
            
            # Get SceneNode attributes
            node_key = row[NODE_KEY]
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
                scenario_node=scenario_node,
                launch_params=launch_params
            )
            
            # If not cascade, return the node with no children loaded
            if not is_cascade:
                return node
            
            # Get all children of the node
            children = self._get_child_keys_from_db(node_key)
            if children:
                with self._connect_db() as conn:
                    cursor = conn.execute(f"""
                        SELECT {NODE_KEY}, {SCENARIO_NODE_NAME} FROM {SCENE_TABLE} WHERE {PARENT_KEY} = ?
                    """, (node_key,))
                    child_rows = cursor.fetchall()
                    for child_row in child_rows:
                        child_node = SceneNodeRecord(
                            node_key=child_row[NODE_KEY],
                            scenario_node=self.scenario[child_row[SCENARIO_NODE_NAME]] if child_row[SCENARIO_NODE_NAME] else None,
                            launch_params=''
                        )
                        node.add_child(child_node)

            return node
    
    def _is_node_independent(self, node_key: str) -> bool:
        """Check if a node is independent (does not exist in the dependency table)"""
        with self._connect_db() as conn:
            cursor = conn.execute(f'SELECT 1 FROM {DEPENDENCY_TABLE} WHERE {NODE_KEY} = ?', (node_key,))
            return cursor.fetchone() is None

    def _is_node_depended(self, node_key: str) -> bool:
        """Check if a node is depended by other node (as a dependent_key by another node)"""
        with self._connect_db() as conn:
            cursor = conn.execute(f'SELECT 1 FROM {DEPENDENCY_TABLE} WHERE {DEPENDENT_KEY} = ?', (node_key,))
            return cursor.fetchone() is not None
        
    def mount_node(self, node_key: str, scenario_node_name: str = '', launch_params: str = '', dependent_node_keys: list[str] = []) -> None:
        # Check if node already exists in db
        if (self._node_exists_in_db(node_key)):
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
            if len(dependent_node_keys) != len(dep_map):
                raise ValueError(f'Node {scenario_node_name} has {len(scenario_node.dependencies)} dependencies, but {len(dependent_node_keys)} provided')
            
            for dep_key in dependent_node_keys:
                if not self._node_exists_in_db(dep_key):
                    raise ValueError(f'Dependency node {dep_key} not found in scene for node {node_key}')
                dep_node = self._load_node_from_db(dep_key, is_cascade=False)
                dep_map[dep_node.scenario_node.name] = True
                
            if not all(dep_map.values()):
                missing_deps = [name for name, exists in dep_map.items() if not exists]
                raise ValueError(f'Missing dependencies for node {scenario_node_name}: {", ".join(missing_deps)}')
        
        # Validate parent key
        parent_key = '.'.join(node_key.split('.')[:-1])
        if parent_key and not self._node_exists_in_db(parent_key):
            raise ValueError(f'Parent node "{parent_key}" not found in scene for node "{node_key}"')

        # If all validations pass, insert node into db
        self._insert_node_to_db(node_key, scenario_node_name if scenario_node_name else None, launch_params, parent_key if parent_key else None, dependent_node_keys)

        logger.info(f'Successfully mounted node "{node_key}" for scenario "{scenario_node_name}"')

    def unmount_node(self, node_key: str) -> bool:
        """Unmount a node from the scene"""
        if not self._node_exists_in_db(node_key):
            logger.warning(f'Node "{node_key}" not found in scene, cannot unmount')
            return False
        
        # Try to unmount node recursively
        nodes_count = 0
        node_stack = [node_key]
        nodes_to_delete: list[str] = []
        while node_stack:
            nodes_count += 1
            current_key = node_stack.pop()
            current_node = self._load_node_from_db(current_key, is_cascade=False)
            
            # If the node is depended, skip it
            if self._is_node_depended(current_key):
                continue
            # If the node is active, skip it
            if RWLock.is_node_active(self.db_path, current_key):
                continue
            else:
                nodes_to_delete.append(current_key)
            
            # If the current node has children (a resource set node)
            # Add all children to the stack for deletion check
            if current_node.is_set:
                child_keys = self._get_child_keys_from_db(current_key)
                node_stack.extend(child_keys)
        
        if len(nodes_to_delete) != nodes_count:
            logger.warning(
                f'Failed to unmount node "{node_key}": {nodes_count - len(nodes_to_delete)} '
                f'node(s) cannot be deleted because they have active dependencies. '
                f'Remove dependencies first, then retry unmounting.'
            )
            return False

        # Delete the node from the database
        self._delete_node_from_db(node_key)
        
        logger.info(f'Successfully unmounted node "{node_key}"')
        return True

    def get_node(
        self,
        node_key: str, writable: bool, 
        access_mode: Literal['l', 'p'],
        timeout: float | None = None,
        retry_interval: float = 1.0
    ) -> SceneNode:
        node_record = self._load_node_from_db(node_key, is_cascade=False)
        if node_record is None:
            raise ValueError(f'Node "{node_key}" not found in scene tree')
        if node_record.scenario_node is None:
            raise ValueError(f'Node "{node_key}" is a resource set node, cannot get its service')
        
        return SceneNode(
            self.scene_path, node_record,
            'w' if writable else 'r',
            access_mode, timeout, retry_interval
        )