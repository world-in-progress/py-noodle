import os
import sys
import time
import yaml
import json
import sqlite3
import logging
import threading
import subprocess
import c_two as cc
from pathlib import Path
from pydantic import BaseModel
from typing import TypeVar, Type
from contextlib import contextmanager
from dataclasses import dataclass, field

from .icrm import ITreeger
from ..scenario import Scenario
from ..schemas import ScenarioConfiguration, ScenarioNodeDescription
# from .icrm import ITreeger, CRMEntry, TreeMeta, ReuseAction, ScenarioNode, SceneNodeInfo, SceneNodeMeta, ScenarioNodeDescription, CRMDuration

T = TypeVar('T')
logger = logging.getLogger(__name__)

# DB-related constants
SCENE_TABLE = 'scene'
NODE_KEY = 'node_key'
PARENT_KEY = 'parent_key'
LAUNCH_PARAMS = 'launch_params'
SCENARIO_NODE_NAME = 'scenario_node_name'

DEPENDENCY_TABLE = 'dependency'
DEPENDENT_KEY = 'dependent_key'

@dataclass
class ProcessInfo:
    address: str
    start_time: float = 0.0
    scenario_node_name: str = ''
    process: subprocess.Popen | None = None

@dataclass
class SceneNodeRecord:
    node_key: str
    scenario_node_name: str | None   # None if this is a resource set node, not a resource node
    launch_params: str
    
    parent_key: str | None = None
    children: list['SceneNodeRecord'] = field(default_factory=list)
    
    def add_child(self, child: 'SceneNodeRecord'):
        self.children.append(child)
        self.children.sort(key=lambda child: child.node_key.split('.')[-1].lower())  # sort children by their name
        child.parent_key = self.node_key
    
    def add_children(self, children: list['SceneNodeRecord']):
        for child in children:
            self.add_child(child)
    
    @property
    def is_set(self):
        return self.scenario_node_name is None

# @cc.iicrm
class Treeger():
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
            cursor = conn.execute(f'SELECT {NODE_KEY} FROM {SCENE_TABLE} WHERE parent_key = ?', (parent_key,))
            return [row['node_key'] for row in cursor.fetchall()]
    
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
            node_key = row['node_key']
            parent_key = row['parent_key'] if row['parent_key'] else None
            launch_params = row['launch_params'] if row['launch_params'] else ''
            scenario_node = row['scenario_node_name'] if row['scenario_node_name'] else None
            if scenario_node is None:
                logger.error(f'Scenario node {row["scenario_node_name"]} not found in scenario graph')
                return None
            
            # Create SceneNode instance
            node = SceneNodeRecord(
                node_key=node_key,
                parent_key=parent_key,
                scenario_node_name=scenario_node,
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
                    
                    for child_row in cursor.fetchall():
                        child_scenario_node = self.scenario_node_dict.get(child_row['scenario_node_name'])
                        if child_scenario_node:
                            child_node = SceneNodeRecord(
                                node_key=child_row['node_key'],
                                scenario_node_name=child_scenario_node,
                                launch_params=''
                            )
                            node.add_child(child_node)
                        else:
                            logger.error(f'Scenario node {child_row["scenario_node_name"]} not found in scenario graph')

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
                dep_map[dep_node.scenario_node_name] = True
                
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
        
        # TODO: How to stop the node service if it is running?

        # Delete the node from the database
        self._delete_node_from_db(node_key)
        
        # TODO: How to delete the node resource?
        
        logger.info(f'Successfully unmounted node "{node_key}"')
        return True
    
    def fork_node(self, node_key: str, icrm: Type[T]) -> T:
        node_record = self._load_node_from_db(node_key, is_cascade=False)
        if node_record is None:
            raise ValueError(f'Node "{node_key}" not found in scene tree')

        scenario_node = self.scenario[node_record.scenario_node_name]
        if scenario_node is None:
            raise ValueError(f'Scenario node "{node_record.scenario_node_name}" not found in scenario graph')
        
        crm = scenario_node.crm_class
        

    # def _release_crm_process(self, node_key: str):
    #     if node_key in self.process_pool:
    #         process_info = self.process_pool[node_key]
            
    #         # Remove record from process pool and scene node in-flight set
    #         del self.process_pool[node_key]
    #         self.scene_nodes_in_flight[process_info.scenario_node_name].remove(node_key)
    
    # def _cleanup_finished_processes(self):
    #     finished_nodes = []
        
    #     for node_name, node_info in self.process_pool.items():
    #         process = node_info.process
    #         if process and process.poll() is not None:
    #             finished_nodes.append(node_name)
        
    #     for node_name in finished_nodes:
    #         self._release_crm_process(node_name)
    
    # def _unmount_node_recursively(self, node_key: str) -> bool:
    #     if not self._node_exists_in_db(node_key):
    #         logger.warning(f'Node "{node_key}" not found in scene, cannot unmount')
    #         return False
        
    #     # Get all child nodes from database
    #     child_keys = self._get_child_keys_from_db(node_key)
        
    #     # Recursively unmount all children
    #     for child_key in child_keys:
    #         self._unmount_node_recursively(child_key)
        
    #     # Stop the node service if it is running
    #     if node_key in self.process_pool:
    #         self.deactivate_node(node_key)
        
    #     # Remove from database
    #     self._delete_node_from_db(node_key)
            
    #     logger.info(f'Successfully unmounted node {node_key}')
    #     return True

    # def unmount_node(self, node_key: str) -> bool:
    #     with self.lock:
    #         return self._unmount_node_recursively(node_key)
        
    #     scene_path = ROOT_DIR / self.meta.configuration.scene_path
    #     with open(scene_path, 'w') as f:
    #         yaml.dump(scene_data, f, default_flow_style=False)
    #     logger.info(f'Scene serialized to {scene_path}')

    # def terminate(self) -> bool:
    #     with self.lock:
    #         try:
    #             for node_key in list(self.process_pool.keys()):
    #                 self.deactivate_node(node_key)
                
    #             logger.info('All nodes stopped successfully')
                
    #             return True
    #         except Exception as e:
    #             logger.error(f'Failed to terminate treeger: {e}')
    #             return False
    
    # def activate_node(self, node_key: str, reusibility: ReuseAction = ReuseAction.REPLACE, duration: CRMDuration = CRMDuration.Medium) -> str:
    #     with self.lock:
    #         self._cleanup_finished_processes()
    #         # Check if the node exists in the db
    #         if not self._node_exists_in_db(node_key):
    #             logger.error(f'Node "{node_key}" not found in scene or database')
    #             raise ValueError(f'Node "{node_key}" not found in scene or database')
    #         else:
    #             node = self._load_node_from_db(node_key)
            
    #         # Check if the node can be launched
    #         if not node.scenario_node.crm:
    #             raise ValueError(f'Node {node_key} does not have a CRM and cannot be launched directly')

    #         # Check if the node is already running
    #         if node_key in self.process_pool:
    #             process_info = self.process_pool[node_key]
    #             return process_info.address
            
    #         # Handle reusability actions
    #         flying_sibling_set = self.scene_nodes_in_flight.get(node.scenario_node.name)
    #         # Get the first available node sharing the same scenario node
    #         sibling_node_name = next(iter(flying_sibling_set), None)
    #         if sibling_node_name:
    #             if reusibility == ReuseAction.KEEP:
    #                 # Keep the crm process
    #                 sibling_process_info = self.process_pool.get(sibling_node_name)
    #                 return sibling_process_info.address

    #             elif reusibility == ReuseAction.REPLACE:
    #                 # Replace the sibling node with the new one (stop the sibling process and create below)
    #                 self.deactivate_node(sibling_node_name)

    #             elif reusibility == ReuseAction.FORK:
    #                 # Fork the sibling node, which means creating a new process for the node but keeping the sibling process running
    #                 pass

    #         # Try to allocate an address for the node
    #         try:
    #             address = f'memory://{node_key.replace("/", "_")}'
    #         except Exception as e:
    #             logger.error(f'Failed to allocate address for node {node_key}: {e}')
    #             raise

    #         # Try to launch a CRM server related to the node
    #         try:
    #             # Platform-specific subprocess arguments
    #             kwargs = {}
    #             if sys.platform != 'win32':
    #                 # Unix-specific: create new process group
    #                 kwargs['preexec_fn'] = os.setsid
                
    #             # Assmble the command to launch the CRM server
    #             params = node.launch_params
    #             crm_entry: CRMEntry = self.crm_entry_dict.get(node.scenario_node.crm, None)
    #             if crm_entry is None:
    #                 raise ValueError(f'CRM template {node.scenario_node.crm} not found in tree meta')
                
    #             cmd = [
    #                 sys.executable,
    #                 crm_entry.crm_launcher,
    #                 '--server_address', address,
    #                 '--timeout', str(duration.value),
    #             ]
    #             if params:
    #                 for key, value in params.items():
    #                     if isinstance(value, dict):
    #                         json_str = json.dumps(value, ensure_ascii=False)
    #                         if sys.platform == 'win32':
    #                             cmd.extend([f'--{key}', json_str])
    #                         else:
    #                             cmd.extend([f'--{key}', f"'{json_str}'"])
    #                     else:
    #                         cmd.extend([f'--{key}', str(value)])
                
    #             process = subprocess.Popen(
    #                 cmd,
    #                 **kwargs
    #             )
                
    #             # Register the process in the process pool and scene node in-flight set
    #             self.process_pool[node_key] = ProcessInfo(
    #                 address=address,
    #                 process=process,
    #                 start_time=time.time(),
    #                 scenario_node_name=node.scenario_node.name
    #             )
    #             self.scene_nodes_in_flight[node.scenario_node.name].add(node_key)
                
    #             # Pin the crm server
    #             while True:
    #                 if cc.rpc.Client.ping(address, timeout=1):
    #                     break
    #                 if time.time() - self.process_pool[node_key].start_time > 60:
    #                     raise RuntimeError(f'Timeout waiting for node "{node_key}" to start')
                    
    #                 time.sleep(0.1)

    #             logger.info(f'Successfully launched node "{node_key}" at {address}')
    #             return address

    #         except Exception as e:
    #             logger.error(f'Failed to launch node {node_key}: {e}')
    #             raise

    # def deactivate_node(self, node_key: str) -> bool:
    #     with self.lock:
    #         if node_key not in self.process_pool:
    #             logger.warning(f'Node "{node_key}" not found in process pool')
    #             return False
            
    #         try:
    #             process_info = self.process_pool[node_key]
    #             server_address = process_info.address
    #             if cc.rpc.Client.shutdown(server_address, timeout=60) is False:
    #                 raise RuntimeError(f'Failed to shutdown node "{node_key}" at {server_address}')
    #             else:
    #                 process = process_info.process
    #                 if process and process.poll() is None:
    #                     process.terminate()
    #                     process.wait()

    #             # Remove record from process pool and scene node in-flight set
    #             del self.process_pool[node_key]
    #             self.scene_nodes_in_flight[process_info.scenario_node_name].remove(node_key)
                
    #             # Cleanup finished processes
    #             self._cleanup_finished_processes()
    #             logger.info(f'Successfully stopped node "{node_key}"')
    #             return True
            
    #         except Exception as e:
    #             logger.error(f'Failed to stop node "{node_key}": {e}')
    #             return False

    # def get_scene_node_info(self, node_key: str, child_start_index: int = 0, child_end_index: int | None = None) -> SceneNodeMeta | None:
    #     with self.lock:
    #         # Check if the node exists in the scene
    #         if self._node_exists_in_db(node_key):
    #             # Load the node from database if it exists
    #             scene_node = self._load_node_from_db(node_key)
    #             if not scene_node:
    #                 logger.warning(f'Node "{node_key}" not found in scene or database')
    #                 return None

    #         # Get the SceneNode instance
    #         scenario_node_path = scene_node.scenario_node.semantic_path
            
    #         child_start_index = min(child_start_index, len(scene_node.children))
    #         child_end_index = len(scene_node.children) if child_end_index is None else min(child_end_index, len(scene_node.children))

    #         # Get meta of children nodes
    #         children_meta: list[SceneNodeMeta] = []
    #         for child in scene_node.children[child_start_index:child_end_index]:
    #             children_meta.append(SceneNodeMeta(
    #                 node_key=child.node_key,
    #                 scenario_path=child.scenario_node.semantic_path,
    #                 children=None  # do not focus on children meta of children
    #             ))

    #         return SceneNodeMeta(
    #             node_key=scene_node.node_key,
    #             scenario_path=scenario_node_path,
    #             children=children_meta if children_meta else None
    #         )
    
    # def get_scenario_description(self) -> list[ScenarioNodeDescription]:
    #     with self.lock:
    #         description: list[ScenarioNodeDescription] = []
            
    #         for scenario_node in self.scenario_node_dict.values():
    #             description.append(
    #                 ScenarioNodeDescription(
    #                     semanticPath=scenario_node.semantic_path,
    #                     children=[child.name for child in scenario_node.children]
    #                 )
    #             )
                
    #         return description
    
    # def get_node_info(self, node_key: str) -> SceneNodeInfo | None:
    #     with self.lock:
    #         self._cleanup_finished_processes()
            
    #         # Check if the node exists in db
    #         if not self._node_exists_in_db(node_key):
    #             logger.warning(f'Node "{node_key}" not found in database')
    #             return None
            
    #         # Get the SceneNode instance
    #         scene_node = self._load_node_from_db(node_key)
        
    #         # Initialize server_address to None by default
    #         server_address = None
            
    #         # Get the server address of the node if it is running
    #         if node_key in self.process_pool:
    #             process_info = self.process_pool[node_key]
    #             if process_info.process and process_info.process.poll() is None:
    #                 # Process is running, return its address
    #                 server_address = process_info.address
    #             else:
    #                 # Remove record from process pool and scene node in-flight set
    #                 del self.process_pool[node_key]
    #                 self.scene_nodes_in_flight[process_info.scenario_node_name].remove(node_key)
            
    #         # Prepare the node info
    #         node_info = SceneNodeInfo(
    #             node_key=scene_node.node_key,
    #             scenario_node_name=scene_node.scenario_node.name,
    #             parent_key=scene_node.parent.node_key if scene_node.parent else None,
    #             server_address=server_address
    #         )
    #         return node_info

    # def get_process_pool_status(self) -> dict:
    #     with self.lock:
    #         self._cleanup_finished_processes()
    #         running_nodes = []
    #         for node_name, node_info in self.process_pool.items():
    #             process = node_info.process
    #             status = 'running' if process and process.poll() is None else 'stopped'
    #             running_nodes.append({
    #                 'status': status,
    #                 'name': node_name,
    #                 'address': node_info.address,
    #                 'template': node_info.scenario_node_name,
    #                 'uptime': time.time() - node_info.start_time
    #             })
            
    #         return {
    #             'nodes': running_nodes,
    #         }
    
    # def trigger(self, node_key: str, proxy_crm_class: Type[T]) -> T | None:
    #     """Can only be used in the same thread by other CRM"""
    #     # TODO: make icrm proxy for remote CRM
    #     # TODO: NOT ROBUST for icrm proxy
    #     is_crm = proxy_crm_class.direction == '<-'
        
    #     with self.lock:
    #         if not self._node_exists_in_db(node_key):
    #             return None
    #         else:
    #             node = self._load_node_from_db(node_key)
    #             if not node:
    #                 return None
    #             if is_crm:
    #                 return proxy_crm_class(**node.launch_params)
    #             else:
    #                 address = self.activate_node(node_key, ReuseAction.KEEP, CRMDuration.Forever)
    #                 client = cc.rpc.Client(address)
    #                 proxy = proxy_crm_class()
    #                 proxy.client = client
    #                 proxy.close = lambda: client.close()
    #                 return proxy