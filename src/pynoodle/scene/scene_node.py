import os
import sys
import time
import json
import subprocess
import c_two as cc
from pathlib import Path
from pydantic import BaseModel
from dataclasses import dataclass, field
from typing import TypeVar, Type, Generic, Literal

from .lock import RWLock
from ..config import settings
from ..scenario import ScenarioNode
from .server_template import CRM_LAUNCHER_IMPORT_TEMPLATE, CRM_LAUNCHER_RUNNING_TEMPLATE

T = TypeVar('T')

SERVING_TABLE = 'serving'
NODE_KEY = 'node_key'
CONNECTION_COUNT = 'connection_count'

class NodeMessage(BaseModel):
    sender_id: str
    action: str

@dataclass
class SceneNodeRecord:
    node_key: str
    scenario_node: ScenarioNode | None   # None if this is a resource set node, not a resource node
    launch_params: str | None = None
    access_info: str | None = None
    
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
        return self.scenario_node is None

class SceneNode(Generic[T]):
    def __init__(
        self,
        icrm_class: Type[T], record: SceneNodeRecord,
        access_mode: Literal['lr', 'lw', 'pr', 'pw'],
        timeout: float | None = None, retry_interval: float = 1.0
    ):
        access_level = access_mode[0]
        lock_type = access_mode[1]
        
        if lock_type not in ['r', 'w']:
            raise ValueError("lock type must be either 'r' for read or 'w' for write")
        if access_level not in ['l', 'p']:
            raise ValueError("access level must be either 'l' for local or 'p' for process-level")
        
        self.node_key = record.node_key
        
        self._crm: T = None
        self._access_level = access_level
        self._crm_params = record.launch_params
        self._crm_class = record.scenario_node.crm_class
        self._crm_module = record.scenario_node.crm_module
        self.lock = RWLock(self.node_key, lock_type, timeout, retry_interval)
    
    @property
    def server_scheme(self) -> Literal['local', 'memory']:
        if self._access_level == 'l':
            return 'local'
        elif self._access_level == 'p':
            return 'memory'
        else:
            raise ValueError(f'Unknown access mode: {self._access_level}')
    
    @property
    def server_address(self) -> str:
        scheme = ''
        if self._access_level == 'l':
            scheme = 'local://'
        elif self._access_level == 'p':
            scheme = 'memory://'
        return scheme + self.node_key.replace('.', '_')
    
    def launch_crm_server(self):
        import_script = f'from {self._crm_module} import {self._crm_class.__name__} as CRM\n'
        scripts = CRM_LAUNCHER_IMPORT_TEMPLATE + import_script + CRM_LAUNCHER_RUNNING_TEMPLATE
        
        # Try to launch a CRM server (Process-Level) related to the node
        try:
            # Platform-specific subprocess arguments
            kwargs = {}
            if sys.platform != 'win32':
                # Unix-specific: create new process group
                kwargs['preexec_fn'] = os.setsid
            
            cmd = [
                sys.executable,
                '-c',
                scripts,
                '--server_address', self.server_address,
                '--node_key', self.node_key,
                '--params', self._crm_params
            ]
        
            subprocess.Popen(
                cmd,
                **kwargs,
            )
        
        except Exception as e:
            raise RuntimeError(f'Failed to launch CRM server for node "{self.node_key}": {e}')
        
    @property
    def crm(self) -> T:
        if self._crm is not None:
            return self._crm
        
        self.lock.acquire()
        
        if self._access_level == 'l':
            params = json.loads(self._crm_params) if self._crm_params else {}
            self._crm = self._crm_class(**params)
            return self._crm
        
        elif self._access_level == 'p':
            self.launch_crm_server()
            
            # Spining up the CRM server, wait for it to be ready
            count = 0
            while cc.rpc.Client.ping(self.server_address, 0.5) is False:
                if count >= 120: # 60 seconds timeout
                    raise TimeoutError(f'CRM server "{self.node_key}" did not start in time')
                time.sleep(0.5)
                count += 1
            
            # Create an ICRM instance related to the node CRM
            self._crm = self._crm_class.__base__()
            
            # Add a C-Two RPC client
            client = cc.rpc.Client(self.server_address)
            self._crm.client = client
            return self._crm
    
    def terminate(self):
        # For Local-level CRM, terminate it manually
        if self._access_level == 'l':
            self._crm.terminate()
        
        # For Process-level CRM, just shutdown the C-Two client
        elif self._access_level == 'p':
            self._crm.client.shutdown(self.server_address)
            
        # Release the lock
        self.lock.release()