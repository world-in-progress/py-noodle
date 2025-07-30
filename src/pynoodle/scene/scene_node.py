import os
import sys
import time
import json
import requests
import subprocess
import c_two as cc
from pydantic import BaseModel
from dataclasses import dataclass, field
from typing import TypeVar, Type, Generic, Literal

from .lock import RWLock
from ..scenario import ScenarioNode
from ..schemas.lock import LockInfo
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

class SceneNodeProxy(Generic[T]):
    def __init__(
        self,
        icrm_class: Type[T], access_info: str,
        access_mode: Literal['lr', 'lw', 'pr', 'pw'],
        timeout: float | None = None, retry_interval: float = 0.1
    ):
        remote_url, remote_key = access_info.split('::')
        self._icrm_class = icrm_class
        self._remote_url = remote_url
        self._remote_key = remote_key
        self._remote_lock_id: str | None = None
        self._lock_type = access_mode[1]
        self._timeout = timeout
        self._retry_interval = retry_interval
        self._crm: T = None
    
    @property
    def crm(self) -> T:
        if self._crm is not None:
            return self._crm
        
        # Get the remote lock from the remote Noodle
        # Refer to activate_node() in src/pynoodle/endpoints/proxy.py for more details about the lock API
        lock_api = (
            f'{self._remote_url}/noodle/proxy?node_key={self._remote_key}&lock_type={self._lock_type}&retry_interval={self._retry_interval}' \
            + (f'&timeout={self._timeout}' if self._timeout is not None else '')
        )
        response = requests.get(lock_api)
        if response.status_code != 200:
            raise RuntimeError(f'Failed to acquire lock for remote CRM server: {response.text}')
        self._remote_lock_id = LockInfo(**response.json()).lock_id
        
        # Create an ICRM instance related to the remote node CRM
        # Refer to proxy_node() in src/pynoodle/endpoints/proxy.py for more details about the proxy API
        proxy_api = f'{self._remote_url}/noodle/proxy?node_key={self._remote_key}'  # can add &timeout=[-1.0 | <timeout>] if needed
        self._crm = self._icrm_class()
        
        # Add a C-Two RPC client
        client = cc.rpc.Client(proxy_api)
        self._crm.client = client
        return self._crm
    
    def terminate(self):
        # Refer to deactivate_node() in src/pynoodle/endpoints/proxy.py for more details about the deactivate API
        deactivate_api = f'{self._remote_url}/noodle/proxy?node_key={self._remote_key}&lock_id={self._remote_lock_id}'
        response = requests.delete(deactivate_api)
        if response.status_code != 200:
            raise RuntimeError(f'Failed to deactivate remote CRM server: {response.text}')
        return
        

class SceneNode(Generic[T]):
    def __init__(
        self,
        icrm_class: Type[T], record: SceneNodeRecord,   # icrm_class only used for type hinting
        access_mode: Literal['lr', 'lw', 'pr', 'pw'],
        timeout: float | None = None, retry_interval: float = 0.1
    ):
        access_level = access_mode[0]
        lock_type = access_mode[1]
        
        if lock_type not in ['r', 'w']:
            raise ValueError("lock type must be either 'r' for read or 'w' for write")
        if access_level not in ['l', 'p']:
            raise ValueError("access level must be either 'l' for local or 'p' for process-level")
        
        self.node_key = record.node_key
        self._remote_url: str | None = None
        self._remote_key: str | None = None
        self._remote_lock_id: str | None = None
        
        # If this node has a remote URL and key
        # It has remote access information
        if record.access_info:
            self._remote_url, self._remote_key = record.access_info.split('::')
        
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
        
        is_proxy = self._remote_url is not None and self._remote_key is not None
        if is_proxy:
            # Get the twin lock from the remote Noodle
            # Refer to activate_node() in src/pynoodle/endpoints/proxy.py for more details about the lock API
            lock_api = (
                f'{self._remote_url}/noodle/proxy?node_key={self._remote_key}&lock_type={self.lock.lock_type}&retry_interval={self.lock.retry_interval}' \
                + (f'&timeout={self.lock.timeout}' if self.lock.timeout is not None else '')
            )
            response = requests.get(lock_api)
            if response.status_code != 200:
                raise RuntimeError(f'Failed to acquire lock for remote CRM server: {response.text}')
            self._remote_lock_id = LockInfo(**response.json()).lock_id
            
            # Create an ICRM instance related to the remote node CRM
            # Refer to proxy_node() in src/pynoodle/endpoints/proxy.py for more details about the proxy API
            proxy_api = f'{self._remote_url}/noodle/proxy?node_key={self._remote_key}'  # can add &timeout=[-1.0 | <timeout>] if needed
            self._crm = self._crm_class.__base__()
            
            # Add a C-Two RPC client
            client = cc.rpc.Client(proxy_api)
            self._crm.client = client
            return self._crm

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
        # For Proxy CRM, terminate it remotely
        is_proxy = self._remote_url is not None and self._remote_key is not None
        if is_proxy:
            # Refer to deactivate_node() in src/pynoodle/endpoints/proxy.py for more details about the deactivate API
            deactivate_api = f'{self._remote_url}/noodle/proxy?node_key={self._remote_key}&lock_id={self._remote_lock_id}'
            response = requests.delete(deactivate_api)
            if response.status_code != 200:
                raise RuntimeError(f'Failed to deactivate remote CRM server: {response.text}')
            self.lock.release()
            return
        
        # For Local-level CRM, terminate it manually
        if self._access_level == 'l':
            self._crm.terminate()
        
        # For Process-level CRM, just shutdown the C-Two client
        elif self._access_level == 'p':
            self._crm.client.shutdown(self.server_address, -1.0)
            
        # Release the lock
        self.lock.release()