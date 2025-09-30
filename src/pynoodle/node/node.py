import os
import sys
import time
import json
import requests
import subprocess
import c_two as cc
from pydantic import BaseModel
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from typing import TypeVar, Type, Generic, Literal

from .lock import RWLock
from ..schemas.lock import LockInfo
from ..module_cache import ResourceNodeTemplateModule
from .template import CRM_LAUNCHER_IMPORT_TEMPLATE, CRM_LAUNCHER_RUNNING_TEMPLATE

T = TypeVar('T')

SERVING_TABLE = 'serving'
NODE_KEY = 'node_key'
CONNECTION_COUNT = 'connection_count'

class NodeMessage(BaseModel):
    sender_id: str
    action: str

@dataclass
class ResourceNodeRecord:
    node_key: str
    access_info: str | None = None
    launch_params: str | None = None
    template: ResourceNodeTemplateModule | None = None   # None if this is a resource set node, not a resource node
    
    parent_key: str | None = None
    children: list['ResourceNodeRecord'] = field(default_factory=list)
    
    def add_child(self, child: 'ResourceNodeRecord'):
        self.children.append(child)
        self.children.sort(key=lambda child: child.node_key.split('.')[-1].lower())  # sort children by their name
        child.parent_key = self.node_key
    
    def add_children(self, children: list['ResourceNodeRecord']):
        for child in children:
            self.add_child(child)
    
    @property
    def has_children(self):
        return len(self.children) > 0

class IResourceNode(Generic[T], metaclass=ABCMeta):
    @property
    @abstractmethod
    def lock(self) -> RWLock:
        raise NotImplementedError

    @property
    @abstractmethod
    def node_key(self) -> str:
        raise NotImplementedError
    
    @property
    @abstractmethod
    def server_scheme(self) -> Literal['local', 'memory', 'http']:
        """Return the scheme of the server address"""
        raise NotImplementedError
    
    @property
    @abstractmethod
    def server_address(self) -> str:
        """Return the server address of the node"""
        raise NotImplementedError
    
    @property
    @abstractmethod
    def crm(self) -> T:
        """Return the CRM instance of the node"""
        raise NotImplementedError
    
    @property
    @abstractmethod
    def lock_id(self) -> str:
        """The ID of the lock associated with this node"""
        raise NotImplementedError
    
    @abstractmethod
    def terminate(self):
        """Terminate the CRM server and release resources"""
        raise NotImplementedError
        
class ResourceNode(IResourceNode[T]):
    def __init__(
        self,
        icrm_class: Type[T],
        record: ResourceNodeRecord,
        access_mode: Literal['lr', 'lw', 'pr', 'pw'],
        timeout: float | None = None, retry_interval: float = 0.1, activate_at_once: bool = True
    ):
        super().__init__()
        
        access_level = access_mode[0]
        lock_type = access_mode[1]
        
        if lock_type not in ['r', 'w']:
            raise ValueError("lock type must be either 'r' for read or 'w' for write")
        if access_level not in ['l', 'p']:
            raise ValueError("access level must be either 'l' for local or 'p' for process-level")
        
        self._node_key = record.node_key

        self._crm: T = None
        self._icrm_class = icrm_class
        self._crm_class = record.template.crm
        self._crm_params = record.launch_params
        
        self._access_level = access_level
        self._lock = RWLock(self._node_key, access_mode, timeout, retry_interval)
        self._import_script = f'from {record.template.module_path} import template\n'

        if activate_at_once:
            self.activate_memory_server()

    @property
    def lock(self) -> RWLock:
        return self._lock
    
    @property
    def lock_id(self) -> str:
        return self._lock.id
    
    @property
    def node_key(self) -> str:
        return self._node_key
    
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
        return scheme + self._node_key.replace('.', '_') + f'_{self._lock.id}'
    
    def activate_memory_server(self):
        # Acquire the lock for the node
        self._lock.acquire()

        if self._access_level == 'l':
            params = json.loads(self._crm_params) if self._crm_params else {}
            self._crm = self._crm_class(**params)
        
        elif self._access_level == 'p':
            self._launch_crm_server()
            
            # Spining up the CRM server, wait for it to be ready
            count = 0
            while cc.rpc.Client.ping(self.server_address, 0.5) is False:
                if count >= 120: # 60 seconds timeout
                    raise TimeoutError(f'CRM server "{self._node_key}" did not start in time')
                time.sleep(0.5)
                count += 1
                
            # Create an ICRM instance related to the node CRM
            self._crm = self._icrm_class()
            
            # Add a C-Two RPC client
            client = cc.rpc.Client(self.server_address)
            self._crm.client = client
    
    def _launch_crm_server(self):
        scripts = CRM_LAUNCHER_IMPORT_TEMPLATE + self._import_script + CRM_LAUNCHER_RUNNING_TEMPLATE
        
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
                '--icrm_tag', self._icrm_class.__tag__,
                '--server_address', self.server_address,
                '--node_key', self._node_key,
                '--params', self._crm_params
            ]
        
            subprocess.Popen(
                cmd,
                **kwargs,
            )
        
        except Exception as e:
            raise RuntimeError(f'Failed to launch CRM server for node "{self._node_key}": {e}')
        
    @property
    def crm(self) -> T:
        return self._crm
    
    def terminate(self):
        if not self._crm is None:
            # For Local-level CRM, terminate it manually
            if self._access_level == 'l':
                self._crm.terminate()
            
            # For Process-level CRM, just shutdown the C-Two client
            elif self._access_level == 'p':
                self._crm.client.terminate()
                cc.rpc.Client.shutdown(self.server_address, -1.0)
            
        # Release the lock
        self._lock.release()

class RemoteResourceNode(IResourceNode[T]):
    def __init__(
        self,
        icrm_class: Type[T], access_info: str,
        access_mode: Literal['lr', 'lw', 'pr', 'pw'],
        timeout: float | None = None, retry_interval: float = 0.1
    ):
        super().__init__()
        
        access_level = access_mode[0]
        lock_type = access_mode[1]
        
        if lock_type not in ['r', 'w']:
            raise ValueError("lock type must be either 'r' for read or 'w' for write")
        if access_level not in ['l', 'p']:
            raise ValueError("access level must be either 'l' for local or 'p' for process-level")
        
        self._node_key = access_info
        self._remote_url, self._remote_key = access_info.split('::')
        
        self._crm: T = None
        self._icrm_class = icrm_class
        
        self._timeout = timeout
        self._lock_type = lock_type
        self._access_level = access_level
        self._retry_interval = retry_interval
        
        # Get the remote lock from the remote Noodle
        # Refer to activate_node() in src/pynoodle/endpoints/proxy.py for more details about the lock API
        lock_api = (
            f'{self.server_address}&icrm_tag={self._icrm_class.__tag__}&lock_type={self._lock_type}&retry_interval={self._retry_interval}' \
            + (f'&timeout={self._timeout}' if self._timeout is not None else '')
        )
        response = requests.get(lock_api)
        if response.status_code != 200:
            raise RuntimeError(f'Failed to acquire lock for remote CRM server: {response.text}')
        self._remote_lock_id = LockInfo(**response.json()).lock_id
            
        # Create an ICRM instance related to the remote node CRM
        # Refer to proxy_node() in src/pynoodle/endpoints/proxy.py for more details about the proxy API
        proxy_api = f'{self.server_address}&lock_id={self._remote_lock_id}'  # can add &timeout=[-1.0 | <timeout>] if needed
        self._crm = self._icrm_class()
        
        # Add a C-Two RPC client
        client = cc.rpc.Client(proxy_api)
        self._crm.client = client
    
    @property
    def lock(self) -> RWLock:
        return None  # remote nodes do not have a local lock, they use remote locks
    
    @property
    def lock_id(self) -> str:
        return self._remote_lock_id
    
    @property
    def node_key(self) -> str:
        return self._node_key
    
    @property
    def server_scheme(self) -> Literal['http']:
        return 'http'
    
    @property
    def server_address(self) -> str:
        # Refer to activate_node(), proxy_node() and deactivate_node() in src/pynoodle/endpoints/proxy.py
        # For more details about the server_address format
        return f'{self._remote_url}/noodle/proxy/?node_key={self._remote_key}'
    
    @property
    def crm(self) -> T:
        return self._crm
    
    def terminate(self):
        self._crm.client.terminate()
        
        # Refer to deactivate_node() in src/pynoodle/endpoints/proxy.py for more details about the deactivate API
        deactivate_api = f'{self.server_address}&lock_id={self._remote_lock_id}'
        response = requests.delete(deactivate_api)
        if response.status_code != 200:
            raise RuntimeError(f'Failed to deactivate remote CRM server: {response.text}')

class RemoteResourceNodeProxy(ResourceNode[T]):
    def __init__(
        self,
        icrm_class: Type[T], record: ResourceNodeRecord,
        access_mode: Literal['lr', 'lw', 'pr', 'pw'],
        timeout: float | None = None, retry_interval: float = 0.1
    ):
        super().__init__(icrm_class, record, access_mode, timeout, retry_interval)
        self._remote_url, self._remote_key = record.access_info.split('::')
        self._remote_lock_id: str | None = None
        self._icrm_class: Type[T] = record.template.icrm_class

    @property
    def server_scheme(self) -> Literal['http']:
        return 'http'
    
    @property
    def lock_id(self) -> str:
        return self._remote_lock_id
    
    @property
    def server_address(self) -> str:
        # Refer to activate_node(), proxy_node() and deactivate_node() in src/pynoodle/endpoints/proxy.py
        # For more details about the server_address format
        return f'{self._remote_url}/noodle/proxy/?node_key={self._remote_key}'
    
    @property
    def crm(self) -> T:
        if self._crm is not None:
            return self._crm
        
        self._lock.acquire()

        icrm_tag = f'{self._icrm_class.__namespace__}/{self._icrm_class.__name__}/{self._icrm_class.__version__}'

        # Get the twin lock from the remote Noodle
        # Refer to activate_node() in src/pynoodle/endpoints/proxy.py for more details about the lock API
        lock_api = (
            f'{self.server_address}&icrm_tag={icrm_tag}&lock_type={self._lock.lock_type}&retry_interval={self._lock.retry_interval}' \
            + (f'&timeout={self._lock.timeout}' if self._lock.timeout is not None else '')
        )
        response = requests.get(lock_api)
        if response.status_code != 200:
            raise RuntimeError(f'Failed to acquire lock for remote CRM server: {response.text}')
        self._remote_lock_id = LockInfo(**response.json()).lock_id
        
        # Create an ICRM instance related to the remote node CRM
        # Refer to proxy_node() in src/pynoodle/endpoints/proxy.py for more details about the proxy API
        proxy_api = f'{self.server_address}&lock_id={self._remote_lock_id}'  # can add &timeout=[-1.0 | <timeout>] if needed
        self._crm = self._icrm_class()
        
        # Add a C-Two RPC client
        client = cc.rpc.Client(proxy_api)
        self._crm.client = client
        return self._crm
    
    def terminate(self):
        self._crm.client.terminate()
        
        # Refer to deactivate_node() in src/pynoodle/endpoints/proxy.py for more details about the deactivate API
        deactivate_api = f'{self.server_address}&lock_id={self._remote_lock_id}'
        response = requests.delete(deactivate_api)
        if response.status_code != 200:
            raise RuntimeError(f'Failed to deactivate remote CRM server: {response.text}')
        self._lock.release()