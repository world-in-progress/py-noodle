import yaml
import logging
import threading
from pathlib import Path
from fastapi import APIRouter
from dataclasses import dataclass
from typing import TypeVar, Type, Callable

from .config import settings
from .schemas.scenario import ScenarioConfiguration

T = TypeVar('T')
logger = logging.getLogger(__name__)

@dataclass
class ScenarioNode:
    name: str
    module: str
    dependencies: list['ScenarioNode']
    
    _crm_class: Type[T] = None
    _icrm_class: Type[T] = None
    _endpoint: APIRouter = None
    _mount: Callable[[str], None] = None
    _unmount: Callable[[str], None] = None
    _lock: threading.Lock = threading.Lock()
    _params_converter: Callable[[str, dict | None], dict | None] = None
    
    def _load_from_module(self):
        m = __import__(self.module, fromlist=[''])
        self._icrm_class = getattr(m, 'ICRM', None)
        if not self._icrm_class:
            raise ImportError(f'ICRM class not found in module {self.module}')
        
        self._crm_class = getattr(m, 'CRM', None)
        if not self._crm_class:
            self._crm_class = self._icrm_class

        self._mount = getattr(m, 'MOUNT', None)
        if not self._mount:
            # Use an empty callable if MOUNT is not defined (no warning, because it might be optional)
            self._mount = lambda x: None

        self._unmount = getattr(m, 'UNMOUNT', None)
        if not self._unmount:
            # Use an empty callable if UNMOUNT is not defined (no warning, because it might be optional)
            self._unmount = lambda x: None
        
        self._params_converter = getattr(m, 'PARAM_CONVERTER', None)
        if not self._params_converter:
            # Use an empty callable if PARAM_CONVERTER is not defined (no warning, because it might be optional)
            self._params_converter = lambda x, y: y
        
        self._endpoint = getattr(m, 'router', None)
    
    @property
    def crm_class(self) -> Type[T]:
        with self._lock:
            if self._crm_class is None:
                self._load_from_module()
            return self._crm_class
    
    @property
    def icrm_class(self) -> Type[T]:
        with self._lock:
            if self._icrm_class is None:
                self._load_from_module()
            return self._icrm_class
    
    @property
    def mount(self) -> Callable[[str], None]:
        with self._lock:
            if self._mount is None:
                self._load_from_module()
            return self._mount
    
    @property
    def unmount(self) -> Callable[[str], None]:
        with self._lock:
            if self._unmount is None:
                self._load_from_module()
            return self._unmount
        
    @property
    def params_converter(self) -> Callable[[str, dict | None], dict | None]:
        with self._lock:
            if self._params_converter is None:
                self._load_from_module()
            return self._params_converter

    @property
    def icrm_name(self) -> str:
        return self._icrm_class.__name__
    
    @property
    def namespace(self) -> str:
        return self.icrm_class.__namespace__
    
    @property
    def icrm_tag(self) -> str:
        return f'{self.namespace}/{self.icrm_name}'
    
    @property
    def endpoint(self) -> APIRouter | None:
        with self._lock:
            if self._endpoint is None:
                self._load_from_module()
            return self._endpoint

class Scenario:
    def __init__(self):
        # Read configuration
        configuration_path = Path(settings.NOODLE_CONFIG_PATH)
        if not configuration_path.is_absolute():
            configuration_path = Path.cwd() / configuration_path
        with open(configuration_path, 'r') as f:
            config_data = yaml.safe_load(f)
            
        # Parse scenario graph
        config = ScenarioConfiguration(**config_data)
        
        self.graph: dict[str, ScenarioNode] = {}
        
        # - Firstly, create all nodes
        module_root = f'{config.module_root}.' if config.module_root else ''
        for node_description in config.scenario_nodes:
            node = ScenarioNode(
                name=node_description.name,
                module=module_root + node_description.name,
                dependencies=[]
            )
            self.graph[node_description.name] = node

        # - Secondly, resolve dependencies
        for node_description in config.scenario_nodes:
            node = self.graph[node_description.name]
            if node_description.dependencies:
                node.dependencies = [
                    self.graph[dep_name]
                    for dep_name in node_description.dependencies
                ]
    
    def __getitem__(self, scenario_node_name: str) -> ScenarioNode | None:
        if scenario_node_name not in self.graph:
            return None
        return self.graph[scenario_node_name]
    
    def __iter__(self):
        return iter(self.graph.values())

    def get_icrm_tag(self, scenario_node_name: str) -> str | None:
        node = self.graph.get(scenario_node_name)
        if not node:
            return None
        return node.icrm_tag

scenario_graph = Scenario()