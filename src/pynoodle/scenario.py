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
class RawScenarioNode:
    CRM: Type[T] = None
    ICRM: Type[T] = None
    MOUNT: Callable[[str], None] = lambda x: None
    UNMOUNT: Callable[[str], None] = lambda x: None
    PARAM_CONVERTER: Callable[[str, dict | None], dict | None] = lambda x, y: y
    ENDPOINT: APIRouter | None = None
    
    def __post_init__(self):
        if not self.ICRM:
            raise ImportError(f'ICRM class not found in module {self.module}')
        if not self.CRM:
            self.CRM = self.ICRM

@dataclass
class ScenarioNode:
    name: str
    module: str
    dependencies: list['ScenarioNode']
    
    _lock: threading.Lock = threading.Lock()
    
    _crm_class: Type[T] = None
    _icrm_class: Type[T] = None
    _endpoint: APIRouter = None
    _mount: Callable[[str], None] = None
    _unmount: Callable[[str], None] = None
    _params_converter: Callable[[str, dict | None], dict | None] = None
    
    def _load_from_module(self):
        m = __import__(self.module, fromlist=[''])
        raw: RawScenarioNode = getattr(m, 'RAW', None)
        if not raw:
            raise ImportError(f'RawScenarioNode class not found in module {self.module}')
        
        self._mount = raw.MOUNT
        self._crm_class = raw.CRM
        self._icrm_class = raw.ICRM
        self._unmount = raw.UNMOUNT
        self._endpoint = raw.ENDPOINT
        self._params_converter = raw.PARAM_CONVERTER
    
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
    def endpoint(self) -> APIRouter | None:
        with self._lock:
            if self._endpoint is None:
                self._load_from_module()
            return self._endpoint
    
    @property
    def icrm_tag(self) -> str:
        cls = self.icrm_class
        
        name = cls.__name__
        version = cls.__version__
        namespace = cls.__namespace__
        
        if not namespace or not name or not version:
            raise ValueError(f'ICRM class {cls.__name__} is missing namespace, name, or version attributes.')
        
        return f'{namespace}/{name}/{version}'

class Scenario:
    def __init__(self):
        # Read configuration
        with open(settings.NOODLE_CONFIG_PATH, 'r') as f:
            config_data = yaml.safe_load(f)
            
        # Parse scenario graph
        config = ScenarioConfiguration(**config_data)
        
        self.graph: dict[str, ScenarioNode] = {}
        
        # - Firstly, create all nodes
        module_prefix = f'{config.module_prefix}.' if config.module_prefix else ''
        module_postfix = f'{config.module_postfix}' if config.module_postfix else ''
        for node_description in config.scenario_nodes:
            node = ScenarioNode(
                name=node_description.name,
                module=module_prefix + node_description.name + module_postfix,
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