import yaml
import logging
import threading
from pathlib import Path
from typing import TypeVar, Type
from dataclasses import dataclass

from ..config import settings
from ..schemas.scenario import ScenarioConfiguration

T = TypeVar('T')
logger = logging.getLogger(__name__)

@dataclass
class ScenarioNode:
    name: str
    module: str
    dependencies: list['ScenarioNode']
    
    _crm_class: Type[T] = None
    _lock: threading.Lock = threading.Lock()
    
    @property
    def crm_class(self) -> Type[T]:
        with self._lock:
            if self._crm_class is None:
                m = __import__(self.module, fromlist=[''])
                self._crm_class = getattr(m, 'CRM', None)
                if not self._crm_class:
                    self._crm_class = getattr(m, 'ICRM', None)
                if not self._crm_class:
                    raise ImportError(f'ICRM class not found in module {self.module}')
            return self._crm_class
    
    @property
    def icrm_name(self) -> str:
        if self._crm_class.direction == '->':
            return self._crm_class.__name__
        else:
            return self._crm_class.__base__.__name__
    
    @property
    def icrm_class(self) -> Type[T]:
        if self._crm_class.direction == '->':
            return self._crm_class
        else:
            return self._crm_class.__base__
    
    @property
    def namespace(self) -> str:
        return self.icrm_class.__namespace__
    
    @property
    def icrm_tag(self) -> str:
        return f'{self.namespace}/{self.icrm_name}'

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

    def get_icrm_tag(self, scenario_node_name: str) -> str | None:
        node = self.graph.get(scenario_node_name)
        if not node:
            return None
        return node.icrm_tag

scenario_graph = Scenario()