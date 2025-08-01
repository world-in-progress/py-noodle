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
    crm_name: str
    crm_module: str
    dependencies: list['ScenarioNode']
    
    _crm_class: Type[T] = None
    _lock: threading.Lock = threading.Lock()
    
    @property
    def namespace(self) -> str:
        return self.name.split('/')[0]
    
    @property
    def icrm_name(self) -> str:
        return self.name.split('/')[1]
    
    @property
    def icrm_class(self) -> Type[T]:
        if self._crm_class.direction == '->':
            return self._crm_class
        else:
            return self._crm_class.__base__
    
    @property
    def crm_class(self) -> Type[T]:
        with self._lock:
            if self._crm_class is None:
                module = __import__(self.crm_module, fromlist=[''])
                self._crm_class = getattr(module, self.crm_name)
            return self._crm_class

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
        for node_description in config.scenario_nodes:
            node = ScenarioNode(
                name=node_description.name,
                crm_name=node_description.crm_name,
                crm_module=node_description.crm_module,
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

scenario_graph = Scenario()