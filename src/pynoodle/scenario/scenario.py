import yaml
import logging
from pathlib import Path
from typing import TypeVar
from dataclasses import dataclass

from ..config import settings
from .schemas import ScenarioConfiguration, ScenarioNodeDescription

T = TypeVar('T')
logger = logging.getLogger(__name__)

@dataclass
class ScenarioNode:
    name: str
    crm_class: T
    crm_module: str
    dependencies: list['ScenarioNode']
    
    @property
    def namespace(self) -> str:
        return self.name.split('/')[0]
    
    @property
    def crm_name(self) -> str:
        return self.crm_class.__name__
    
    @property
    def icrm_name(self) -> str:
        return self.crm_class.__base__.__name__
    
    @property
    def icrm_class(self) -> T:
        return self.crm_class.__base__

class Scenario:
    def __init__(self):
        # Read configuration
        configuration_path = Path(settings.NOODLE_CONFIG_PATH)
        if not configuration_path.is_absolute():
            configuration_path = Path.cwd() / configuration_path
        with open(configuration_path, 'r') as f:
            config_data = yaml.safe_load(f)
        config = ScenarioConfiguration(**config_data)
        
        # Parse scene path
        self.scene_path = Path(config.scene_path)
        if not self.scene_path.is_absolute():
            self.scene_path = Path.cwd() / self.scene_path
        
        # Parse scenario graph
        self.graph: dict[str, ScenarioNode] = {}
        
        # - Firstly, create all nodes
        for node_description in config.scenario_nodes:
            module = __import__(node_description.crm_module, fromlist=[''])
            crm_class = getattr(module, node_description.crm_name)
            node = ScenarioNode(
                name=node_description.name,
                crm_class=crm_class,
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