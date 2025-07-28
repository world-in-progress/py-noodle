import yaml
import shutil
import logging
import c_two as cc
from pathlib import Path
from typing import TypeVar

from .config import settings
from .scene import Treeger, RWLock
from .scenario import Scenario, ScenarioConfiguration

T = TypeVar('T')
logger = logging.getLogger(__name__)

icrm = cc.icrm
transferable = cc.transferable

def crm(cls: T) -> T:
    if not hasattr(cls, 'terminate'):
        raise TypeError(f'Class {cls.__name__} does not have a "terminate" method')
    
    return cc.iicrm(cls)

class Noodle(Treeger):
    def __init__(self):
        super().__init__(Scenario())
    
    @staticmethod
    def init():
        # Read configuration
        configuration_path = Path(settings.NOODLE_CONFIG_PATH)
        if not configuration_path.is_absolute():
            configuration_path = Path.cwd() / configuration_path
        with open(configuration_path, 'r') as f:
            config_data = yaml.safe_load(f)
        config = ScenarioConfiguration(**config_data)
        
        # Pre-remove all locks if configured
        if settings.PRE_REMOVE_ALL_LOCKS:
            scene_path = Path(config.scene_path)
            if not scene_path.is_absolute():
                scene_path = Path.cwd() / scene_path
            RWLock.clear_all(scene_path)

        # Pre-remove existing memory temp directory if configured
        if settings.PRE_REMOVE_MEMORY_TEMP_DIR:
            memory_temp_path = Path(settings.MEMORY_TEMP_DIR)
            if not memory_temp_path.is_absolute():
                memory_temp_path = Path.cwd() / memory_temp_path
            if memory_temp_path.exists():
                shutil.rmtree(memory_temp_path)
            
        # Create a new memory temp directory
        memory_temp_path.mkdir(parents=True, exist_ok=True)