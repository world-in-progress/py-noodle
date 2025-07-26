import logging
from typing import TypeVar
from .scenario import Scenario

T = TypeVar('T')
logger = logging.getLogger(__name__)

class Noodle:
    def __init__(self):
        # Create scenario graph
        self.scenario = Scenario()
        
        