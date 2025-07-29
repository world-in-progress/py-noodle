import logging
from .scene import Treeger
from .scenario import Scenario

logger = logging.getLogger(__name__)

class Noodle(Treeger):
    def __init__(self):
        super().__init__(Scenario())