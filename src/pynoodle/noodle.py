import sqlite3
import logging
from typing import Literal

from .config import settings
from .scene.treeger import Treeger, SCENE_TABLE, NODE_KEY

logger = logging.getLogger(__name__)

class Noodle(Treeger):
    def __init__(self):
        super().__init__()
    
    @staticmethod
    def has_node(node_key: str) -> bool:
        """Check if a node exists in the scene database."""
        with sqlite3.connect(settings.SQLITE_PATH) as conn:
            cursor = conn.execute(f'SELECT 1 FROM {SCENE_TABLE} WHERE {NODE_KEY} = ?', (node_key,))
            return cursor.fetchone() is not None
    
    @staticmethod
    def node_server_address(node_key: str, access_level: Literal['l', 'p']) -> str:
        """Get the server address for a node based on its access level."""
        scheme = ''
        if access_level == 'l':
            scheme = 'local://'
        elif access_level == 'p':
            scheme = 'memory://'
        return scheme + node_key.replace('.', '_')

noodle = Noodle()