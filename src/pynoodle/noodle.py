import sqlite3
import logging
from typing import Literal

from .config import settings
from .node.treeger import Treeger, NODE_TABLE, NODE_KEY

logger = logging.getLogger(__name__)

class Noodle(Treeger):
    def __init__(self):
        super().__init__()
    
    def has_node(self, node_key: str) -> bool:
        """Check if a node exists in the resource tree."""
        with sqlite3.connect(settings.SQLITE_PATH) as conn:
            cursor = conn.execute(f'SELECT 1 FROM {NODE_TABLE} WHERE {NODE_KEY} = ?', (node_key,))
            return cursor.fetchone() is not None
    
    def node_server_address(self, node_key: str, lock_id: str, access_level: Literal['l', 'p']) -> str:
        """Get the server address for a node based on its access level."""
        scheme = ''
        if access_level == 'l':
            scheme = 'local://'
        elif access_level == 'p':
            scheme = 'memory://'
        return scheme + node_key.replace('.', '_') + f'_{lock_id}'

noodle = Noodle()