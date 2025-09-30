import logging
from fastapi import APIRouter, HTTPException

from ..noodle import noodle
from ..node.lock import RWLock
from ..schemas.lock import LockedInfo
from ..schemas.node import ResourceNodeInfo

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get('/', response_model=ResourceNodeInfo)
def get_node_info(node_key: str, child_start_index: int = 0, child_end_index: int = None):
    try:
        node_info = noodle.get_node_info(node_key, child_start_index, child_end_index)
        if not node_info:
            raise HTTPException(status_code=404, detail='Node not found')
        return node_info
    except Exception as e:
        logger.error(f'Error fetching node info: {e}')
        raise HTTPException(status_code=500, detail='Internal Server Error')

@router.get('/lock')
def is_node_locked(node_key: str, lock_id: str):
    try:
        node = noodle.get_node_info(node_key)
        if not node:
            raise HTTPException(status_code=404, detail='Node not found')
        return LockedInfo(locked=RWLock.has_lock(lock_id))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error checking node lock: {e}')