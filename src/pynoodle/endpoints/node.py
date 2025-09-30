import logging
from typing import Literal
from fastapi import APIRouter, HTTPException

from ..noodle import noodle
from ..node.lock import RWLock
from ..schemas.lock import LockInfo
from ..schemas.node import ResourceNodeInfo, UnlinkInfo

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

@router.get('/link', response_model=LockInfo)
def link_node(icrm_tag: str, node_key: str, access_mode: Literal['r', 'w']):
    try:
        # Try to get ICRM
        icrm_module = noodle.module_cache.icrm_modules.get(icrm_tag)
        if not icrm_module:
            raise HTTPException(status_code=404, detail=f'ICRM tag "{icrm_tag}" not found in noodle.')
        
        # Link the node
        icrm = icrm_module.icrm
        lock_id = noodle.link(icrm, node_key, access_mode)
        return RWLock.get_lock_info(lock_id)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error linking nodes: {e}')

@router.get('/unlink', response_model=UnlinkInfo)
def unlink_node(node_key: str, lock_id: str):
    try:
        success, error = noodle.unlink(node_key, lock_id)
        if not success and error:
            raise HTTPException(status_code=400, detail=error)
        return UnlinkInfo(success=True)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error unlinking node: {e}')