import asyncio
import logging
import c_two as cc
from typing import Literal
from fastapi import APIRouter, HTTPException

from ..noodle import Noodle
from ..scene.lock import RWLock
from ..schemas.lock import LockInfo

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get('/', response_model=LockInfo)
async def activate_node(node_key: str, lock_type: Literal['r', 'w'], timeout: float | None = None, retry_interval: float = 1.0):
    """
    Activates a node in the Noodle system.
    """
    try:
        # Get the node (mock icrm_class as bool)
        noodle = Noodle()
        node = noodle.get_node(bool, node_key, 'p' + lock_type, timeout, retry_interval)

        # Acquire the lock for the node asynchronously
        lock = node.lock
        await lock.async_acquire()
        
        # Launch the node CRM server at process level
        node.launch_crm_server()

        # Spin up the CRM server, asynchronously wait for it to be ready
        count = 0
        while cc.rpc.Client.ping(node.server_address, 0.5) is False:
            if (timeout is not None) and count >= timeout * 2:
                raise TimeoutError(f'CRM server "{node.node_key}" did not start in time')
            await asyncio.sleep(0.5)
            count += 1
        
        return LockInfo(lock_id=lock.id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error activating node {node_key}: {e}')

@router.delete('/')
def deactivate_node(node_key: str, lock_id: str):
    """
    Deactivates a node in the Noodle system.
    """
    try:
        # Release a local lock (as write lock)
        lock = RWLock(node_key, 'w')
        lock.id = lock_id
        lock.release()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error deactivating node {node_key}: {e}')