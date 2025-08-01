import asyncio
import logging
import c_two as cc
from typing import Literal
from fastapi import APIRouter, HTTPException, Body, Response

from ..noodle import noodle
from ..scene.lock import RWLock
from ..schemas.lock import LockInfo

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get('/', response_model=LockInfo)
async def activate_node(node_key: str, icrm_tag: str, lock_type: Literal['r', 'w'], timeout: float | None = None, retry_interval: float = 1.0):
    """
    Activates a node in the Noodle system.
    """
    try:
        # Try to get node information
        node_info = noodle.get_node_info(node_key)
        if not node_info:
            raise HTTPException(status_code=404, detail=f'Node {node_key} not found')
        
        # Validate the ICRM tag
        node_icrm_tag = noodle.scenario.get_icrm_tag(node_info.scenario_node_name)
        if node_icrm_tag != icrm_tag:
            raise HTTPException(status_code=404, detail=f'ICRM tag "{icrm_tag}" not match node "{node_key}", expected "{node_icrm_tag}"')

        # Get the node
        node = noodle.get_node(None, node_key, 'p' + lock_type, timeout, retry_interval)

        # Acquire the lock for the node asynchronously
        lock = node._lock
        await lock.async_acquire()
        
        # Launch the node CRM server at process level
        node.launch_crm_server()
        server_address = noodle.node_server_address(node_key, lock.id, 'p')

        # Spin up the CRM server, asynchronously wait for it to be ready
        count = 0
        while cc.rpc.Client.ping(server_address, 0.5) is False:
            if (timeout is not None) and count >= timeout * 2:
                raise TimeoutError(f'CRM server "{node._node_key}" did not start in time')
            await asyncio.sleep(0.5)
            count += 1
        
        return LockInfo(lock_id=lock.id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error activating node {node_key}: {e}')

@router.post('/')
async def proxy_node(node_key: str, lock_id: str, timeout: float | None = None, body: bytes=Body(..., description='C-Two Event Message in Bytes')):
    """
    Proxies a C-Two event message to the specified node.
    """
    try:
        # Check if the lock exists
        if not RWLock.has_lock(lock_id):
            raise HTTPException(status_code=404, detail=f'Lock {lock_id} not found for node {node_key}')
        
        # Relay the message to the node's CRM server asynchronously
        timeout = timeout if timeout is not None else -1.0
        res = await cc.rpc.routing(noodle.node_server_address(node_key, lock_id, 'p'), body, timeout)
        return Response(res, media_type='application/octet-stream')
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error proxying node {node_key}: {e}')

@router.delete('/')
def deactivate_node(node_key: str, lock_id: str):
    """
    Deactivates a node in the Noodle system.
    """
    try:
        # Check if the lock exists
        if not RWLock.has_lock(lock_id):
            raise HTTPException(status_code=404, detail=f'Lock {lock_id} not found for node {node_key}')
        
        # Shutdown the node's CRM server
        cc.rpc.Client.shutdown(noodle.node_server_address(node_key, lock_id, 'p'), -1.0)
        
        # Release a local lock (mock as a write lock)
        lock = RWLock(node_key, 'w')
        lock.id = lock_id
        lock.release()
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error deactivating node {node_key}: {e}')