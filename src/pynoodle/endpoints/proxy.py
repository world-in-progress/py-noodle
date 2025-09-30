import asyncio
import logging
import c_two as cc
from typing import Literal
from fastapi import APIRouter, HTTPException, Body, Response

from ..noodle import noodle
from ..node.lock import RWLock
from ..schemas.lock import LockInfo
from ..node.node import ResourceNode

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get('/', response_model=LockInfo)
async def activate_node(node_key: str, icrm_tag: str, lock_type: Literal['r', 'w'], timeout: float | None = None, retry_interval: float = 1.0):
    """
    Activate a resource node in Noodle resource tree.
    """
    try:
        # Try to get node information
        node_info = noodle.get_node_info(node_key)
        if not node_info:
            raise HTTPException(status_code=404, detail=f'Node "{node_key}" not found')
        if not node_info.template_name:
            raise HTTPException(status_code=500, detail=f'Node "{node_key}" is a resource set, cannot be activated')
        
        # Validate the ICRM tag
        is_matched, error = noodle.module_cache.match(icrm_tag, node_info.template_name)
        if not is_matched and error:
            raise HTTPException(status_code=404, detail=f'ICRM tag "{icrm_tag}" not match template "{node_info.template_name}" of node "{node_key}", reason: {error}')

        # Get the node
        icrm = noodle.module_cache.icrm_modules.get(icrm_tag).icrm
        node = ResourceNode(
            icrm,
            noodle._load_node_record(node_key, is_cascade=False),
            'p' + lock_type,
            timeout,
            retry_interval,
            activate_at_once=False # do not activate CRM server at once here, for we need to acquire the lock asynchronously to avoid blocking the event loop
        )

        # Acquire the lock for the node asynchronously
        lock = node.lock
        await lock.async_acquire()
        
        # Launch the node CRM server at process level
        node.activate_memory_server()
        server_address = node.server_address

        # Spin up the CRM server, asynchronously wait for it to be ready
        count = 0
        while cc.rpc.Client.ping(server_address, 0.1) is False:
            if (timeout is not None) and count >= timeout * 10:
                raise TimeoutError(f'CRM server "{node._node_key}" did not start in time')
            await asyncio.sleep(0.1)
            count += 1
        
        return RWLock.get_lock_info(lock.id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error activating node {node_key}: {e}')

@router.post('/')
async def proxy_node(node_key: str, lock_id: str, timeout: float | None = None, body: bytes=Body(..., description='C-Two Event Message in Bytes')):
    """
    Proxy a C-Two event message to the specified node CRM server in process level.
    """
    try:
        # Check if the lock exists
        if not RWLock.has_lock(lock_id):
            raise HTTPException(status_code=404, detail=f'Lock {lock_id} not found for node {node_key}')
        
        server_address = noodle.node_server_address(node_key, lock_id, 'p')
        
        # Relay the message to the node's CRM server asynchronously
        timeout = timeout if timeout is not None else -1.0
        res = await cc.rpc.routing(server_address, body, timeout)
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
        
        server_address = noodle.node_server_address(node_key, lock_id, 'p')
        
        # Shutdown the node's CRM server
        cc.rpc.Client.shutdown(server_address, -1.0)
        
        # Remove the local lock
        RWLock.remove_lock(lock_id)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error deactivating node {node_key}: {e}')