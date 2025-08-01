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
async def activate_node(node_key: str, scenario_node_name: str, lock_type: Literal['r', 'w'], timeout: float | None = None, retry_interval: float = 1.0):
    """
    Activates a node in the Noodle system.
    """
    try:
        # Try to get scenario node
        scenario_node = noodle.scenario.graph[scenario_node_name]
        if not scenario_node:
            namespace, icrm_class_name = scenario_node_name.split('/')
            raise HTTPException(status_code=404, detail=f'ICRM class {icrm_class_name} not found in namespace {namespace}')
    
        # Get the node
        node = noodle.get_node(scenario_node.icrm_class, node_key, 'p' + lock_type, timeout, retry_interval)

        # Acquire the lock for the node asynchronously
        lock = node._lock
        await lock.async_acquire()
        
        # Launch the node CRM server at process level
        node.launch_crm_server()

        # Spin up the CRM server, asynchronously wait for it to be ready
        count = 0
        while cc.rpc.Client.ping(node.server_address, 0.5) is False:
            if (timeout is not None) and count >= timeout * 2:
                raise TimeoutError(f'CRM server "{node._node_key}" did not start in time')
            await asyncio.sleep(0.5)
            count += 1
        
        return LockInfo(lock_id=lock.id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error activating node {node_key}: {e}')

@router.post('/')
async def proxy_node(node_key: str, timeout: float | None = None, body: bytes=Body(..., description='C-Two Event Message in Bytes')):
    """
    Proxies a C-Two event message to the specified node.
    """
    try:
        # Check if the node exists
        if not noodle.has_node(node_key):
            raise HTTPException(status_code=404, detail=f'Node {node_key} not found')
        
        # Relay the message to the node's CRM server asynchronously
        timeout = timeout if timeout is not None else -1.0
        res = await cc.rpc.routing(noodle.node_server_address(node_key, 'p'), body, timeout)
        return Response(res, media_type='application/octet-stream')
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error proxying node {node_key}: {e}')

@router.delete('/')
def deactivate_node(node_key: str, lock_id: str):
    """
    Deactivates a node in the Noodle system.
    """
    try:
        # Shutdown the node's CRM server
        cc.rpc.Client.shutdown(noodle.node_server_address(node_key, 'p'), -1.0)
        
        # Release a local lock (mock as a write lock)
        lock = RWLock(node_key, 'w')
        lock.id = lock_id
        lock.release()
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error deactivating node {node_key}: {e}')