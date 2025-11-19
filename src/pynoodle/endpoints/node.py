import base64
import httpx
import logging
import threading
from typing import Literal
from fastapi import APIRouter, HTTPException

from ..noodle import noodle
from ..config import settings
from ..node.lock import RWLock
from ..utils import get_parent_key
from ..schemas.lock import LockInfo
from ..schemas.node import ResourceNodeInfo, UnlinkInfo, PullResponse, PackingResponse, MountRequest, PushResponse

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

@router.post('/mount')
def mount_node(mount_request: MountRequest):
    """
    Mount a node
    """
    node_key = mount_request.node_key
    node_template_name = mount_request.template_name
    mount_params_string = mount_request.mount_params_string
    
    try:
        success, error = noodle.mount(node_key, node_template_name, mount_params_string)
        if not success and error:
            raise RuntimeError(error)
        
    except Exception as e:
        message = f'Error mounting node: {e}'
        logger.error(message)
        raise HTTPException(status_code=500, detail=message)

@router.post('/unmount')
def unmount_node(node_key: str):
    """
    Unmount a node
    """
    try:
        success, error = noodle.unmount(node_key)
        if not success and error:
            raise RuntimeError(error)
        
    except Exception as e:
        message = f'Error unmounting node: {e}'
        logger.error(message)
        raise HTTPException(status_code=500, detail=message)

def parse_target_resource_path(launch_params_str: str, node_key: str) -> str:
    """
    Parse the target resource path from the startup arguments.
    """
    try:
        import json
        launch_params = json.loads(launch_params_str)
        target_resource_path = launch_params.get('resource_space')
        if not target_resource_path:
            raise ValueError(f'Node "{node_key}" has no resource_space in launch parameters')
        return target_resource_path
    except json.JSONDecodeError:
        raise ValueError(f'Invalid launch parameters for node "{node_key}"')

@router.post('/push', response_model=PushResponse)
def push_node(template_name: str, source_node_key: str, target_node_key: str):
    """
    Push a node to remote resource tree.
    """
    try:
        source_node = noodle.get_node_info(source_node_key)
        if source_node is None:
            raise HTTPException(status_code=404, detail=f'Source node "{source_node_key}" not found')
        
        template = noodle.get_template(template_name)
        if template is None:
            raise HTTPException(status_code=404, detail=f'ResourceNodeTemplate "{template_name}" not found in noodle.')
        
        parent_key = get_parent_key(source_node_key)
        parent_node_info = noodle.get_node_info(parent_key)
        if not parent_node_info:
            raise HTTPException(status_code=404, detail=f'Parent of node "{source_node_key}" not found')
        
        try:
            tar_lock_key = f'{source_node_key}_tar'
            with threading.Lock():
                tar_path = settings.MEMORY_TEMP_PATH / 'push_cache' / f'{source_node_key.replace(".", "_")}.tar.gz'
                tar_path.parent.mkdir(parents=True, exist_ok=True)
                if not tar_path.exists():
                    template.pack(source_node_key, str(tar_path))
                    
                RWLock.lock_node(source_node_key, 'r', 'l')
                RWLock.lock_node(tar_lock_key, 'r', 'l')
        except Exception as e:
            message = f'Error pushing node: {e}'
            logger.error(message)
            raise HTTPException(status_code=500, detail=message)
    
        try:
            target_node_address, target_key = target_node_key.split('::')
            pull_from_url = f"{target_node_address}/noodle/node/pull_from"

            chunk_size = 1024 * 1024
            with open(tar_path, 'rb') as f:
                chunk_index = 0
                while True:
                    chunk_data = f.read(chunk_size)
                    if not chunk_data:
                        break
                    
                    encoded_chunk = base64.b64encode(chunk_data).decode('utf-8')
                    params = {
                        'template_name': template_name,
                        'target_node_key': target_key,
                        'source_node_key': source_node_key,
                        'chunk_data': encoded_chunk,
                        'chunk_index': chunk_index,
                        'is_last_chunk': len(chunk_data) < chunk_size
                    }

                    response = httpx.post(pull_from_url, params=params, timeout=30.0)
                    response.raise_for_status()
                    chunk_index += 1
            return PushResponse(success=True, message='Push successful')
        except Exception as e:
            message = f'Error pushing node: {e}'
            logger.error(message)
            raise HTTPException(status_code=500, detail=message)
    except Exception as e:
        message = f'Error pushing node: {e}'
        logger.error(message)
        raise HTTPException(status_code=500, detail=message)

@router.post('/pull_from')
def pull_node(template_name: str, target_node_key: str, source_node_key:str, chunk_data:str, chunk_index:int, is_last_chunk: bool):
    try:
        source_temp_path = settings.MEMORY_TEMP_PATH / 'push_cache' / f'{target_node_key}.tar.gz'
        source_temp_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if target node exists
        target_node = noodle.get_node_info(target_node_key)
        if target_node is not None:
            target_node_key = target_node.node_key + '_copy'

        with open(source_temp_path, 'ab') as f:
            f.seek(chunk_index * 1024 * 1024)
            chunck_bytes = base64.b64decode(chunk_data)
            f.write(chunck_bytes)
        
        if is_last_chunk:
            template = noodle.get_template(template_name)
            if template is None:
                raise HTTPException(status_code=404, detail=f'ResourceNodeTemplate "{template_name}" not found in noodle.')
            
            template.unpack(target_node_key, str(source_temp_path), template_name)

    except Exception as e:
        message = f'Error receiving pushed data: {e}'
        logger.error(message)
        raise HTTPException(status_code=500, detail=message)
    finally:
        tar_lock_key = f'{source_node_key}_tar'
        RWLock.remove_lock(source_node_key)

        with threading.Lock():
            RWLock.remove_lock(tar_lock_key)
            if not RWLock.is_node_locked(tar_lock_key):
                source_temp_path.unlink()
                source_temp_path.parent.rmdir()
        
@router.post('/pull', response_model=PullResponse)
def pull_node(template_name: str, target_node_key: str, source_node_key: str):
    """
    Pull a node from remote resource tree.
    """
    temp_path = settings.MEMORY_TEMP_PATH / 'pull_cache' / f'pull_{target_node_key.replace(".", "_")}.tar.gz'
    temp_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Check if target node exists
        target_node = noodle.get_node_info(target_node_key)
        if target_node is not None:
            target_node_key = target_node.node_key + '_copy'
        
        # Check if template exists
        template = noodle.get_template(template_name)
        if template is None:
            raise HTTPException(status_code=404, detail=f'ResourceNodeTemplate "{template_name}" not found in noodle.')

        # Check if parent node exists
        parent_key = get_parent_key(target_node_key)
        parent_node_info = noodle.get_node_info(parent_key)
        if not parent_node_info:
            raise HTTPException(status_code=404, detail=f'Parent of node "{target_node_key}" not found')
        
        source_noodle_address, source_key = source_node_key.split('::')
        
        # Trigger the pack operation on the remote noodle to package the resource node of the remote noodle.
        remote_packing_url = f"{source_noodle_address}/noodle/node/packing"
        packing_params = {'node_key': source_key}
        try:
            response = httpx.post(remote_packing_url, params = packing_params, timeout=10.0)
            if response.status_code == 404:
                raise HTTPException(status_code=404, detail=f'Source node "{source_key}" not found in remote noodle.')
            if response.status_code != 200:
                raise HTTPException(status_code=500, detail=f'Error pulling node: {response.text}')

        except Exception as e:
            raise HTTPException(status_code=500, detail=f'Error pulling node: {e}')
        
        # Trigger the push_to operations of the remote noodle to send the packaged content to the local noodle
        try:
            push_to_url = f"{source_noodle_address}/noodle/node/push_to?node_key={source_key}"

            with open(temp_path, 'wb') as target_file:
                    chunk_index = 0
                    while True:
                        # Request specific chunk
                        chunk_url = f"{push_to_url}&chunk_index={chunk_index}"
                        response = httpx.get(chunk_url, timeout=30.0)
                        response.raise_for_status()
                        
                        try:
                            chunk_data = response.json()
                            # Check if required fields exist
                            if "chunk_data" not in chunk_data:
                                raise HTTPException(status_code=500, detail="Invalid chunk data format")
                            
                            # Write chunk data
                            chunk_bytes = base64.b64decode(chunk_data["chunk_data"])
                            target_file.write(chunk_bytes)
                            
                            # Check if this is the last chunk
                            if chunk_data.get("is_last_chunk", False):
                                break
                                
                            chunk_index += 1
                        except ValueError as e:
                            # If not JSON format, it might be raw binary data
                            target_file.write(response.content)
                            break

            template.unpack(target_node_key, str(temp_path), template_name)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f'Error pulling node: {e}')
            
        return PullResponse(
            success=True,
            message="Node pulled successfully.",
            target_node_key=target_node_key
        )

    finally:
        with threading.Lock():
            if temp_path.exists():
                temp_path.unlink()
                temp_path.parent.rmdir()
                
@router.post('/packing', response_model=PackingResponse)
def packing(node_key: str):
    try:
        node_info = noodle.get_node_info(node_key)
        if node_info is None:
            raise HTTPException(status_code=404, detail=f'Node "{node_key}" not found')
        
        template = noodle.get_template(node_info.template_name)
        tar_lock_key = f'{node_key}_tar'

        with threading.Lock():
            tar_path = settings.MEMORY_TEMP_PATH / 'pull_cache' / f"{node_key.replace('.', '_')}.tar.gz"
            tar_path.parent.mkdir(parents=True, exist_ok=True)
            if not tar_path.exists():
                _, file_size = template.pack(node_key, str(tar_path))

            else:
                file_size = tar_path.stat().st_size
            RWLock.lock_node(node_key, 'r', 'l')
            RWLock.lock_node(tar_lock_key, 'r', 'l')
        return PackingResponse(compress_file_size = file_size)
    
    except Exception as e:
        message = f'Unexpected error in packing function: {e}'
        logger.error(message)
        raise HTTPException(status_code=500, detail=message)

@router.get('/push_to')
def push_to(node_key: str,chunk_index: int = 0, chunk_size: int = 1024*1024):
    try:        
        source_temp_path = settings.MEMORY_TEMP_PATH / 'pull_cache' / f"{node_key.replace('.', '_')}.tar.gz"
        if not source_temp_path.exists():
            raise HTTPException(status_code=404, detail=f'File not found: {source_temp_path}')

        with open(source_temp_path, 'rb') as f:
            f.seek(chunk_index * chunk_size)
            chunk_data = f.read(chunk_size)
            
        import base64
        return {
            "chunk_index": chunk_index,
            "chunk_data": base64.b64encode(chunk_data).decode('utf-8'),
            "is_last_chunk": len(chunk_data) < chunk_size
        }
        
    except Exception as e:
        message = f'Error pulling from {source_temp_path}'
        logger.error(message)
        raise HTTPException(status_code=500, detail=message)
    finally:
        tar_lock_key = f'{node_key}_tar'
        RWLock.remove_lock(node_key)
        
        with threading.Lock():
            RWLock.remove_lock(tar_lock_key)
            if not RWLock.is_node_locked(tar_lock_key):
                source_temp_path.unlink()
                source_temp_path.parent.rmdir()
