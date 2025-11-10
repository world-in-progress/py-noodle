import httpx
import logging
import tempfile
import threading
from pathlib import Path
from typing import Literal
from fastapi import APIRouter, HTTPException, UploadFile

from ..noodle import noodle
from ..node.lock import RWLock
from urllib.parse import urljoin
from ..schemas.lock import LockInfo
from ..schemas.node import ResourceNodeInfo, UnlinkInfo, PushResponse, PullResponse, PackingResponse, PullFromResponse, FileResponse

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

@router.post("/mount", response_model=dict)
def mount_node(node_key: str, node_template_name: str = None, mount_params: dict = None):
    """
    Mount a node
    """
    try:
        success, error = noodle.mount(node_key, node_template_name, mount_params)
        if not success and error:
            raise HTTPException(status_code=400, detail=error)
        return {
            "success": True,
            "message": f"Node {node_key} mounted successfully"
        }
    except Exception as e:
        logger.error(f'Error mounting node: {e}')
        raise HTTPException(status_code=500, detail=f'Error mounting node: {e}')

@router.post("/unmount", response_model=dict)
def unmount_node(node_key: str):
    """
    Unmount a node
    """
    try:
        success, error = noodle.unmount(node_key)
        if not success and error:
            raise HTTPException(status_code=400, detail=error)
        return {
            "success": True,
            "message": f"Node {node_key} unmounted successfully"
        }
    except Exception as e:
        logger.error(f'Error unmounting node: {e}')
        raise HTTPException()


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

# @router.post('/push', response_model=PushResponse)
# def push_node(source_node_key: str, target_node_key: str):
#     """
#     Push a node to remote resource tree.
#     """
#     node_info = noodle.get_node_info(source_node_key)
#     if not node_info:
#         raise ValueError(f'Node "{source_node_key}" not found')
    
#     launch_params_str = getattr(node_info, 'launch_params', None)
#     if not launch_params_str:
#             raise ValueError(f'Node "{source_node_key}" has no launch parameters')
    
#     resource_path = parse_target_resource_path(launch_params_str, source_node_key)

#     template_name = node_info.template_name
#     if template_name is None:
#         raise HTTPException(status_code=400, detail=f'Node "{source_node_key}" is a resource set, cannot be pushed')
#     template = noodle.get_template(template_name)

#     try:
#         compress_file_path = template.pack(source_node_key)
#         node_info = noodle.get_node_info(source_node_key)
#         mount_params_str = getattr(node_info, 'mount_params', '')

#         return PushResponse(
#             success=True,
#             message="Node pushed successfully.",
#             target_node_key=target_node_key,
#             mount_params=mount_params_str,
#             compress_file_path=compress_file_path
#         )
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f'Error pushing node: {e}')

@router.post('/pull', response_model=PullResponse)
def pull_node(template_name: str, target_node_key: str, source_node_key: str, remote_noodle_url: str, mount_params: str, compress_file: UploadFile):
    """
    Pull a node from remote resource tree.
    """
    template = noodle.get_template(template_name)
    if template is None:
        raise HTTPException(status_code=404, detail=f'ResourceNodeTemplate "{template_name}" not found in noodle.')

    node_info = noodle.get_node_info(target_node_key)
    if not node_info:
        raise ValueError(f'Node "{target_node_key}" not found')
    
    node_info_recode = noodle._load_node_record(target_node_key, is_parent=False)
    if node_info_recode.parent_key is None:
        raise ValueError(f'Node "{target_node_key}" has no parent')
    
    launch_params_str = getattr(node_info, 'launch_params', None)
    if not launch_params_str:
            raise ValueError(f'Node "{target_node_key}" has no launch parameters')
    
    # Check whether the node_key exists in the remote noodle.
    get_params = {"node_key": source_node_key}
    try:
       response = httpx.get(remote_noodle_url, params = get_params, timeout=10.0)
       if response.status_code == 404:
           raise HTTPException(status_code=404, detail=f'Node "{source_node_key}" not found in remote noodle.')
       elif response.status_code != 200:
           raise HTTPException(status_code=500, detail=f'Error pulling node: {response.text}')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error pulling node: {e}')
    
    # Trigger the pack operation on the remote noodle to package the resource nodes under the remote noodle.
    remote_packing_url = urljoin(remote_noodle_url, '/packing')
    packing_params = {'source_node_key': source_node_key, 'template_name': template_name}
    try:
        response = httpx.get(remote_packing_url, params = packing_params, timeout=10.0)
        if response.status_code != 200:
            raise HTTPException(status_code=500, detail=f'Error pulling node: {response.text}')
        
        packing_result = response.json()
        file_size = packing_result.get('compress_file_size', 0)

        temp_path = Path('/tmp/pull_cache') / f'pull_{target_node_key.replace(".", "_")}.tar.gz'
        temp_path.parent.mkdir(parents=True, exist_ok=True)

        with open(temp_path, 'wb') as f:
            f.seek(file_size - 1)
            f.write(b'\0')

    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error pulling node: {e}')
    
    # Trigger the pull_from and download_file operations on the remote noodle to send the packaged content from the remote noodle to the local noodle.
    remote_pull_from_url = urljoin(remote_noodle_url, '/pull_from')
    params = {
        'source_node_key': source_node_key,
        'target_node_key': target_node_key,
    }
    try:
        response = httpx.post(remote_pull_from_url, params = params, timeout=10.0)
        if response.status_code != 200:
            raise HTTPException(status_code=500, detail=f'Error pulling node: {response.text}')
        
        pull_result = response.json()
        remote_file_path = pull_result.get('remote_file_path')

        remote_download_url = urljoin(remote_noodle_url, f'/download_file?file_path={remote_file_path}')

        with httpx.stream('GET', remote_download_url, timeout=10.0) as response:
            if response.status_code != 200:
                return HTTPException(status_code=500, detail=f'Error pulling node: {response.text}')
            
            with open(temp_path, 'wb') as target_file:
                for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                    target_file.write(chunk)

        template.unpack(temp_path, target_node_key, template_name, mount_params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error pulling node: {e}')

    finally:
        RWLock.remove_lock(source_node_key)
        with threading.Lock():
            tar_lock_key = f'{source_node_key}_tar'
            RWLock.remove_lock(tar_lock_key)
            if temp_path.exists():
                temp_path.unlink()
                temp_path.parent.rmdir()
    


@router.post('/packing', response_model=PackingResponse)
def packing(source_node_key: str,template_name: str):
    try:
        template = noodle.get_template(template_name)
        tar_lock_key = f'{source_node_key}_tar'

        with threading.Lock():
            temp_dir = Path(tempfile.gettempdir())
            tar_filename = f"push_{source_node_key.replace('.', '_')}.tar.gz"
            tar_path = temp_dir / tar_filename
            if tar_path.exists():
                RWLock.lock_node(source_node_key, 'r', 'l')
                RWLock.remove_lock(tar_lock_key)
            else:
                _, file_size = template.pack(source_node_key, str(tar_path))
                RWLock.lock_node(source_node_key, 'r', 'l')
                RWLock.lock_node(tar_lock_key, 'r', 'l')
        return PackingResponse(
            success = True,
            message = "Node packed successfully.",
            compress_file_path = str(tar_path),
            compress_file_size = file_size
        )
    except Exception as e:
        logger.error(f'Unexpected error in packing function: {e}')

@router.get('/pull_from', response_model=PullFromResponse)
def pull_from(source_node_key: str, target_node_key: str):
    try:
        source_temp_dir = Path(tempfile.gettempdir())
        source_filename = f"push_{source_node_key.replace('.', '_')}.tar.gz"
        source_path = source_temp_dir / source_filename

        if not source_path.exists():
            raise HTTPException(status_code=404, detail=f'File not found: {source_path}')
            
        file_size = source_path.stat().st_size
        
        return PullFromResponse(
            success=True,
            message="File ready for download",
            source_node_key=source_node_key,
            target_node_key=target_node_key,
            file_path=str(source_path),
            file_size=file_size
        )
    except Exception as e:
        logger.error(f'Error preparing file transfer for {source_node_key}: {e}')
        raise HTTPException(status_code=500, detail=f'Error preparing file transfer: {e}')

@router.get('/download_file')
def download_file(file_path: str):
    try:
        path = Path(file_path)
        if not path.exists():
            raise HTTPException(status_code=404, detail=f'File not found: {file_path}')

        return FileResponse(
            path=str(path),
            media_type='application/gzip',
            filename=path.name
        )
    except Exception as e:
        logger.error(f'Error downloading file {file_path}: {e}')
        raise HTTPException(status_code=500, detail=f'Error downloading file: {e}')
