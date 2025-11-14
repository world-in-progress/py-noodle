import os
import json
import sys
import tarfile
from pathlib import Path
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'py-noodle', 'src'))
from pynoodle.noodle import noodle

def MOUNT(node_key: str, params: dict | None) -> dict | None:
    name = node_key.split('.')[-1]
    resource_space = Path.cwd() / 'resource' / name / 'names.json'
    if not resource_space.exists():
        resource_space.parent.mkdir(parents=True, exist_ok=True)
        with open(resource_space, 'w') as f:
            f.write('{"names": []}')
    
    return {
        'resource_space': str(resource_space)
    }

def UNMOUNT(node_key: str) -> None:
    name = node_key.split('.')[-1]
    resource_space = Path.cwd() / 'resource' /  name / 'names.json'
    if resource_space.exists():
        resource_space.unlink()
        
    # Remove the directory if empty
    parent_dir = resource_space.parent
    if parent_dir.exists() and not any(parent_dir.iterdir()):
        parent_dir.rmdir()

def PRIVATIZATION(node_key: str, mount_params: dict | None) -> dict | None:
    """
    Generate node-specific launch parameters for the names resource node.
    
    Args:
        node_key: The node key being mounted
        mount_params: Mount parameters provided during node mounting
        
    Returns:
        Dictionary containing node-specific launch parameters
    """
    try:
        # Extract node name from node_key (last part after splitting by '.')
        node_name = node_key.split('.')[-1]
        
        # Generate node-specific resource space path
        resource_space = Path.cwd() / 'resource'  / node_name / 'names.json'
        
        # Ensure the directory exists
        resource_space.parent.mkdir(parents=True, exist_ok=True)
        
        # Create default launch parameters
        launch_params = {
            'resource_space': str(resource_space),
        }
        
        # Merge with provided mount parameters if any
        if mount_params and isinstance(mount_params, dict):
            launch_params.update(mount_params)
        
        return launch_params
        
    except Exception as e:
        # In a real implementation, you might want to log this error
        raise Exception(f"Error generating privatized parameters for node {node_key}: {e}")

def PACK(node_key: str, tar_path: str) -> tuple[str, int]:
    try:
        
        node_record = noodle._load_node_record(node_key, is_cascade=False)
        launch_params_str = node_record.launch_params
        launch_params = json.loads(launch_params_str)
        target_resource_path = launch_params.get('resource_space')
        resource_path = Path(target_resource_path)
        
        with tarfile.open(tar_path, 'w:gz') as tarf:
            if resource_path.is_file():
                # If it's a single file, add it directly
                tarf.add(resource_path, arcname=resource_path.name)
            elif resource_path.is_dir():
                # If it's a directory, add all files recursively
                for file_path in resource_path.rglob('*'):
                    if file_path.is_file():
                        arcname = file_path.relative_to(resource_path.parent)
                        tarf.add(file_path, arcname=arcname)
        
        file_size = Path(tar_path).stat().st_size

        return str(tar_path), file_size
        
    except Exception as e:
        raise Exception(f"Error packing node {node_key}: {e}")
def UNPACK(target_node_key: str, tar_path: str, template_name: str, mount_params: dict) -> None:
    """
    Generic unpack implementation that extracts resource data from a tar file.
    
    Args:
        node_key: The node key being unpacked
        tar_path: Path to the compressed tar file
    """
    try:
        node_record = noodle._load_node_record(target_node_key, False)
        if node_record is None:
            raise Exception(f"Node {target_node_key} not found in local resource tree")
        
        launch_params_str = node_record.launch_params
        launch_params = json.loads(launch_params_str) if launch_params_str else {}
        target_node_path = launch_params.get('resource_space')
    
        Path(target_node_path).mkdir(parents=True, exist_ok=True)

        with tarfile.open(tar_path, 'r:gz') as tarf:
            target_path = Path(target_node_path)
            if target_path.exists() and target_path.is_dir():
                for item in target_path.iterdir():
                    if item.is_file():
                        item.unlink()
                    elif item.is_dir():
                        import shutil
                        shutil.rmtree(item)
            
            tarf.extractall(target_node_path)
        
        noodle.mount(target_node_key, node_template_name=template_name, mount_params=mount_params)
    except Exception as e:
        raise Exception(f"Error unpacking node {target_node_key}: {e}")