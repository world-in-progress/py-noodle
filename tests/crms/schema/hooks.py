import os
import json
import sys
import tarfile
from pathlib import Path

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'py-noodle', 'src'))
from pynoodle.noodle import noodle

def MOUNT(node_key: str, params: dict | None) -> dict | None:
    """
    Mount a schema node.
    
    Args:
        node_key: The node key being mounted
        params: Mount parameters provided during node mounting
        
    Returns:
        Dictionary containing node-specific launch parameters
    """
    name = node_key.split('.')[-1]
    resource_space = Path.cwd() / 'resource' / name / 'schema.json'
    if not resource_space.exists():
        resource_space.parent.mkdir(parents=True, exist_ok=True)
        default_info = {
            'name': '',
            'epsg': 4326,
            'alignment_origin': [0.0, 0.0],
            'grid_info': []
        }
        
        # Use params to initialize default info if provided
        if params and isinstance(params, dict):
            # Only update keys that exist in default_info to avoid polluting schema with unrelated params
            for key in default_info.keys():
                if key in params:
                    default_info[key] = params[key]

        with open(resource_space, 'w') as f:
            json.dump(default_info, f, indent=4)
    
    return {
        'resource_space': str(resource_space)
    }

def UNMOUNT(node_key: str) -> None:
    """
    Unmount a schema node.
    
    Args:
        node_key: The node key being unmounted
    """
    name = node_key.split('.')[-1]
    resource_space = Path.cwd() / 'resource' / name / 'schema.json'
    if resource_space.exists():
        resource_space.unlink()
        
    # Remove the directory if empty
    parent_dir = resource_space.parent
    if parent_dir.exists() and not any(parent_dir.iterdir()):
        parent_dir.rmdir()

def PRIVATIZATION(node_key: str, mount_params: dict | None) -> dict | None:
    """
    Generate node-specific launch parameters for the schema resource node.
    
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
        resource_space = Path.cwd() / 'resource' / node_name / 'schema.json'
        
        # Ensure the directory exists
        resource_space.parent.mkdir(parents=True, exist_ok=True)
        
        # Create default launch parameters
        launch_params = {
            'resource_space': str(resource_space),
        }
        
        return launch_params
        
    except Exception as e:
        raise Exception(f"Error generating privatized parameters for node {node_key}: {e}")

def PACK(node_key: str, tar_path: str) -> tuple[str, int]:
    """
    Pack schema node data into a tar.gz file.
    
    Args:
        node_key: The node key being packed
        tar_path: Path where the compressed tar file should be created
        
    Returns:
        Tuple of (tar_path, file_size)
    """
    try:
        node_record = noodle._load_node_record(node_key, is_cascade=False)
        launch_params_str = node_record.launch_params
        launch_params = json.loads(launch_params_str)
        target_resource_path = launch_params.get('resource_space')
        resource_path = Path(target_resource_path)
        mount_params_str = node_record.mount_params
        
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
            
            # Add mount_params to the tar file as a separate file
            if mount_params_str:
                mount_params_path = Path(tar_path).parent / 'mount_params.json'
                with open(mount_params_path, 'w') as f:
                    f.write(mount_params_str)
                tarf.add(mount_params_path, arcname='mount_params.json')
                mount_params_path.unlink() # Clean up temporary file
        
        file_size = Path(tar_path).stat().st_size
        return str(tar_path), file_size
        
    except Exception as e:
        raise Exception(f"Error packing node {node_key}: {e}")

def UNPACK(target_node_key: str, tar_path: str, template_name: str) -> None:
    """
    Unpack schema node data from a tar.gz file.
    
    Args:
        target_node_key: The node key being unpacked
        tar_path: Path to the compressed tar file
        template_name: Name of the template to use for unpacking
    """
    try:      
        name = target_node_key.split('.')[-1]
        resource_space = Path.cwd() / 'resource' / name
                    
        Path(resource_space).mkdir(parents=True, exist_ok=True)

        mount_params_str = None

        with tarfile.open(tar_path, 'r:gz') as tarf:
            target_path = Path(resource_space)
            if target_path.exists() and target_path.is_dir():
                for item in target_path.iterdir():
                    if item.is_file():
                        item.unlink()
                    elif item.is_dir():
                        import shutil
                        shutil.rmtree(item)
            
            tarf.extractall(resource_space)
            
            # Check for mount_params.json
            try:
                mount_params_file = resource_space / 'mount_params.json'
                if mount_params_file.exists():
                    with open(mount_params_file, 'r') as f:
                        mount_params_str = f.read()
                    mount_params_file.unlink() # Remove it after reading
            except Exception as e:
                print(f"Warning: Failed to read mount_params.json: {e}")
        
        parent_key = '.'.join(target_node_key.split('.')[:-1])
        parent_key = parent_key if parent_key else None
        if parent_key and not noodle._has_node(parent_key):
            raise ValueError(f'Parent node "{parent_key}" not found in scene for node "{target_node_key}"')

        schema_json_path = resource_space / 'schema.json'
        launch_params = json.dumps({'resource_space': str(schema_json_path)}, indent=4)
        
        # Check if node already exists
        if noodle._has_node(target_node_key):
            # Update existing node to handle potential changes in parent_key or params
            noodle._update_node(target_node_key, parent_key, template_name, launch_params, mount_params_str)
        else:
            # Insert new node
            noodle._insert_node(target_node_key, parent_key, template_name, launch_params, mount_params_str)

    except Exception as e:
        raise Exception(f"Error unpacking node {target_node_key}: {e}")