import yaml
import json
import shutil
import tarfile
import inspect
import logging
import threading
import importlib
from pathlib import Path
from dataclasses import dataclass
from typing import TypeVar, Type, Callable

from . import noodle
from .node.treeger import Treeger
from .config import settings
from .schemas.config import NoodleConfiguration

T = TypeVar('T')
logger = logging.getLogger(__name__)

def default_pack(node_key: str, tar_path: str) -> tuple[str, int]:
    """
    Generic pack implementation that compresses resource data into a tar file.
    
    Args:
        node_key: The node key being packed
        
    Returns:
        Path to the compressed tar file
    """

    try:
        treeger = Treeger()
        node_record = treeger._load_node_record(node_key, is_cascade=False)
        launch_params_str = node_record.launch_params
        launch_params = json.loads(launch_params_str)
        target_resource_path = launch_params.get('resource_space')
        resource_path = target_resource_path
        
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
        
        file_size = tar_path.stat().st_size
        
        logger.info(f"Successfully packed node {node_key} to {tar_path}")
        return str(tar_path), file_size
        
    except Exception as e:
        logger.error(f"Error packing node {node_key}: {e}")
        # Clean up the tar file if creation failed
        if Path(tar_path).exists():
            Path(tar_path).unlink()
        raise

def default_unpack(target_node_key: str, tar_path: str, template_name: str, mount_params: dict) -> None:
    """
    Generic unpack implementation that extracts resource data from a tar file.
    
    Args:
        node_key: The node key being unpacked
        tar_path: Path to the compressed tar file
    """
    try:
        node_info = noodle.get_node_info(target_node_key)
        launch_params_str = getattr(node_info, 'launch_params', None)
        launch_params = json.loads(launch_params_str)
        target_node_path = launch_params.get('resource_space')
        
        Path(target_node_path).mkdir(parents=True, exist_ok=True)

        with tarfile.open(tar_path, 'r:gz') as tarf:
            tarf.extractall(target_node_path)
        
        noodle.mount(target_node_key, node_template_name=template_name, mount_params=mount_params)
    except Exception as e:
        logger.error(f"Error unpacking node {target_node_key}: {e}")
        raise
        


@dataclass
class ICRMModule:
    tag: str
    module_path: str
    
    _lock: threading.Lock = threading.Lock()

    _icrm: Type[T] = None

    def __post_init__(self):
        # Validate tag format
        parts = self.tag.split('/')
        if len(parts) != 3:
            raise ValueError(f'ICRM tag "{self.tag}" is not in the format "namespace/name/version"')
    
    def _load_from_module(self):
        module = importlib.import_module(self.module_path)
        if not module:
            raise ImportError(f'Module {self.module_path} could not be imported')
        self._icrm = getattr(module, self.name, None)
        if not self._icrm:
            raise ImportError(f'ICRM class "{self.name}" not found in module {self.module_path}')
        if self._icrm.__tag__ != self.tag:
            raise ValueError(f'ICRM class tag "{self._icrm.__tag__}" does not match expected tag "{self.tag}"')

    @property
    def namespace(self) -> str:
        return self.tag.split('/')[0]
    
    @property
    def name(self) -> str:
        return self.tag.split('/')[1]
    
    @property
    def version(self) -> str:
        return self.tag.split('/')[2]
    
    @property
    def icrm(self) -> Type[T]:
        with self._lock:
            if self._icrm is None:
                self._load_from_module()
            return self._icrm

@dataclass
class ResourceNodeTemplate:
    crm: Type[T]
    pack: Callable[[str, str], tuple[str, int]] = lambda x, y: ('', 0) # pack(node_key) -> compress file path
    unpack: Callable[[str, str, str, dict | None], None] = lambda x, y, z, w: None
    unmount: Callable[[str], None] = lambda x: None
    mount: Callable[[str, dict | None], dict | None] = lambda x, y: y

    def __post_init__(self):
        if not self.crm:
            raise ValueError('CRM class must be provided for ResourceNodeTemplate')
        if not hasattr(self, 'pack') or not callable(self.pack):
            # Default pull implementation
            self.pack = default_pack
        if not hasattr(self, 'unpack') or not callable(self.unpack):
            # Default unpack implementation
            self.unpack = default_unpack

    
@dataclass
class ResourceNodeTemplateModule:
    name: str
    module_path: str | None = None

    _lock: threading.Lock = threading.Lock()
    _crm: Type[T] = None
    _pack: Callable[[str, str], tuple[str, int]] = None
    _unpack: Callable[[str, str, str, dict], None] = None
    _unmount: Callable[[str], None] = None
    _mount: Callable[[str, dict | None], dict | None] = None

    def _load_from_module(self):
        module = __import__(self.module_path, fromlist=[''])
        if not module:
            raise ImportError(f'Module {self.module_path} could not be imported')

        template: ResourceNodeTemplate = getattr(module, 'template', None)
        if not template:
            raise ImportError(f'ResourceNodeTemplate "template" not found in module {self.module_path}')

        self._crm = template.crm
        self._pack = template.pack
        self._unpack = template.unpack
        self._mount = template.mount
        self._unmount = template.unmount

        if not self._crm:
            raise ImportError(f'CRM class "{self.name}" not found in module {self.module_path}')

    @property
    def crm(self) -> Type[T]:
        with self._lock:
            if self._crm is None:
                self._load_from_module()
            return self._crm
    
    @property
    def pack(self) -> Callable[[str, str], tuple[str, int]]:
        with self._lock:
            if self._pack is None:
                self._load_from_module()
            return self._pack

    @property
    def unpack(self) -> Callable[[str, str, str, dict], None]:
        with self._lock:
            if self._unpack is None:
                self._load_from_module()
            return self._unpack

    @property
    def mount(self) -> Callable[[str, dict | None], dict | None]:
        with self._lock:
            if self._mount is None:
                self._load_from_module()
            return self._mount

    @property
    def unmount(self) -> Callable[[str], None]:
        with self._lock:
            if self._unmount is None:
                self._load_from_module()
            return self._unmount
        
class ModuleCache:
    def __init__(self):
        # Read configuration
        with open(settings.NOODLE_CONFIG_PATH, 'r') as f:
            config = NoodleConfiguration(**yaml.safe_load(f))

        # Record ICRM modules and ResourceNodeTemplate modules
        self.icrm_modules: dict[str, ICRMModule] = {}
        self.templates: dict[str, ResourceNodeTemplate] = {}

        for icrm_desc in config.icrms:
            if icrm_desc.tag in self.icrm_modules:
                raise ValueError(f'Duplicate ICRM tag "{icrm_desc.tag}" found in configuration.')
            self.icrm_modules[icrm_desc.tag] = ICRMModule(tag=icrm_desc.tag, module_path=icrm_desc.module_path)
        for node_template_desc in config.node_templates:
            if node_template_desc.name in self.templates:
                raise ValueError(f'Duplicate ResourceNodeTemplate name "{node_template_desc.name}" found in configuration.')
            self.templates[node_template_desc.name] = ResourceNodeTemplateModule(name=node_template_desc.name, module_path=node_template_desc.module_path)

    def match(self, icrm_tag: str, node_template_name: str) -> tuple[bool, str | None]:
        error: str | None = None
        
        # Try to find the ICRM and CRM
        icrm_module = self.icrm_modules.get(icrm_tag, None)
        if not icrm_module:
            error = f'ICRM tag "{icrm_tag}" not found in noodle.'
            return False, error
        icrm = icrm_module.icrm
        
        template = self.templates.get(node_template_name, None)
        if not template:
            error = f'ResourceNodeTemplate "{node_template_name}" not found in noodle.'
            return False, error
        crm = template.crm
        
        # Get all public methods from ICRM (excluding private methods)
        icrm_methods = {
            name: method
            for name, method in inspect.getmembers(icrm, predicate=inspect.isfunction)
            if not name.startswith('_')
        }
        
        # Get all public methods from CRM
        crm_methods = {
            name: method
            for name, method in inspect.getmembers(crm, predicate=inspect.isfunction)
            if not name.startswith('_')
        }
        
        # Check for missing methods
        missing_methods = set(icrm_methods.keys()) - set(crm_methods.keys())
        if missing_methods:
            error = f'CRM "{node_template_name}" is missing methods required by ICRM "{icrm_tag}": {missing_methods}'
            return False, error

        return True, None