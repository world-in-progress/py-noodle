import yaml
import inspect
import logging
import threading
import importlib
from dataclasses import dataclass
from typing import TypeVar, Type, Callable

from .config import settings
from .schemas.config import NoodleConfiguration

T = TypeVar('T')
logger = logging.getLogger(__name__)

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
    unmount: Callable[[str], None] = lambda x: None # hook for unmount actions
    mount: Callable[[str, dict | None], dict | None] = lambda x, y: y # hook for mount actions, and return launch params for node CRM __init__
    
    def __post_init__(self):
        if not self.crm:
            raise ValueError('CRM class must be provided for ResourceNodeTemplate')
    
@dataclass
class ResourceNodeTemplateModule:
    name: str
    module_path: str | None = None
    
    _lock: threading.Lock = threading.Lock()
    
    _crm: Type[T] = None
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