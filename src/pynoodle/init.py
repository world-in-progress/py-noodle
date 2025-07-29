import yaml
import shutil
import logging
from pathlib import Path
from fastapi import FastAPI

from .config import settings
from .endpoints import router

logger = logging.getLogger(__name__)

from .scene import Treeger, RWLock
from .schemas.scenario import ScenarioConfiguration

def NOODLE_INIT(app: FastAPI | None = None) -> None:
        # Read configuration
        configuration_path = Path(settings.NOODLE_CONFIG_PATH)
        if not configuration_path.is_absolute():
            configuration_path = Path.cwd() / configuration_path
        with open(configuration_path, 'r') as f:
            config_data = yaml.safe_load(f)
        config = ScenarioConfiguration(**config_data)
        
        # Pre-remove all locks if configured
        if settings.PRE_REMOVE_ALL_LOCKS:
            scene_path = Path(config.scene_path)
            if not scene_path.is_absolute():
                scene_path = Path.cwd() / scene_path
            RWLock.clear_all(scene_path)

        # Pre-remove existing memory temp directory if configured
        if settings.PRE_REMOVE_MEMORY_TEMP_DIR:
            memory_temp_path = Path(settings.MEMORY_TEMP_DIR)
            if not memory_temp_path.is_absolute():
                memory_temp_path = Path.cwd() / memory_temp_path
            if memory_temp_path.exists():
                shutil.rmtree(memory_temp_path)
            
        # Create a new memory temp directory
        memory_temp_path.mkdir(parents=True, exist_ok=True)

        # Register Noodle endpoints to the FastAPI app
        if app is not None:
            app.include_router(router, prefix='/noodle', tags=['noodle'])
        else:
            logger.warning('No FastAPI app provided, Noodle endpoints will not be registered.')