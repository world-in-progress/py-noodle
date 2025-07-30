import shutil
import logging
from fastapi import FastAPI

from .config import settings
from .endpoints import router
from .scene import RWLock, Treeger

logger = logging.getLogger(__name__)

def NOODLE_INIT(app: FastAPI | None = None) -> None:
        # Initialize Treeger
        Treeger.init()
        
        # Initialize RWLock
        RWLock.init()
    
        # Pre-remove all locks if configured
        if settings.PRE_REMOVE_ALL_LOCKS:
            RWLock.clear_all()

        # Pre-remove existing memory temp directory if configured
        if settings.PRE_REMOVE_MEMORY_TEMP_PATH:
            if settings.MEMORY_TEMP_PATH.exists():
                shutil.rmtree(settings.MEMORY_TEMP_PATH)
            
        # Create a new memory temp directory
        settings.MEMORY_TEMP_PATH.mkdir(parents=True, exist_ok=True)

        # Register Noodle endpoints to the FastAPI app
        if app is not None:
            app.include_router(router, prefix='/noodle', tags=['noodle'])
        else:
            logger.debug('No FastAPI app provided, Noodle endpoints will not be registered.')