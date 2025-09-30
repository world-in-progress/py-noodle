import shutil
import logging
from fastapi import FastAPI

from .config import settings
from .endpoints import router
from .node import RWLock, Treeger

logger = logging.getLogger(__name__)

def NOODLE_INIT(app: FastAPI | None = None) -> None:
        # Initialize Treeger
        Treeger.init()
        
        # Initialize RWLock
        RWLock.init()
            
        # Create a new memory temp directory
        settings.MEMORY_TEMP_PATH.mkdir(parents=True, exist_ok=True)

        # Register Noodle endpoints to the FastAPI app
        if app is not None:
            app.include_router(router, prefix='/noodle', tags=['noodle'])
        else:
            logger.debug('No FastAPI app provided, Noodle endpoints will not be registered.')

def NOODLE_TERMINATE() -> None:
    """Terminate Noodle CRM servers running in process level and clean up locks."""
    
    # Shutdown all process-level CRM servers gracefully
    RWLock.release_all_process_servers()
    
    # Forcefully shutdown all nodes' CRM servers running in process level if graceful shutdown fails
    # Nodes's CRM servers running in local level will be automatically shutdown when the process exits
    if settings.MEMORY_TEMP_PATH.exists():
        shutil.rmtree(settings.MEMORY_TEMP_PATH)
        
    # Clear all locks
    RWLock.clear_all()