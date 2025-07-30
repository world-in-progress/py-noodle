import logging
from fastapi import APIRouter, HTTPException

from ..noodle import noodle
from ..schemas.dependencies import DependencyRequest

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post('/')
def process_dependencies(req: DependencyRequest):
    try:
        if req.method == 'ADD':
            noodle.add_dependency(req.node_key, req.dependent_node_key)
        elif req.method == 'REMOVE':
            noodle.remove_dependency(req.node_key, req.dependent_node_key)
    
    except Exception as e:
        logger.error(f'Error processing dependencies: {e}')
        raise HTTPException(status_code=500, detail=str(e))