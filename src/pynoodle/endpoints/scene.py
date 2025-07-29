import logging
from fastapi import APIRouter, HTTPException

from ..noodle import Noodle
from ..schemas.scene import SceneNodeInfo

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get('/', response_model=SceneNodeInfo)
def get_node_info(node_key: str, child_start_index: int = 0, child_end_index: int = None):
    try:
        node_info = Noodle().get_node_info(node_key, child_start_index, child_end_index)
        if not node_info:
            raise HTTPException(status_code=404, detail='Node not found')
        return node_info
    except Exception as e:
        logger.error(f'Error fetching node info: {e}')
        raise HTTPException(status_code=500, detail='Internal Server Error')