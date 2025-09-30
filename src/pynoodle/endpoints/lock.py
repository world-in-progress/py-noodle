import logging
from fastapi import APIRouter, HTTPException

from ..node.lock import RWLock
from ..schemas.lock import LockInfo

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get('/', response_model=LockInfo)
def get_lock_info(lock_id: str):
    try:
        lock_info = RWLock.get_lock_info(lock_id)
        if not lock_info:
            raise HTTPException(status_code=404, detail='Lock not found')
        return lock_info
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error checking node lock: {e}')