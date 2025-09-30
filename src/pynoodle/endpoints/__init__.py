from fastapi import APIRouter

from . import node
from . import lock
from . import proxy

router = APIRouter()
router.include_router(node.router, prefix='/node', tags=['noodle/node'])
router.include_router(lock.router, prefix='/lock', tags=['noodle/lock'])
router.include_router(proxy.router, prefix='/proxy', tags=['noodle/proxy'])