from fastapi import APIRouter

from . import node
from . import proxy

router = APIRouter()
router.include_router(node.router, prefix='/node', tags=['noodle/node'])
router.include_router(proxy.router, prefix='/proxy', tags=['noodle/proxy'])