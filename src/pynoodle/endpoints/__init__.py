from fastapi import APIRouter

from . import scene
from . import proxy
from . import scenario
from . import dependencies

router = APIRouter()
router.include_router(scene.router, prefix='/scene', tags=['noodle/scene'])
router.include_router(proxy.router, prefix='/proxy', tags=['noodle/proxy'])
router.include_router(scenario.router, prefix='/scenario', tags=['noodle/scenario'])
router.include_router(dependencies.router, prefix='/dependencies', tags=['noodle/dependencies'])