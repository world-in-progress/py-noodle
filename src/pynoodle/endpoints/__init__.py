from fastapi import APIRouter

from . import scene
from . import dependencies

router = APIRouter()

router.include_router(dependencies.router, prefix='/dependencies', tags=['noodle/dependencies'])
router.include_router(scene.router, prefix='/scene', tags=['noodle/scene'])