from pynoodle import ResourceNodeTemplate

from .schema import Schema
from .hooks import  MOUNT, UNMOUNT, PRIVATIZATION, PACK, UNPACK

template = ResourceNodeTemplate(
    crm=Schema,
    mount=MOUNT,
    unmount=UNMOUNT,
    privatization=PRIVATIZATION,
    pack=PACK,
    unpack=UNPACK
)