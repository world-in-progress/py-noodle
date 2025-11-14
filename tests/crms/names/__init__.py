from pynoodle import ResourceNodeTemplate

from .names import Names
from .hooks import MOUNT, UNMOUNT, PRIVATIZATION, PACK, UNPACK

template = ResourceNodeTemplate(
    crm=Names,
    mount=MOUNT,
    unmount=UNMOUNT,
    privatization=PRIVATIZATION,
    pack=PACK,
    unpack=UNPACK
)