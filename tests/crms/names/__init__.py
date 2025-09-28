from pynoodle import ResourceNodeTemplate

from .names import Names
from .hooks import MOUNT, UNMOUNT

template = ResourceNodeTemplate(
    crm=Names,
    mount=MOUNT,
    unmount=UNMOUNT
)