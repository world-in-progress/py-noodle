import logging
import c_two as cc
from typing import TypeVar

T = TypeVar('T')
logger = logging.getLogger(__name__)

icrm = cc.icrm

transferable = cc.transferable

def crm(cls: T) -> T:
    if not hasattr(cls, 'terminate'):
        raise TypeError(f'Class {cls.__name__} does not have a "terminate" method, which is required for CRM functionality.')
    
    return cc.iicrm(cls)