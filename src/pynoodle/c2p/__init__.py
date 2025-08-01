import logging
import c_two as cc
from typing import TypeVar

T = TypeVar('T')
logger = logging.getLogger(__name__)

transferable = cc.transferable

def icrm(namespace: str) -> T:
    if not namespace:
        raise ValueError('Namespace of ICRM cannot be empty.')
    
    def wrapper(cls: T) -> T:
        cls.__namespace__ = namespace
        return cc.icrm(cls)
    return wrapper

def crm(cls: T) -> T:
    if not hasattr(cls, 'terminate'):
        raise TypeError(f'Class {cls.__name__} does not have a "terminate" method, which is required for CRM functionality.')
    
    return cc.iicrm(cls)