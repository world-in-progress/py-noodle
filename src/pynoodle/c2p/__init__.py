import logging
import c_two as cc
from typing import TypeVar

T = TypeVar('T')
logger = logging.getLogger(__name__)

transferable = cc.transferable

def icrm(namespace: str, version: str) -> T:
    if not namespace:
        raise ValueError('Namespace of ICRM cannot be empty.')
    if not version:
        raise ValueError('Version of ICRM cannot be empty (version example: "1.0.0").')
    if not isinstance(version, str) or not version.count('.') == 2:
        raise ValueError('Version must be a string in the format "major.minor.patch".')
    
    def wrapper(cls: T) -> T:
        new_cls = cc.icrm(cls)
        new_cls.__version__ = version
        new_cls.__namespace__ = namespace
        return new_cls
    
    return wrapper

def crm(cls: T) -> T:
    if not hasattr(cls, 'terminate'):
        raise TypeError(f'Class {cls.__name__} does not have a "terminate" method, which is required for CRM functionality.')
    
    return cc.iicrm(cls)