from pynoodle import noodle
from fastapi import APIRouter
from pydantic import BaseModel

from tests.icrms.inames import INames

class NameInfo(BaseModel):
    name: str
    
class NameIndexRequest(BaseModel):
    index: int

router = APIRouter(prefix='/names', tags=['crm/names'])

@router.post('/')
def add_name(node_key: str, request: NameInfo):
    with noodle.connect_node(INames, node_key, 'lw') as names:
        names.crm.add_name(request.name)

@router.get('/')
def get_name(node_key: str, index: int):
    with noodle.connect_node(INames, node_key, 'lr') as names:
        name = names.crm.get_names()[index]
        return NameInfo(name=name)