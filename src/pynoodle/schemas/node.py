from pydantic import BaseModel

class ResourceNodeInfo(BaseModel):
    node_key: str
    access_info: str | None = None
    template_name: str | None = None
    children: list['ResourceNodeInfo'] | None = None

class UnlinkInfo(BaseModel):
    success: bool