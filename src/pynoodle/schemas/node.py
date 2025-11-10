from pydantic import BaseModel

class ResourceNodeInfo(BaseModel):
    node_key: str
    access_info: str | None = None
    template_name: str | None = None
    children: list['ResourceNodeInfo'] | None = None

class UnlinkInfo(BaseModel):
    success: bool

class PushResponse(BaseModel):
    success: bool
    message: str
    target_node_key: str
    mount_params: str
    compress_file_path: str

class PullResponse(BaseModel):
    success: bool
    message: str
    target_node_key: str

class PackingResponse(BaseModel):
    success: bool
    message: str
    compress_file_size: int
    compress_file_path: str

class PullFromResponse(BaseModel):
    success: bool
    message: str
    source_node_key: str
    target_node_key: str
    compress_file_path: str
    compress_file_size: int

class FileResponse(BaseModel):
    path: str
    media_type: str
    filename: str