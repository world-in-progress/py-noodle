from pydantic import BaseModel

class LockInfo(BaseModel):
    lock_id: str
    node_key: str
    lock_type: str
    access_mode: str

class LockedInfo(BaseModel):
    locked: bool