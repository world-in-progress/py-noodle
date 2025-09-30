from pydantic import BaseModel

class LockInfo(BaseModel):
    lock_id: str

class LockedInfo(BaseModel):
    locked: bool