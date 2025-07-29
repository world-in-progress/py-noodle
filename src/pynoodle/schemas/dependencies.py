from typing import Literal
from pydantic import BaseModel

class DependencyRequest(BaseModel):
    method: Literal['ADD', 'REMOVE']
    node_key: str
    dependent_key: str
    dependent_url: str | None = None
    
    @property
    def dependent_node_key(self) -> str:
        if self.dependent_url is None:
            return self.dependent_key
        return f'{self.dependent_url}::{self.dependent_key}'