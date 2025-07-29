from pydantic import BaseModel

class SceneNodeInfo(BaseModel):
    node_key: str
    access_info: str | None = None
    scenario_node_name: str | None = None
    children: list['SceneNodeInfo'] | None = None