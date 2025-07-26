from pydantic import BaseModel

class ScenarioNodeDescription(BaseModel):
    name: str
    crm_name: str
    crm_module: str
    dependencies: list[str] | None = None

class ScenarioConfiguration(BaseModel):
    scene_path: str
    scenario_nodes: list[ScenarioNodeDescription]