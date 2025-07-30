from pydantic import BaseModel

class ScenarioNodeDescription(BaseModel):
    name: str
    crm_name: str
    crm_module: str
    dependencies: list[str] | None = None

class ScenarioConfiguration(BaseModel):
    scenario_nodes: list[ScenarioNodeDescription]