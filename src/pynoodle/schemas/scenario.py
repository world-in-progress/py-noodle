from pydantic import BaseModel

class ScenarioNodeDescription(BaseModel):
    name: str
    dependencies: list[str] | None = None

class ScenarioConfiguration(BaseModel):
    module_root: str | None = None
    scenario_nodes: list[ScenarioNodeDescription]