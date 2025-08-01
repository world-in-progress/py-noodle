from pydantic import BaseModel

class ScenarioNodeDescription(BaseModel):
    name: str
    module: str
    dependencies: list[str] | None = None

class ScenarioConfiguration(BaseModel):
    scenario_nodes: list[ScenarioNodeDescription]