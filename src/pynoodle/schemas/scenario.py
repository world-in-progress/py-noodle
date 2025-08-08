from pydantic import BaseModel

class ScenarioNodeDescription(BaseModel):
    name: str
    dependencies: list[str] | None = None

class ScenarioConfiguration(BaseModel):
    module_prefix: str | None = None
    module_postfix: str | None = None
    scenario_nodes: list[ScenarioNodeDescription]