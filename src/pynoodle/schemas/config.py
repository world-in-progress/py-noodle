from pydantic import BaseModel

class ICRMDescription(BaseModel):
    tag: str
    module_path: str

class ResourceNodeTemplateDescription(BaseModel):
    name: str
    module_path: str | None = None

class NoodleConfiguration(BaseModel):
    icrms: list[ICRMDescription] = []
    node_templates: list[ResourceNodeTemplateDescription] = []