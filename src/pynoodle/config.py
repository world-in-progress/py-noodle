from pathlib import Path
from pydantic import field_validator
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Server settings
    SERVER_PORT: int = 8000
    
    # Noodle CRM settings
    NOODLE_CONFIG_PATH: str = './noodle.config.yaml'
    
    # Scene database settings
    SQLITE_PATH: Path
    PRE_REMOVE_ALL_LOCKS: bool = True
    
    # Memory temp directory settings
    MEMORY_TEMP_PATH: Path = Path('./temp')
    PRE_REMOVE_MEMORY_TEMP_PATH: bool = True

    @field_validator('SQLITE_PATH', 'MEMORY_TEMP_PATH', mode='before')
    @classmethod
    def validate_path(cls, p: str) -> Path:
        path = Path(p)
        if not path.is_absolute():
            path = Path.cwd() / path
        
        path.resolve()
            
        if not path.exists():
            if path.suffix:
                path.parent.mkdir(parents=True, exist_ok=True)
            else:
                path.mkdir(parents=True, exist_ok=True)
            
        return path

    class Config:
        env_file = '.env'
        
settings = Settings()