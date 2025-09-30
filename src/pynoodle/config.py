from pathlib import Path
from pydantic import field_validator
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Scene database settings
    SQLITE_PATH: Path = Path('./noodle.db')
    
    # Memory temp directory settings
    MEMORY_TEMP_PATH: Path = Path('./temp')
    
    # Noodle module settings
    NOODLE_CONFIG_PATH: Path = Path('./noodle.config.yaml')
    
    @field_validator('NOODLE_CONFIG_PATH')
    @classmethod
    def validate_noodle_config_path(cls, v: str) -> Path:
        path = Path(v)
        if not path.is_absolute():
            path = Path.cwd() / path
            
        if not path.exists():
            raise FileNotFoundError(f'No such file or directory: {path}')
        
        return path

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
        extra = 'ignore'
        
settings = Settings()