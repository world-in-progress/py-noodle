from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Server settings
    SERVER_PORT: int = 8000
    
    # Noodle CRM settings
    NOODLE_CONFIG_PATH: str = './noodle.config.yaml'
    
    # Scene database settings
    PRE_REMOVE_ALL_LOCKS: bool = True
    
    # Memory temp directory settings
    MEMORY_TEMP_DIR: str | None = None
    PRE_REMOVE_MEMORY_TEMP_DIR: bool = True
    
    class Config:
        env_file = '.env'
        
settings = Settings()