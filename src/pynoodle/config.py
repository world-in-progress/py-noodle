from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Noodle CRM settings
    NOODLE_SERVER_ADDRESS: str = 'thread://noodle'
    NOODLE_CONFIG_PATH: str = './noodle.config.yaml'
    
    # Memory temp directory settings
    MEMORY_TEMP_DIR: str | None = None
    PRE_REMOVE_MEMORY_TEMP_DIR: bool = False