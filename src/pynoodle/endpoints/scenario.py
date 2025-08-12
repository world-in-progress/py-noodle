import yaml
import logging
from fastapi import APIRouter, HTTPException

from ..config import settings
from ..schemas.scenario import ScenarioConfiguration, ScenarioNodeDescription

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get('/', response_model=list[ScenarioNodeDescription])
def get_scenario_nodes():
    try:
        # Read configuration
        with open(settings.NOODLE_CONFIG_PATH, 'r') as f:
            config_data = yaml.safe_load(f)
            
        # Parse scenario graph
        config = ScenarioConfiguration(**config_data)
        return config.scenario_nodes
    
    except Exception as e:
        logger.error(f'Error fetching scenario nodes: {e}')
        raise HTTPException(status_code=500, detail='Internal Server Error')