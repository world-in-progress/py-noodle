import json
import logging
from pathlib import Path
import pyproj
from icrms.ischema import ISchema

logger = logging.getLogger(__name__)

class Schema(ISchema):
    def __init__(self, resource_space: str):
        """Initialize Schema from resource space (JSON file)"""
        self.resource_path = Path(resource_space)
        if not self.resource_path.exists():
            self.resource_path.parent.mkdir(parents=True, exist_ok=True)
            # 默认值
            self.name = ""
            self.epsg = 4326
            self.alignment_origin = [0.0, 0.0]
            self.grid_info = []
        else:
            with open(self.resource_path, 'r') as f:
                data = json.load(f)
                self.name = data.get('name', '')
                self.epsg = data.get('epsg', 4326)
                self.alignment_origin = data.get('alignment_origin', [0.0, 0.0])
                self.grid_info = data.get('grid_info', [])
    
    def get_epsg(self) -> str:
        # If EPSG is 4326, directly return the PROJ.4 definition for WGS84
        # if self.epsg == 4326:
        # logger.info(f"Using EPSG:4326 coordinates")
        # return '+proj=longlat +datum=WGS84 +no_defs'
        
        try:
            crs = pyproj.CRS.from_epsg(self.epsg)
            proj_string = crs.to_proj4()
            return proj_string
        except Exception as e:
            logger.warning(f'Unable to get PROJ4 representation for EPSG {self.epsg} via pyproj: {e}')
            return f'EPSG:{self.epsg}'

    def get_alignment_point(self) -> tuple[float, float]:
        return tuple(self.alignment_origin)

    def get_level_resolutions(self) -> list[tuple[float, float]]:
        return [tuple(item) for item in self.grid_info]
        
    def update_info(self, info: dict) -> dict:
        try:
            if 'name' in info:
                self.name = info['name']
            if 'epsg' in info:
                self.epsg = info['epsg']
            if 'alignment_origin' in info:
                self.alignment_origin = info['alignment_origin']
            if 'grid_info' in info:
                self.grid_info = info['grid_info']
                
            # Save to file
            self._save_to_file()
            return {'success': True, 'message': 'Schema updated successfully'}
        except Exception as e:
            logger.error(f'Error updating schema: {e}')
            return {'success': False, 'message': str(e)}
            
    def adjust_rules(self, rules: dict) -> dict:
        try:
            self.grid_info = rules.get('grid_info', self.grid_info)
            self._save_to_file()
            return {'success': True, 'message': 'Grid subdivision rules adjusted successfully'}
        except Exception as e:
            logger.error(f'Error adjusting rules: {e}')
            return {'success': False, 'message': str(e)}
    
    def _save_to_file(self) -> None:
        """Save schema data to resource file"""
        data = {
            'name': self.name,
            'epsg': self.epsg,
            'alignment_origin': self.alignment_origin,
            'grid_info': self.grid_info
        }
        with open(self.resource_path, 'w') as f:
            json.dump(data, f, indent=4)
    
    def terminate(self) -> None:
        """Save data when terminating"""
        self._save_to_file()