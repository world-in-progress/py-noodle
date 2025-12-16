import c_two as cc
from typing import Any, Tuple, List

@cc.icrm()
class ISchema:
    """
    ICRM
    =
    Interface of Core Resource Model (ICRM) specifies how to interact with CRM. 
    """
    def get_epsg(self) -> str:
        """
        Get EPSG coordinate system code
        
        Returns:
            str: EPSG coordinate system code string
        """
        ...

    def get_alignment_point(self) -> Tuple[float, float]:
        """
        Get the longitude and latitude coordinates of the alignment point
        
        Returns:
            Tuple[float, float]: Alignment point coordinates (longitude, latitude)
        """
        ...
        
    def get_level_resolutions(self) -> List[Tuple[float, float]]:
        """
        Get resolution information for each level
        
        Returns:
            List[Tuple[float, float]]: Resolution information for each level [(width, height), ...]
        """
        ...
        
    def update_info(self, info: dict) -> dict:
        """
        Update Schema information
        Parameters:
            info (dict): A dictionary containing the information to be updated
        Returns:
            dict: Operation result, including success status and message
        """
        ...
        
    def adjust_rules(self, rules: dict) -> dict:
        """
        Adjust grid subdivision rules
        Parameters:
            rules (dict): A dictionary containing new grid subdivision rules
        Returns:
            dict: Operation result, including success status and message
        """
        ...