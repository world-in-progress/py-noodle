import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class Names:
    def __init__(self, resource_space: str):
        self.name_path = Path(resource_space)
        if not self.name_path.exists():
            self.name_path.parent.mkdir(parents=True, exist_ok=True)
            self.names: list[str] = []
        else:
            with open(self.name_path, 'r') as f:
                self.names = json.load(f)['names']
    
    def get_names(self) -> list[str]:
        return self.names

    def add_name(self, name: str) -> None:
        """Add a name to the list."""
        if name in self.names:
            logger.info(f'Name "{name}" already exists, skipping addition.')
            return
        self.names.append(name)

    def remove_name(self, name: str) -> None:
        """Remove a name from the list."""
        self.names.remove(name)
    
    def terminate(self) -> None:
        """Save names to the file."""
        with open(self.name_path, 'w') as f:
            json.dump({'names': self.names}, f, indent=4)