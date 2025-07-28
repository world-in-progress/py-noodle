import json
from pathlib import Path
import c_two as cc
from src.pynoodle import crm
from tests.icrms.ihello import IHello

@crm
class Hello(IHello):
    def __init__(self):
        self.name_path = Path.cwd() / 'tests' / 'crms' / 'names.json'
        if not self.name_path.exists():
            self.name_path.parent.mkdir(parents=True, exist_ok=True)
            self.names: list[str] = []
        else:
            with open(self.name_path, 'r') as f:
                self.names = json.load(f)['names']
                
    def set_name(self, name: str) -> None:
        if name not in self.names:
            self.names.append(name)
    
    def greet(self, index: int) -> str:
        if index < 0 or index >= len(self.names):
            raise IndexError('Index out of range')
        return f'Hello, {self.names[index]}!'
    
    def terminate(self) -> None:
        with open(self.name_path, 'w') as f:
            json.dump({'names': self.names}, f)