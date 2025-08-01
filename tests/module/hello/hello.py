from src.pynoodle import crm, noodle
from .ihello import IHello
from .inames import INames

@crm
class Hello(IHello):
    def __init__(self, names_node_key: str):
        self.names_node = noodle.get_node(INames, names_node_key, 'lr')
    
    def greet(self, index: int) -> str:
        names = self.names_node.crm.get_names()
        return f'Hello, {names[index]}!'
    
    def terminate(self) -> None:
        self.names_node.terminate()