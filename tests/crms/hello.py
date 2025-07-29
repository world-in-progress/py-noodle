from src.pynoodle import crm, Noodle
from tests.icrms.ihello import IHello
from tests.icrms.inames import INames

@crm
class Hello(IHello):
    def __init__(self, names_node_key: str):
        self.nood = Noodle()
        self.names_node_key = names_node_key
    
    def greet(self, index: int) -> str:
        node = self.nood.get_node(INames, self.names_node_key, 'lr')
        try:
            names = node.crm.get_names()
            return f'Hello, {names[index]}!'
        finally:
            node.terminate()
    
    def terminate(self) -> None:
        pass