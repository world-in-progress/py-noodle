from tests.icrms.ihello import IHello
from tests.icrms.inames import INames
from src.pynoodle import crm, Noodle, SceneNode

@crm
class Hello(IHello):
    def __init__(self):
        self.nood = Noodle()
    
    def greet(self, index: int) -> str:
        node = self.nood.get_node(INames, 'names', False, 'l')
        try:
            names = node.crm.get_names()
            return f'Hello, {names[index]}!'
        finally:
            node.terminate()
    
    def terminate(self) -> None:
        pass