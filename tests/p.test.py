from src.pynoodle import Noodle, SceneNode
from tests.crms.hello import IHello, Hello

if __name__ == '__main__':
    nood = Noodle()
    
    nood.get_node('hello', True, 'l')