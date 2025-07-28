# from src.pynoodle.treeger.crm import Treeger
from tests.crms.hello import IHello, Hello
from src.pynoodle import Noodle, SceneNode

if __name__ == '__main__':
    Noodle.init()
    nood = Noodle()
    
    nood.treeger.mount_node('hello', 'test/IHello')
    
    node: SceneNode[Hello] = nood.treeger.get_node('hello', True, 'p')
    print(node.server_address)
    crm = node.crm
    crm.set_name('World')
    print(node.crm.greet(0))
    
    node.terminate()