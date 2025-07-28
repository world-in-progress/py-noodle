from tests.crms.hello import IHello
from tests.icrms.inames import INames
from src.pynoodle import Noodle, SceneNode

if __name__ == '__main__':
    Noodle.init()
    nood = Noodle()
    
    nood.mount_node('names', 'test/names')
    nood.mount_node('hello', 'test/hello', dependent_node_keys=['names'])

    with nood.connect_node(INames, 'names', True, 'l') as names:
        crm = names.crm
        crm.add_name('Alice')
        crm.add_name('Bob')
        crm.add_name('Charlie')

    with nood.connect_node(IHello, 'hello', True, 'p') as hello:
        print(hello.server_address)
        print(hello.crm.greet(0))
        print(hello.crm.greet(1))
        print(hello.crm.greet(2))
        
    # hello = nood.get_node(IHello, 'hello', True, 'p')
    # try:
    #     crm = hello.crm
    #     print(crm.greet(8))
    # finally:
    #     hello.terminate()