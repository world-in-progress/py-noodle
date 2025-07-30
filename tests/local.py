import os
import sys
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.pynoodle import noodle, NOODLE_INIT
from tests.crms.hello import IHello
from tests.icrms.inames import INames

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    NOODLE_INIT()
    
    noodle.mount_node('root')
    # noodle.mount_node('root.names', 'test/names')
    noodle.import_node('root.names', 'test/names', 'http://127.0.0.1:8000', 'names')
    # noodle.mount_node('root.hello', 'test/hello', launch_params={'names_node_key': 'http://127.0.0.1:8000::names'}, dependent_node_keys_or_infos=['http://127.0.0.1:8000::names'])
    noodle.mount_node('root.hello', 'test/hello', launch_params={'names_node_key': 'root.names'}, dependent_node_keys_or_infos=['root.names'])

    with noodle.connect_node(INames, 'root.names', 'lw') as names:
        crm = names.crm
        crm.add_name('Dave')
    #     crm.add_name('Alice')
    #     crm.add_name('Bob')
    #     crm.add_name('Charlie')

    with noodle.connect_node(IHello, 'root.hello', 'pr') as hello:
        print(hello.server_address)
        crm = hello.crm
        print(crm.greet(0))
        print(crm.greet(1))
        print(crm.greet(2))
        print(crm.greet(3))
        # print(hello.crm.greet(0))
        # print(hello.crm.greet(1))
        # print(hello.crm.greet(2))
    
    noodle.unmount_node('root.names')
    noodle.unmount_node('root.hello')
    noodle.unmount_node('root.names')
        
    # hello = noodle.get_node(IHello, 'hello', True, 'p')
    # try:
    #     crm = hello.crm
    #     print(crm.greet(8))
    # finally:
    #     hello.terminate()