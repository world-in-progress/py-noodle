import os
import sys
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.pynoodle import noodle, NOODLE_INIT
from tests.icrms.ihello import IHello
from tests.icrms.inames import INames

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    print('\n----- Initialize Noodle ------\n')
    
    NOODLE_INIT()
    
    print('\n----- Mount and import nodes ------\n')
    
    noodle.mount_node('root')
    
    # Mount local nodes: root.names
    noodle.mount_node('root.names', 'test/INames')
    
    # Proxy remote nodes http://127.0.0.1:8000::names as root.names
    # noodle.proxy_node('root.names', 'test/INames', 'http://127.0.0.1:8000', 'names')
    
    # Mount local nodes: root.hello, dependent on remote node http://127.0.0.1:8000::names
    # noodle.mount_node('root.hello', 'test/IHello', launch_params={'names_node_key': 'http://127.0.0.1:8000::names'}, dependent_node_keys_or_infos=['http://127.0.0.1:8000::names'])
    
    # Mount local nodes: root.hello, dependent on local node root.names
    noodle.mount_node('root.hello', 'test/IHello', launch_params={'names_node_key': 'root.names'}, dependent_node_keys_or_infos=['root.names'])

    print('\n----- Connect to nodes ------\n')

    # Connect to remote node http://127.0.0.1:8000::names
    # with noodle.connect_node(INames, 'http://127.0.0.1:8000::names', 'lw') as names:
    
    # Connect to local node root.names
    with noodle.connect_node(INames, 'root.names', 'lw') as names:
        names.crm.add_name('Alice')
        names.crm.add_name('Bob')
        names.crm.add_name('Charlie')
        names.crm.add_name('Dave')

    with noodle.connect_node(IHello, 'root.hello', 'lr') as hello:
        print(hello.server_address)
        crm = hello.crm
        print(crm.greet(0))
        print(crm.greet(1))
        print(crm.greet(2))
        print(crm.greet(3))
    
    print('\n----- Use node directly and error calling ------\n')
    
    hello = noodle.get_node(IHello, 'root.hello', 'pr')
    try:
        crm = hello.crm
        print(crm.greet(8))
    except Exception as e:
        print(f'Error: {e}')
    finally:
        hello.terminate()

    print('\n----- Unmount nodes ------\n')

    noodle.unmount_node('root.names')   # failed, as root.hello depends on it
    noodle.unmount_node('root.hello')   # success, as it is not a dependency of any other node
    noodle.unmount_node('root.names')   # success, as it is not a dependency of any other node