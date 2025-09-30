import sys
import logging
from pathlib import Path

test_module_path = Path.cwd()
sys.path.insert(0, str(test_module_path))

from tests.icrms.inames import INames
from pynoodle import noodle, NOODLE_INIT, NOODLE_TERMINATE

logging.basicConfig(level=logging.INFO)

NODE_KEY = 'root.names'
# NODE_KEY = 'http://127.0.0.1:8000::nameSet'

if __name__ == '__main__':
    NOODLE_INIT()
    
    print('\n----- Mount nodes ------\n')
    
    noodle.mount_node('root')
    
    # Mount local nodes: root.names
    if NODE_KEY == 'root.names':
        noodle.mount_node(NODE_KEY, 'names')
    
    print('\n----- Access node ------\n')
    
    # Connect to local node root.names
    with noodle.connect(INames, NODE_KEY, 'pw') as names:
        names.add_name('Alice')
        names.add_name('Bob')
        names.add_name('Charlie')
        names.add_name('Noodle1')
        print(names.get_names())

    with noodle.connect(INames, NODE_KEY, 'lw') as names:
        print(names.get_names())
        names.remove_name('Noodle1')
        print(names.get_names())
    
    print('\n----- Link to node and access ------\n')
    
    lock_id = noodle.link(INames, NODE_KEY, 'w')
    names = noodle.access(INames, NODE_KEY, lock_id)
    
    print(names.get_names())
    names.add_name('Noodle1')
    print(names.get_names())
    names.remove_name('Noodle1')
    print(names.get_names())
    
    noodle.unlink(NODE_KEY, lock_id)
    
    print('\n----- Link to node and use context manager ------\n')
    
    lock_id = noodle.link(INames, NODE_KEY, 'w')
    
    with noodle.connect(INames, NODE_KEY, 'lw', lock_id=lock_id) as names:
        print(names.get_names())
        names.add_name('Noodle1')
        print(names.get_names())
        names.remove_name('Noodle1')
        print(names.get_names())
    
    noodle.unlink(NODE_KEY, lock_id)

    if NODE_KEY == 'root.names':
        print('\n----- Unmount nodes ------\n')
        
        noodle.unmount_node('root.names')
        noodle.unmount_node('root')
    
    NOODLE_TERMINATE()