import os
import sys
import json
import httpx
import logging
from pathlib import Path

print("Running as file:", __file__)

pynoodle_src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, pynoodle_src_path)

from pynoodle import noodle, NOODLE_INIT, NOODLE_TERMINATE
from icrms.inames import INames

logging.basicConfig(level=logging.INFO)

NODE_KEY = 'root.names'
# NODE_KEY = 'http://127.0.0.1:8000::nameSet'

if __name__ == '__main__':
    NOODLE_INIT()
    
    print('\n----- Mount nodes ------\n')
    
    # Mount local node: root
    noodle.mount('root')
    
    # Mount node: root.names
    if NODE_KEY == 'root.names':
        noodle.mount(NODE_KEY, 'names')
    
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
    
    print('\n----- Test Pull ------\n')
# 配置拉取参数
template_name = 'names'
target_node_key = 'root.pulledNode'
source_node_key = 'testNode'
remote_noodle_url = 'http://127.0.0.1:8000'  # 远程服务器URL
mount_params = '{}'

print(f"准备从 {remote_noodle_url} 拉取节点 {source_node_key} 到本地 {target_node_key}")

# 直接调用远程服务器的pull接口
params = {
    'template_name': template_name,
    'target_node_key': target_node_key,
    'source_node_key': source_node_key,
    'remote_noodle_url': remote_noodle_url,
    'mount_params': mount_params
}

# 直接向远程服务器发送请求
response = httpx.post(
    f'{remote_noodle_url}/noodle/pull',
    params=params,
    timeout=30.0
)

if response.status_code == 200:
    print("Pull 操作成功!")
    result = response.json()
    print(f"响应结果: {json.dumps(result, indent=2)}")
else:
    print(f"Pull 操作失败，状态码: {response.status_code}")
    print(f"错误信息: {response.text}")
    
    print(noodle.access(INames, NODE_KEY).get_names())

    if NODE_KEY == 'root.names':
        print('\n----- Unmount nodes ------\n')
        
        noodle.unmount('root.names')
        noodle.unmount('root')
    
    NOODLE_TERMINATE()