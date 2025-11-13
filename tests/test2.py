import os
import sys
import json
import httpx
import logging
import subprocess
import time
from pathlib import Path

pynoodle_src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, pynoodle_src_path)
server_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, server_root_path)

from pynoodle import noodle, NOODLE_INIT, NOODLE_TERMINATE
logging.basicConfig(level=logging.INFO)

LOCAL_SERVER_URL = 'http://127.0.0.1:8004'
REMOTE_SERVER_URL = 'http://127.0.0.1:8005'

def wait_for_server(url, timeout=30):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = httpx.get(f'{url}/noodle?node_key=root', timeout=2.0)
            if response.status_code in [200, 404]:
                print(f"服务器 {url} 已启动")
                return True
        except Exception as e:
            print(f"等待服务器... ({int(time.time() - start_time)}s)")
        time.sleep(1)
    
    print(f"服务器 {url} 启动超时")
    return False

def start_local_server():
    print("启动本地服务器...")
    server_script = os.path.join(os.path.dirname(__file__), 'local2.py')
    
    if not os.path.exists(server_script):
        print(f"错误: {server_script} 文件不存在")
        return None
    
    try:
        # 启动服务器进程，显式指定环境编码
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        
        process = subprocess.Popen(
            [sys.executable, server_script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=os.path.dirname(server_script), #确保子进程使用与父进程相同的工作目录
            env=env
        )
        
        # 检查进程是否立即退出
        time.sleep(1)
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            print(f"服务器进程意外退出，返回码: {process.returncode}")
            if stdout:
                print(f"标准输出: {stdout.decode('utf-8')}")
            if stderr:
                print(f"错误输出: {stderr.decode('utf-8')}")
            return None
        
        # 等待服务器启动
        if wait_for_server(LOCAL_SERVER_URL, timeout=30):
            return process
        else:
            print("服务器启动失败，尝试读取错误信息...")
            try:
                process.terminate()
                stdout, stderr = process.communicate(timeout=5)
                if stdout:
                    print(f"服务器标准输出:\n{stdout.decode('utf-8', errors='ignore')}")
                if stderr:
                    print(f"服务器错误输出:\n{stderr.decode('utf-8', errors='ignore')}")
            except:
                pass
            return None
    except Exception as e:
        print(f"启动服务器过程中出错: {e}")
        return None

def test_pull_functionality():
    """测试 pull 功能"""
    print("开始测试 pull 功能...")
    
    try:
        print("挂载本地根节点...")
        # 通过 API 挂载根节点
        response = httpx.post(
            f'{LOCAL_SERVER_URL}/noodle/mount',
            params={
                'node_key': 'root',
                'node_template_name': 'names',
                'mount_params': json.dumps({'resource_space': './local_names.json'})
            },
            timeout=10.0
        )
        
        if response.status_code != 200:
            print(f"挂载失败: {response.status_code}")
            print(f"错误信息: {response.text}")
            return
        
        print("本地根节点挂载成功")
        
        # 配置拉取参数
        template_name = 'names'
        target_node_key = 'root.pulledNode'
        source_node_key = 'testNode'
        mount_params = '{}'
        
        print(f"准备从 {REMOTE_SERVER_URL} 拉取节点 {source_node_key} 到本地 {target_node_key}")
        
        # 调用 pull 接口
        params = {
            'template_name': template_name,
            'target_node_key': target_node_key,
            'source_node_key': source_node_key,
            'remote_noodle_url': REMOTE_SERVER_URL,
            'mount_params': mount_params
        }
        
        response = httpx.post(
            f'{LOCAL_SERVER_URL}/noodle/pull',
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
            
    except Exception as e:
        print(f"测试过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 清理工作
        try:
            httpx.post(f'{LOCAL_SERVER_URL}/noodle/unmount', params={'node_key': 'root.pulledNode'}, timeout=10.0)
        except:
            pass
        
        try:
            httpx.post(f'{LOCAL_SERVER_URL}/noodle/unmount', params={'node_key': 'root'}, timeout=10.0)
        except:
            pass
        
        print("测试完成")

if __name__ == '__main__':
    local_server = start_local_server()
    
    if local_server:
        try:
            time.sleep(2)
            
            test_pull_functionality()
        finally:
            print("关闭本地服务器...")
            try:
                if sys.platform == 'win32':
                    os.killpg(os.getpgid(local_server.pid), 9)
                else:
                    local_server.terminate()
                    local_server.wait(timeout=5)
            except:
                try:
                    local_server.kill()
                except:
                    pass
    else:
        print("无法启动本地服务器")