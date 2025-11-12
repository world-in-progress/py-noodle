import os
import sys
from pathlib import Path
from contextlib import asynccontextmanager

# 添加 pynoodle 源代码路径
pynoodle_src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, pynoodle_src_path)

# 添加服务器根路径（用于访问 crms 和 icrms）
server_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, server_root_path)

import uvicorn
from fastapi import FastAPI
from pynoodle import NOODLE_INIT, NOODLE_TERMINATE
from pynoodle.endpoints import node, lock
from pynoodle.noodle import noodle

def create_app():
    """创建 FastAPI 应用"""
    
    @asynccontextmanager
    async def lifespan(app: FastAPI):

        print("应用启动，初始化 Noodle...")
        NOODLE_INIT(app)
        
        # 挂载本地根节点
        try:
            noodle.mount('root', 'names', {'resource_space': './local_names.json'})
            print("本地根节点挂载成功")
        except Exception as e:
            print(f"挂载根节点失败: {e}")
            import traceback
            traceback.print_exc()
        
        yield
        
        # 关闭事件
        print("应用关闭，清理 Noodle...")
        try:
            NOODLE_TERMINATE()
        except Exception as e:
            print(f"关闭 Noodle 失败: {e}")
    
    # 创建应用
    app = FastAPI(lifespan=lifespan)
    
    # 注册路由
    app.include_router(node.router, prefix='/noodle', tags=['node'])
    app.include_router(lock.router, prefix='/lock', tags=['lock'])
    
    return app

def main():
    """启动本地 Noodle 服务"""
    print("初始化 Noodle...")
    
    try:
        # 创建 FastAPI 应用
        app = create_app()
        
        print("启动本地服务在 http://127.0.0.1:8004")
        # 运行 Uvicorn 服务器
        uvicorn.run(
            app,
            host='127.0.0.1',
            port=8004,
            log_level='error'
        )
    except Exception as e:
        print(f"启动服务失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()