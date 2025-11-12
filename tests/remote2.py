import os
import sys
import uvicorn
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

# Setup Python path for imports
# Add pynoodle src to sys.path
pynoodle_src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, pynoodle_src_path)
# Add server root to sys.path for accessing crms and icrms
server_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, server_root_path)

from pynoodle import noodle, NOODLE_INIT, NOODLE_TERMINATE
from icrms.inames import INames

logging.basicConfig(level=logging.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI):
    NOODLE_INIT(app)

    # 挂载一个测试节点
    noodle.mount('nameSet', 'names') 
    with noodle.connect(INames, 'nameSet', 'lw') as names:
        names.add_name('Alice')
        names.add_name('Bob')
        names.add_name('Charlie')

    yield
    
    noodle.unmount('nameSet')
    NOODLE_TERMINATE()

def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title='Remote_Noodle_Test',
        version='0.1.0',
        lifespan=lifespan,
    )
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    )
    return app

app = create_app()

if __name__ == '__main__':
    uvicorn.run('remote2:app', host='127.0.0.1', port=8005, reload=True)