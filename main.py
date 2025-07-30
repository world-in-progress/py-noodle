import uvicorn
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

from src.pynoodle import Noodle, NOODLE_INIT
from tests.crms.hello import IHello
from tests.icrms.inames import INames

logging.basicConfig(level=logging.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI):
    NOODLE_INIT(app)

    nood = Noodle()
    nood.mount_node('names', 'test/names')
    with nood.connect_node(INames, 'root.names', 'lw') as names:
        crm = names.crm
        crm.add_name('Alice')
        crm.add_name('Bob')
        crm.add_name('Charlie')
    
    yield

def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title='Noodle_Test',
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
    # uvicorn.run('main:app', host='0.0.0.0', port=8000)

    NOODLE_INIT()
    nood = Noodle()
    
    nood.mount_node('root')
    nood.mount_node('root.names', 'test/names')
    nood.mount_node('root.hello', 'test/hello', launch_params={'names_node_key': 'root.names'}, dependent_node_keys_or_infos=['root.names'])

    with nood.connect_node(INames, 'root.names', 'lw') as names:
        crm = names.crm
        crm.add_name('Alice')
        crm.add_name('Bob')
        crm.add_name('Charlie')

    with nood.connect_node(IHello, 'root.hello', 'lr') as hello:
        print(hello.server_address)
        print(hello.crm.greet(0))
        print(hello.crm.greet(1))
        print(hello.crm.greet(2))
    
    nood.unmount_node('root.names')
    nood.unmount_node('root.hello')
    nood.unmount_node('root.names')
        
    # hello = nood.get_node(IHello, 'hello', True, 'p')
    # try:
    #     crm = hello.crm
    #     print(crm.greet(8))
    # finally:
    #     hello.terminate()