import os
import sys
import uvicorn
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.pynoodle import Noodle, NOODLE_INIT
from tests.crms.hello import IHello
from tests.icrms.inames import INames

logging.basicConfig(level=logging.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI):
    NOODLE_INIT(app)

    nood = Noodle()
    nood.mount_node('names', 'test/names')
    with nood.connect_node(INames, 'names', 'lw') as names:
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
    uvicorn.run('tests.remote:app', host='0.0.0.0', port=8000)