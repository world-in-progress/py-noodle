import os
import sys
import uvicorn
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pynoodle import noodle, NOODLE_INIT, NOODLE_TERMINATE
from tests.icrms.inames import INames

logging.basicConfig(level=logging.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI):
    NOODLE_INIT(app)

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