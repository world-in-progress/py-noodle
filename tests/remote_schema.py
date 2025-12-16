import os
import sys
import uvicorn
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

pynoodle_src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, pynoodle_src_path)
# Add server root to sys.path for accessing crms and icrms
server_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, server_root_path)

from pynoodle import noodle, NOODLE_INIT, NOODLE_TERMINATE
from tests.icrms.ischema import ISchema

logging.basicConfig(level=logging.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI):
    NOODLE_INIT(app)

    noodle.mount('.schemaSet', 'schema')
    with noodle.connect(ISchema, '.schemaSet', 'lw') as schema:
        schema.update_info({'name': 'TestSchema_8002'})
        schema.update_info({'epsg': '4326'})
        schema.update_info({'alignment_origin': [1.0, 2.0]})
        schema.update_info({'grid_info': [(256.0, 256.0), (256.0, 256.0)]})
        logging.info("Updated Schema name to 'TestSchema_8002'")

    yield
    
    noodle.unmount('.schemaSet')
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
    # Run using the app instance so the file can be executed directly
    uvicorn.run(app, host='127.0.0.1', port=8002)