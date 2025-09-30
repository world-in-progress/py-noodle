CRM_LAUNCHER_IMPORT_TEMPLATE = """
import json
import logging
import argparse
import c_two as cc
from pynoodle import noodle

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""

CRM_LAUNCHER_RUNNING_TEMPLATE = """

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--server_address', type=str, required=True, help='C-Two Server address (e.g., memory://...)')
    parser.add_argument('--icrm_tag', type=str, required=True, help='ICRM class tag for the CRM')
    parser.add_argument('--node_key', type=str, required=True, help='Node key for the SceneNode')
    parser.add_argument('--params', type=str, help='Json-string of parameters for the CRM')
    args = parser.parse_args()
    
    node_key: str = args.node_key
    server_address: str = args.server_address
    crm_params = json.loads(args.params) if args.params else {}

    crm = template.crm(**crm_params)
    icrm = noodle.module_cache.icrm_modules.get(args.icrm_tag).icrm
    config = cc.rpc.ServerConfig(
        name=f'CRM Server for node {node_key}',
        crm=crm,
        icrm=icrm,
        on_shutdown=crm.terminate,
        bind_address=server_address,
    )
    
    server = cc.rpc.Server(config)
    server.start()
"""