import logging
import argparse
import c_two as cc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from tests.crms.patch import Patch as CRM

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--node_key', type=str, required=True, help='Node key for the SceneNode')
    parser.add_argument('--params', type=str, help='Json-string of parameters for the CRM')
    parser.add_argument()
    args = parser.parse_args()
    
    node_key: str = args.node_key
    crm_params: str = args.params if args.params else None
    
    server_address = f'memory://{node_key.replace(".", "_")}'
    crm = CRM(**cc.json.loads(crm_params)) if crm_params else CRM()
    server = cc.rpc.Server(server_address, crm, node_key)

    logger.info(f'Starting CRM Server for node {node_key}...')
    server.start()
    logger.info(f'CRM Server for node {node_key} started at %s', server_address)
    try:
        server.wait_for_termination()
        server.stop()
    except KeyboardInterrupt:
        logger.info(f'KeyboardInterrupt received, terminating CRM Server for node {node_key}...')
        server.stop()
    finally:
        logger.info(f'CRM Server for node {node_key} terminated.')