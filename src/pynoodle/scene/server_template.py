CRM_LAUNCHER_IMPORT_TEMPLATE = """
import logging
import argparse
import c_two as cc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""

CRM_LAUNCHER_RUNNING_TEMPLATE = """

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--server_address', type=str, required=True, help='C-Two Server address (e.g., memory://...)')
    parser.add_argument('--node_key', type=str, required=True, help='Node key for the SceneNode')
    parser.add_argument('--params', type=str, help='Json-string of parameters for the CRM')
    args = parser.parse_args()
    
    node_key: str = args.node_key
    server_address: str = args.server_address
    crm_params = json.loads(args.params) if args.params else {}

    crm = CRM(**crm_params)
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
"""