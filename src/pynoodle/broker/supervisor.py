import os
import zmq
import logging

logger = logging.getLogger(__name__)

def supervisor_process():
    logger.info('Start Noodle broker...')
    try:
        sub_port = int(os.getenv('SUB_PORT', 5559))
        pub_port = int(os.getenv('PUB_PORT', 5560))
        
        context = zmq.Context()
        xsub_socket = context.socket(zmq.XSUB)
        xsub_socket.bind(f'tcp://*:{sub_port}')
        
        xpub_socket = context.socket(zmq.XPUB)
        xpub_socket.bind(f'tcp://*:{pub_port}')

        logger.info('Noodle broker is running...')
        zmq.proxy(xsub_socket, xpub_socket)
        
    except Exception as e:
        logger.error(f'Error happened in supervisor process: {e}')
    
    finally:
        xsub_socket.close()
        xpub_socket.close()
        context.term()
        logger.info('Noodle broker stopped.')