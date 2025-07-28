import os
import zmq
import uuid
import json
import sqlite3
import threading
import subprocess
import c_two as cc
from pydantic import BaseModel
from typing import TypeVar, Generic
from contextlib import contextmanager
from dataclasses import dataclass, field

from ..scenario import ScenarioNode

T = TypeVar('T')

SERVING_TABLE = 'serving'
NODE_KEY = 'node_key'
CONNECTION_COUNT = 'connection_count'

class NodeMessage(BaseModel):
    sender_id: str
    action: str

@dataclass
class SceneNodeRecord:
    node_key: str
    scenario_node: ScenarioNode | None   # None if this is a resource set node, not a resource node
    launch_params: str
    
    parent_key: str | None = None
    children: list['SceneNodeRecord'] = field(default_factory=list)
    
    def add_child(self, child: 'SceneNodeRecord'):
        self.children.append(child)
        self.children.sort(key=lambda child: child.node_key.split('.')[-1].lower())  # sort children by their name
        child.parent_key = self.node_key
    
    def add_children(self, children: list['SceneNodeRecord']):
        for child in children:
            self.add_child(child)
    
    @property
    def is_set(self):
        return self.scenario_node is None

class SceneNode(Generic[T]):
    def __init__(self, crm_or_icrm: T, scene_path: str, record: SceneNodeRecord):
        self.scene_path = scene_path
        self.node_key = record.node_key
        self._instance_id = str(uuid.uuid4())
        self._crm_class = record.scenario_node.crm_class
        self._crm_params = json.loads(record.launch_params)

        sub_port = int(os.getenv('SUB_PORT', 5559))
        pub_port = int(os.getenv('PUB_PORT', 5560))
        self._context = zmq.Context()
        
        self._pub_socket = self._context.socket(zmq.PUB)
        self._pub_socket.connect(f'tcp://localhost:{pub_port}')
        
        self._sub_socket = self._context.socket(zmq.SUB)
        self._sub_socket.connect(f'tcp://localhost:{sub_port}')
        self._sub_socket.setsockopt_string(zmq.SUBSCRIBE, self.node_key)
        
        self._stop_event = threading.Event()
        server_thread = threading.Thread(target=self._serve, daemon=True)
        server_thread.start()
    
    @property
    def server_address(self) -> str:
        return f'memory://{str.join("_", self.node_key.split("."))}'
    
    @contextmanager
    def _connect_db(self):
        """Context manager for database connection."""
        conn = sqlite3.connect(self.scene_path)
        conn.row_factory = sqlite3.Row  # enable column access by name
        try:
            yield conn
        finally:
            conn.close()
    
    def _decrement_node_connection(self, node_key: str) -> None:
        """Decrement the connection count for a node in the serving table"""
        # Get connection count
        with self._connect_db() as conn:
            cursor = conn.execute(f'SELECT {CONNECTION_COUNT} FROM {SERVING_TABLE} WHERE {NODE_KEY} = ?', (node_key,))
            row = cursor.fetchone()
            if row is None:
                return
            connection_count = row[CONNECTION_COUNT] - 1
            if connection_count <= 0:
                # Delete the node from the serving table if connection count is 0
                conn.execute(f'DELETE FROM {SERVING_TABLE} WHERE {NODE_KEY} = ?', (node_key,))
                # Shutdown the CRM server
                cc.rpc.Client.shutdown(f'memory://{str.join("_", node_key.split("."))}', timeout=60)
            else:
                # Update the connection count
                conn.execute(f'UPDATE {SERVING_TABLE} SET {CONNECTION_COUNT} = ? WHERE {NODE_KEY} = ?', (connection_count, node_key))
    
    def _serve(self):
        while not self._stop_event.is_set():
            try:
                if self._sub_socket.poll(timeout=1000):
                    _, payload = self._sub_socket.recv_multipart()
                    message = NodeMessage(**json.loads(payload.decode('utf-8')))
                    if message.sender_id != self._instance_id:
                        self._handle_message(message)
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    break
                else:
                    raise
    
    def _handle_recreation(self):
        self._wrapped = self._crm_class(**self.crm_params)
    
    def _handle_message(self, message: NodeMessage):
        if message.action == 'recreate':
            self._handle_recreation()
        elif message.action == 'terminate':
            self.terminate()
        
    @property
    def icrm(self) -> T:
        return self._wrapped
    
    def terminate(self):
        self._stop_event.set()
        self._listener_thread.join(timeout=2)
        self._pub_socket.close()
        self._sub_socket.close()
        self._context.term()