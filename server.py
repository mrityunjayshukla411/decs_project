import socket
import json
import threading
import hashlib
import time
from typing import Dict, List, Tuple, Optional, Set
from collections import OrderedDict
import argparse

class LRUCache:
    """Least Recently Used (LRU) cache implementation"""
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.cache = OrderedDict()
        self.lock = threading.Lock()

    def get(self, key: str) -> Optional[str]:
        """Get item from cache and move it to the end (most recently used)"""
        with self.lock:
            if key in self.cache:
                value = self.cache.pop(key)
                self.cache[key] = value
                return value
            return None

    def put(self, key: str, value: str):
        """Add item to cache, remove least recently used if at capacity"""
        with self.lock:
            if key in self.cache:
                self.cache.pop(key)
            elif len(self.cache) >= self.capacity:
                self.cache.popitem(last=False)
            self.cache[key] = value

    def remove(self, key: str):
        """Remove item from cache"""
        with self.lock:
            if key in self.cache:
                del self.cache[key]

class Prefetcher:
    """Implements prefetching mechanism for frequently accessed keys"""
    def __init__(self, threshold: int = 5):
        self.access_counts = {}  # Track access frequency
        self.prefetch_keys = set()  # Keys to prefetch
        self.threshold = threshold  # Minimum accesses to trigger prefetching
        self.lock = threading.Lock()

    def record_access(self, key: str):
        """Record access to a key"""
        with self.lock:
            self.access_counts[key] = self.access_counts.get(key, 0) + 1
            if self.access_counts[key] >= self.threshold:
                self.prefetch_keys.add(key)

    def should_prefetch(self, key: str) -> bool:
        """Check if key should be prefetched"""
        return key in self.prefetch_keys

class DistributedNode:
    def __init__(self, host: str, port: int, cluster_nodes: List[Tuple[str, int]] = None):
        self.host = host
        self.port = port
        self.node_id = self.generate_node_id(f"{host}:{port}")
        self.data: Dict[str, str] = {}
        self.lock = threading.Lock()
        self.cluster_nodes = set()
        self.virtual_nodes = 3

        # Initialize cache and prefetcher
        self.cache = LRUCache(capacity=1000)  # Cache for 1000 items
        self.prefetcher = Prefetcher(threshold=5)
        
        # Prefetch queue and worker thread
        self.prefetch_queue: List[str] = []
        self.prefetch_lock = threading.Lock()
        self.prefetch_thread = threading.Thread(target=self._prefetch_worker, daemon=True)
        self.prefetch_thread.start()

        # Add self to cluster nodes
        self.add_node((host, port))
        # Add other known nodes
        if cluster_nodes:
            for node in cluster_nodes:
                self.add_node(node)

    def _prefetch_worker(self):
        """Background worker to handle prefetching"""
        while True:
            with self.prefetch_lock:
                if self.prefetch_queue:
                    key = self.prefetch_queue.pop(0)
                    responsible_node = self.get_responsible_node(key)
                    if responsible_node != (self.host, self.port):
                        try:
                            value = self._fetch_from_node(key, *responsible_node)
                            if value:
                                self.cache.put(key, value)
                        except Exception as e:
                            print(f"Prefetch error for key {key}: {e}")
            time.sleep(0.1)  # Avoid busy waiting

    def _fetch_from_node(self, key: str, host: str, port: int) -> Optional[str]:
        """Fetch value from another node"""
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((host, port))
            
            request = {
                'operation': 'GET',
                'key': key,
                'forwarded': True
            }
            
            client_socket.send(json.dumps(request).encode('utf-8'))
            response = json.loads(client_socket.recv(1024).decode('utf-8'))
            client_socket.close()
            
            if response['status'] == 'success':
                return response['value']
            return None
        except Exception:
            return None

    def queue_prefetch(self, key: str):
        """Add key to prefetch queue"""
        with self.prefetch_lock:
            if key not in self.prefetch_queue:
                self.prefetch_queue.append(key)

    def get_value(self, key: str, forwarded: bool = False) -> Optional[str]:
        """Get value with caching and prefetching"""
        # Check cache first
        cached_value = self.cache.get(key)
        if cached_value is not None:
            return cached_value

        # Record access for prefetching
        self.prefetcher.record_access(key)

        # Get from storage
        with self.lock:
            value = self.data.get(key)
            if value is not None:
                # Update cache
                self.cache.put(key, value)
                # Queue prefetch of related keys
                if self.prefetcher.should_prefetch(key):
                    self.queue_prefetch(key)
            return value

    def process_operation(self, operation: str, key: str, value: str = None, forwarded: bool = False) -> dict:
        """Process the requested operation with caching"""
        if not forwarded:
            responsible_node = self.get_responsible_node(key)
            if responsible_node != (self.host, self.port):
                try:
                    return self.forward_request(operation, key, value, *responsible_node)
                except Exception as e:
                    return {'status': 'error', 'message': f'Failed to forward request: {str(e)}'}

        with self.lock:
            if operation == 'GET':
                value = self.get_value(key, forwarded)
                if value is not None:
                    return {'status': 'success', 'value': value}
                return {'status': 'error', 'message': 'Key not found'}

            elif operation == 'PUT':
                self.data[key] = value
                self.cache.put(key, value)
                return {'status': 'success', 'message': 'Key-value pair added'}

            elif operation == 'DELETE':
                if key in self.data:
                    del self.data[key]
                    self.cache.remove(key)
                    return {'status': 'success', 'message': 'Key-value pair deleted'}
                return {'status': 'error', 'message': 'Key not found'}

            elif operation == 'UPDATE':
                if key in self.data:
                    self.data[key] = value
                    self.cache.put(key, value)
                    return {'status': 'success', 'message': 'Value updated'}
                return {'status': 'error', 'message': 'Key not found'}

            else:
                return {'status': 'error', 'message': 'Invalid operation'}
                
    def generate_node_id(self, node_string: str) -> int:
        """Generate a consistent hash for the node."""
        return int(hashlib.md5(node_string.encode()).hexdigest(), 16)
    
    def get_responsible_node(self, key: str) -> Tuple[str, int]:
        """Determine which node is responsible for a given key."""
        if not self.cluster_nodes:
            return (self.host, self.port)
            
        key_hash = int(hashlib.md5(key.encode()).hexdigest(), 16)
        nodes = sorted(self.cluster_nodes)
        for node in nodes:
            node_hash = self.generate_node_id(f"{node[0]}:{node[1]}")
            if key_hash <= node_hash:
                return node
        return nodes[0]  # Wrap around to first node
        
    def add_node(self, node: Tuple[str, int]):
        """Add a node to the cluster."""
        with self.lock:
            self.cluster_nodes.add(node)
            self.rebalance_data()
            
    def remove_node(self, node: Tuple[str, int]):
        """Remove a node from the cluster."""
        with self.lock:
            if node in self.cluster_nodes:
                self.cluster_nodes.remove(node)
                self.rebalance_data()
                
    def rebalance_data(self):
        """Rebalance data across nodes when cluster membership changes."""
        if not self.data:
            return
            
        items_to_redistribute = []
        for key, value in self.data.items():
            responsible_node = self.get_responsible_node(key)
            if responsible_node != (self.host, self.port):
                items_to_redistribute.append((key, value))
                
        for key, value in items_to_redistribute:
            self.forward_data_to_node(key, value, *self.get_responsible_node(key))
            del self.data[key]
            
    def forward_data_to_node(self, key: str, value: str, host: str, port: int):
        """Forward data to the responsible node."""
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((host, port))
            
            request = {
                'operation': 'PUT',
                'key': key,
                'value': value,
                'forwarded': True
            }
            
            client_socket.send(json.dumps(request).encode('utf-8'))
            client_socket.close()
        except Exception as e:
            print(f"Error forwarding data to {host}:{port}: {e}")
            
    # def process_operation(self, operation: str, key: str, value: str = None, forwarded: bool = False) -> dict:
    #     """Process the requested operation."""
    #     if not forwarded:
    #         responsible_node = self.get_responsible_node(key)
    #         if responsible_node != (self.host, self.port):
    #             try:
    #                 return self.forward_request(operation, key, value, *responsible_node)
    #             except Exception as e:
    #                 return {'status': 'error', 'message': f'Failed to forward request: {str(e)}'}
        
    #     with self.lock:
    #         if operation == 'GET':
    #             if key in self.data:
    #                 return {'status': 'success', 'value': self.data[key]}
    #             return {'status': 'error', 'message': 'Key not found'}
                
    #         elif operation == 'PUT':
    #             self.data[key] = value
    #             return {'status': 'success', 'message': 'Key-value pair added'}
                
    #         elif operation == 'DELETE':
    #             if key in self.data:
    #                 del self.data[key]
    #                 return {'status': 'success', 'message': 'Key-value pair deleted'}
    #             return {'status': 'error', 'message': 'Key not found'}
                
    #         elif operation == 'UPDATE':
    #             if key in self.data:
    #                 self.data[key] = value
    #                 return {'status': 'success', 'message': 'Value updated'}
    #             return {'status': 'error', 'message': 'Key not found'}
                
    #         else:
    #             return {'status': 'error', 'message': 'Invalid operation'}
                
    def forward_request(self, operation: str, key: str, value: str, host: str, port: int) -> dict:
        """Forward request to responsible node."""
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((host, port))
        
        request = {
            'operation': operation,
            'key': key,
            'value': value,
            'forwarded': True
        }
        
        client_socket.send(json.dumps(request).encode('utf-8'))
        response = client_socket.recv(1024).decode('utf-8')
        client_socket.close()
        
        return json.loads(response)
        
    def handle_client(self, client_socket: socket.socket):
        """Handle client requests."""
        while True:
            try:
                data = client_socket.recv(1024).decode('utf-8')
                if not data:
                    break
                    
                request = json.loads(data)
                operation = request.get('operation')
                key = request.get('key')
                value = request.get('value')
                forwarded = request.get('forwarded', False)
                
                response = self.process_operation(operation, key, value, forwarded)
                client_socket.send(json.dumps(response).encode('utf-8'))
                
            except Exception as e:
                print(f"Error handling client request: {e}")
                break
                
        client_socket.close()
        
    def start(self):
        """Start the node and listen for connections."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Node started on {self.host}:{self.port}")
        print(f"Node ID: {self.node_id}")
        
        while True:
            client_socket, address = server_socket.accept()
            print(f"Connected to client: {address}")
            client_thread = threading.Thread(
                target=self.handle_client,
                args=(client_socket,)
            )
            client_thread.start()

def main():
    parser = argparse.ArgumentParser(description='Distributed Key-Value Store Node')
    parser.add_argument('--host', default='localhost', help='Node host address')
    parser.add_argument('--port', type=int, required=True, help='Node port number')
    parser.add_argument('--cluster', nargs='*', help='Cluster nodes in format host:port', default=[])
    
    args = parser.parse_args()
    
    # Parse cluster nodes
    cluster_nodes = []
    for node in args.cluster:
        host, port = node.split(':')
        cluster_nodes.append((host, int(port)))
    
    # Start the node
    node = DistributedNode(args.host, args.port, cluster_nodes)
    node.start()

if __name__ == '__main__':
    main()