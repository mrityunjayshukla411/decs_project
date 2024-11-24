import socket
import json
import time
import random
import argparse
from typing import Optional, Dict, Any, List, Tuple
from collections import defaultdict

class DistributedClient:
    def __init__(self, nodes: List[str]):
        self.nodes = []
        for node in nodes:
            host, port = node.split(':')
            self.nodes.append((host, int(port)))
        
        # Initialize latency tracking
        self.latency_history = defaultdict(list)  # Store recent latencies for each node
        self.window_size = 10  # Number of recent requests to consider for average
        self.latency_threshold = 0.5  # Threshold in seconds to consider a node as high latency
        self.failed_nodes = set()  # Track nodes that have failed recently
        self.failure_timeout = 30  # Seconds to wait before retrying failed nodes

    def track_latency(self, node: Tuple[str, int], latency: float) -> None:
        """Track the latency for a given node."""
        self.latency_history[node].append(latency)
        # Keep only the most recent measurements
        if len(self.latency_history[node]) > self.window_size:
            self.latency_history[node].pop(0)

    def get_average_latency(self, node: Tuple[str, int]) -> float:
        """Calculate average latency for a node."""
        history = self.latency_history[node]
        if not history:
            return float('inf')  # No history means unknown latency
        return sum(history) / len(history)

    def select_best_node(self) -> Tuple[str, int]:
        """Select the node with the lowest average latency."""
        current_time = time.time()
        
        # Remove nodes from failed set if they've timed out
        self.failed_nodes = {
            node for node in self.failed_nodes 
            if current_time - self.latency_history[node][-1] < self.failure_timeout
        }

        available_nodes = [node for node in self.nodes if node not in self.failed_nodes]
        if not available_nodes:
            # If all nodes are marked as failed, reset and try all nodes
            self.failed_nodes.clear()
            available_nodes = self.nodes

        # Sort nodes by average latency
        nodes_with_latency = [
            (node, self.get_average_latency(node))
            for node in available_nodes
        ]
        nodes_with_latency.sort(key=lambda x: x[1])

        # Select the node with lowest latency
        best_node = nodes_with_latency[0][0]
        
        # If best node's latency is too high, randomly select from top 3 performers
        if nodes_with_latency[0][1] > self.latency_threshold and len(nodes_with_latency) > 1:
            candidates = nodes_with_latency[:min(3, len(nodes_with_latency))]
            best_node = random.choice([node for node, _ in candidates])

        return best_node

    def send_request(self, node: Tuple[str, int], operation: str, key: str, value: str = None) -> Dict[str, Any]:
        """Send request to a specific node and track latency."""
        host, port = node
        request = {
            'operation': operation,
            'key': key,
            'value': value
        }

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(5.0)  # Set timeout for connections
        
        start_time = time.time()
        try:
            client_socket.connect((host, port))
            client_socket.send(json.dumps(request).encode('utf-8'))
            response = client_socket.recv(1024).decode('utf-8')
            
            # Track successful request latency
            latency = time.time() - start_time
            self.track_latency(node, latency)
            
            return json.loads(response)
        except Exception as e:
            # Mark node as failed and track high latency
            self.failed_nodes.add(node)
            self.track_latency(node, float('inf'))
            raise e
        finally:
            client_socket.close()

    def execute_operation(self, operation: str, key: str, value: str = None) -> Dict[str, Any]:
        """Execute operation on the best available node."""
        last_error = None
        retries = 2  # Number of retries before giving up
        
        for _ in range(retries):
            try:
                node = self.select_best_node()
                response = self.send_request(node, operation, key, value)
                return response
            except Exception as e:
                last_error = e
                continue
                
        raise Exception(f"Failed to execute operation after {retries} retries: {last_error}")

    # Rest of the methods (get, put, delete, update) remain the same
    def get(self, key: str) -> Optional[str]:
        """Retrieve value for a given key."""
        response = self.execute_operation('GET', key)
        if response['status'] == 'success':
            return response['value']
        print(f"Error: {response['message']}")
        return None

    def put(self, key: str, value: str) -> bool:
        """Add a new key-value pair."""
        response = self.execute_operation('PUT', key, value)
        if response['status'] == 'success':
            print(response['message'])
            return True
        print(f"Error: {response['message']}")
        return False

    def delete(self, key: str) -> bool:
        """Delete a key-value pair."""
        response = self.execute_operation('DELETE', key)
        if response['status'] == 'success':
            print(response['message'])
            return True
        print(f"Error: {response['message']}")
        return False

    def update(self, key: str, value: str) -> bool:
        """Update value for an existing key."""
        response = self.execute_operation('UPDATE', key, value)
        if response['status'] == 'success':
            print(response['message'])
            return True
        print(f"Error: {response['message']}")
        return False

# Main function remains the same
def main():
    parser = argparse.ArgumentParser(description='Distributed Key-Value Store Client')
    parser.add_argument('--nodes', required=True, nargs='+', help='List of nodes in format host:port')
    parser.add_argument('--op', required=True, choices=['get', 'put', 'delete', 'update'],
                        help='Operation to perform (get/put/delete/update)')
    parser.add_argument('key', nargs='?', help='Key for the operation')
    parser.add_argument('value', nargs='?', help='Value for put/update operations')

    args = parser.parse_args()

    if args.op in ['put', 'update'] and (args.key is None or args.value is None):
        parser.error(f"Operation '{args.op}' requires both key and value arguments")
    elif args.op in ['get', 'delete'] and args.key is None:
        parser.error(f"Operation '{args.op}' requires a key argument")

    client = DistributedClient(args.nodes)

    try:
        if args.op == 'get':
            value = client.get(args.key)
            if value is not None:
                print(f"Value: {value}")
        elif args.op == 'put':
            client.put(args.key, args.value)
        elif args.op == 'delete':
            client.delete(args.key)
        elif args.op == 'update':
            client.update(args.key, args.value)
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == '__main__':
    main()