import socket
import json
import random
import argparse
from typing import Optional, Dict, Any, List, Tuple

class DistributedClient:
    def __init__(self, nodes: List[str]):
        self.nodes = []
        for node in nodes:
            host, port = node.split(':')
            self.nodes.append((host, int(port)))

    def send_request(self, node: Tuple[str, int], operation: str, key: str, value: str = None) -> Dict[str, Any]:
        """Send request to a specific node."""
        host, port = node
        request = {
            'operation': operation,
            'key': key,
            'value': value
        }

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client_socket.connect((host, port))
            client_socket.send(json.dumps(request).encode('utf-8'))
            response = client_socket.recv(1024).decode('utf-8')
            return json.loads(response)
        finally:
            client_socket.close()

    def execute_operation(self, operation: str, key: str, value: str = None) -> Dict[str, Any]:
        """Try operation on all nodes until successful."""
        # last_error = None
        # for node in self.nodes:
        #     try:
        #         response = self.send_request(node, operation, key, value)
        #         return response
        #     except Exception as e:
        #         last_error = e
        #         continue
        # raise Exception(f"Failed to execute operation on all nodes: {last_error}")

        """Route operations to a specific node based on the key."""
        num_nodes = len(self.nodes)
        if num_nodes == 0:
            raise Exception("No nodes available to execute the operation")

        # # Determine the node responsible for this key using a hash
        # node_index = hash(key) % num_nodes
        # node = self.nodes[node_index]

        # Use the key as the seed for randomness
        # random.seed(100)  # Ensures the same key always maps to the same random node
        node = random.randint(0, num_nodes - 1)
        node = self.nodes[node]

        try:
            # Send the request to the selected node
            response = self.send_request(node, operation, key, value)
            return response
        except Exception as e:
            raise Exception(f"Failed to execute operation on node {node}: {e}")

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

def main():
    parser = argparse.ArgumentParser(description='Distributed Key-Value Store Client')

    # Required arguments
    parser.add_argument('--nodes', required=True, nargs='+', help='List of nodes in format host:port')
    parser.add_argument('--op', required=True, choices=['get', 'put', 'delete', 'update'],
                        help='Operation to perform (get/put/delete/update)')

    # Optional arguments for key and value
    parser.add_argument('key', nargs='?', help='Key for the operation')
    parser.add_argument('value', nargs='?', help='Value for put/update operations')

    args = parser.parse_args()

    # Validate arguments based on operation
    if args.op in ['put', 'update'] and (args.key is None or args.value is None):
        parser.error(f"Operation '{args.op}' requires both key and value arguments")
    elif args.op in ['get', 'delete'] and args.key is None:
        parser.error(f"Operation '{args.op}' requires a key argument")

    # Create client instance
    client = DistributedClient(args.nodes)

    try:
        # Execute the requested operation
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