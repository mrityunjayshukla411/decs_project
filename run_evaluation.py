#!/usr/bin/env python3
# run_evaluation.py

import os
import sys
import time
from kvstore_evaluation import KVStoreEvaluation

def check_nodes_availability(nodes):
    """Check if all nodes are available"""
    import socket
    
    for node in nodes:
        host, port = node.split(':')
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((host, int(port)))
            sock.close()
            
            if result != 0:
                return False
        except:
            return False
    return True

def main():
    # Configuration
    nodes = ['localhost:5000', 'localhost:5001', 'localhost:5002']
    max_retries = 5
    retry_interval = 2
    
    # Check if nodes are available
    print("Checking node availability...")
    retries = 0
    while not check_nodes_availability(nodes) and retries < max_retries:
        print(f"Waiting for nodes to become available (attempt {retries + 1}/{max_retries})...")
        time.sleep(retry_interval)
        retries += 1
        
    if retries >= max_retries:
        print("Error: Could not connect to all nodes. Make sure the cluster is running.")
        sys.exit(1)
        
    print("All nodes are available. Starting evaluation...")
    
    # Create evaluation directory
    os.makedirs("evaluation_report", exist_ok=True)
    
    try:
        # Run evaluation
        evaluator = KVStoreEvaluation(nodes)
        evaluator.run_full_evaluation()
        
        print("\nEvaluation completed successfully!")
        print("Results are available in the 'evaluation_report' directory:")
        print("- evaluation_report/summary.txt")
        print("- evaluation_report/correctness.txt")
        print("- evaluation_report/latencies.png")
        print("- evaluation_report/throughput.png")
        print("- evaluation_report/scalability.png")
        
    except Exception as e:
        print(f"Error during evaluation: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()