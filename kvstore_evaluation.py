import time
import threading
import random
import string
import statistics
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Tuple
import pandas as pd

class KVStoreEvaluation:
    def __init__(self, nodes: List[str]):
        self.nodes = nodes
        self.results = {}
        
    def generate_random_kv_pairs(self, n: int) -> List[Tuple[str, str]]:
        """Generate n random key-value pairs"""
        pairs = []
        for _ in range(n):
            key = ''.join(random.choices(string.ascii_letters, k=10))
            value = ''.join(random.choices(string.ascii_letters + string.digits, k=20))
            pairs.append((key, value))
        return pairs
        
    def test_correctness(self) -> Dict[str, bool]:
        """Test basic correctness of operations"""
        from client import DistributedClient
        client = DistributedClient(self.nodes)
        results = {}
        
        # Test 1: Basic Put-Get
        print("Running Basic Put-Get Test...")
        key, value = "test_key", "test_value"
        results['basic_put_get'] = False
        client.put(key, value)
        if client.get(key) == value:
            results['basic_put_get'] = True
            
        # Test 2: Update
        print("Running Update Test...")
        results['update'] = False
        new_value = "updated_value"
        client.update(key, new_value)
        if client.get(key) == new_value:
            results['update'] = True
            
        # Test 3: Delete
        print("Running Delete Test...")
        results['delete'] = False
        client.delete(key)
        if client.get(key) is None:
            results['delete'] = True
            
        # Test 4: Non-existent key
        print("Running Non-existent Key Test...")
        results['non_existent'] = False
        if client.get("non_existent_key") is None:
            results['non_existent'] = True
            
        # Test 5: Concurrent Operations
        print("Running Concurrent Operations Test...")
        results['concurrent_ops'] = self.test_concurrent_operations(client)
        
        return results
        
    def test_concurrent_operations(self, client) -> bool:
        """Test concurrent operations for correctness"""
        n_threads = 10
        n_operations = 100
        test_key = "concurrent_test_key"
        
        def worker(worker_id):
            for i in range(n_operations):
                value = f"value_{worker_id}_{i}"
                client.put(f"{test_key}_{i}", value)
                read_value = client.get(f"{test_key}_{i}")
                if read_value != value:
                    return False
            return True
            
        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            results = list(executor.map(worker, range(n_threads)))
            
        return all(results)
        
    def measure_latency(self, operation: str, n_samples: int = 1000) -> Dict[str, float]:
        """Measure operation latency"""
        from client import DistributedClient
        client = DistributedClient(self.nodes)
        latencies = []
        
        pairs = self.generate_random_kv_pairs(n_samples)
        
        for key, value in pairs:
            start_time = time.time()
            
            if operation == 'put':
                client.put(key, value)
            elif operation == 'get':
                client.put(key, value)  # First put
                client.get(key)         # Then measure get
            elif operation == 'update':
                client.put(key, value)  # First put
                client.update(key, value + '_updated')  # Then measure update
            elif operation == 'delete':
                client.put(key, value)  # First put
                client.delete(key)      # Then measure delete
                
            end_time = time.time()
            latencies.append((end_time - start_time) * 1000)  # Convert to milliseconds
            
        return {
            'min': min(latencies),
            'max': max(latencies),
            'avg': statistics.mean(latencies),
            'median': statistics.median(latencies),
            'p95': statistics.quantiles(latencies, n=20)[18],  # 95th percentile
            'p99': statistics.quantiles(latencies, n=100)[98]  # 99th percentile
        }
        
    def measure_throughput(self, n_clients: int, duration: int = 60) -> Dict[str, float]:
        """Measure system throughput under load"""
        from client import DistributedClient
        operations_count = {
            'put': 0,
            'get': 0,
            'update': 0,
            'delete': 0
        }
        
        def client_worker():
            client = DistributedClient(self.nodes)
            start_time = time.time()
            while time.time() - start_time < duration:
                key = f"key_{random.randint(1, 1000)}"
                value = f"value_{random.randint(1, 1000)}"
                
                # Random operation
                op = random.choice(['put', 'get', 'update', 'delete'])
                if op == 'put':
                    client.put(key, value)
                    operations_count['put'] += 1
                elif op == 'get':
                    client.get(key)
                    operations_count['get'] += 1
                elif op == 'update':
                    client.update(key, value)
                    operations_count['update'] += 1
                elif op == 'delete':
                    client.delete(key)
                    operations_count['delete'] += 1
                    
        # Start client threads
        threads = []
        for _ in range(n_clients):
            thread = threading.Thread(target=client_worker)
            thread.start()
            threads.append(thread)
            
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
            
        total_ops = sum(operations_count.values())
        ops_per_second = total_ops / duration
        
        return {
            'total_operations': total_ops,
            'operations_per_second': ops_per_second,
            'operations_breakdown': {
                op: count/duration for op, count in operations_count.items()
            }
        }
        
    def measure_scalability(self, max_clients: int = 100, step: int = 10) -> List[Dict]:
        """Measure system scalability with increasing clients"""
        results = []
        for n_clients in range(step, max_clients + step, step):
            throughput = self.measure_throughput(n_clients, duration=30)
            results.append({
                'n_clients': n_clients,
                'throughput': throughput['operations_per_second']
            })
        return results
        
    def run_full_evaluation(self):
        """Run complete evaluation suite"""
        print("Starting Full Evaluation...")
        
        # 1. Correctness Tests
        print("\n=== Correctness Tests ===")
        correctness_results = self.test_correctness()
        for test, passed in correctness_results.items():
            print(f"{test}: {'PASSED' if passed else 'FAILED'}")
            
        # 2. Latency Measurements
        print("\n=== Latency Tests ===")
        operations = ['put', 'get', 'update', 'delete']
        latency_results = {}
        for op in operations:
            print(f"Measuring {op} latency...")
            latency_results[op] = self.measure_latency(op)
            
        # 3. Throughput Tests
        print("\n=== Throughput Tests ===")
        print("Measuring throughput with 10 concurrent clients...")
        throughput_results = self.measure_throughput(n_clients=10)
        
        # 4. Scalability Tests
        print("\n=== Scalability Tests ===")
        print("Measuring scalability...")
        scalability_results = self.measure_scalability()
        
        # Generate report
        self.generate_report(correctness_results, latency_results, 
                           throughput_results, scalability_results)
        
    def generate_report(self, correctness_results, latency_results, 
                       throughput_results, scalability_results):
        """Generate evaluation report with visualizations"""
        # Create report directory if it doesn't exist
        import os
        os.makedirs("evaluation_report", exist_ok=True)
        
        # 1. Save correctness results
        with open("evaluation_report/correctness.txt", "w") as f:
            for test, result in correctness_results.items():
                f.write(f"{test}: {'PASSED' if result else 'FAILED'}\n")
                
        # 2. Create latency visualization
        plt.figure(figsize=(10, 6))
        ops = list(latency_results.keys())
        avg_latencies = [results['avg'] for results in latency_results.values()]
        p95_latencies = [results['p95'] for results in latency_results.values()]
        
        x = range(len(ops))
        width = 0.35
        
        plt.bar([i - width/2 for i in x], avg_latencies, width, label='Average')
        plt.bar([i + width/2 for i in x], p95_latencies, width, label='95th Percentile')
        plt.xlabel('Operation')
        plt.ylabel('Latency (ms)')
        plt.title('Operation Latencies')
        plt.xticks(x, ops)
        plt.legend()
        plt.savefig("evaluation_report/latencies.png")
        plt.close()
        
        # 3. Create throughput visualization
        plt.figure(figsize=(10, 6))
        ops = throughput_results['operations_breakdown'].keys()
        ops_per_sec = [val for val in throughput_results['operations_breakdown'].values()]
        plt.bar(ops, ops_per_sec)
        plt.xlabel('Operation')
        plt.ylabel('Operations/second')
        plt.title('Operation Throughput')
        plt.savefig("evaluation_report/throughput.png")
        plt.close()
        
        # 4. Create scalability visualization
        plt.figure(figsize=(10, 6))
        clients = [r['n_clients'] for r in scalability_results]
        throughputs = [r['throughput'] for r in scalability_results]
        plt.plot(clients, throughputs, marker='o')
        plt.xlabel('Number of Clients')
        plt.ylabel('Operations/second')
        plt.title('Scalability: Throughput vs Number of Clients')
        plt.grid(True)
        plt.savefig("evaluation_report/scalability.png")
        plt.close()
        
        # 5. Generate summary report
        with open("evaluation_report/summary.txt", "w") as f:
            f.write("=== Evaluation Summary ===\n\n")
            
            f.write("1. Correctness Tests:\n")
            f.write("All tests passed: " + str(all(correctness_results.values())) + "\n\n")
            
            f.write("2. Latency Results (ms):\n")
            for op, results in latency_results.items():
                f.write(f"{op}:\n")
                for metric, value in results.items():
                    f.write(f"  {metric}: {value:.2f}\n")
            f.write("\n")
            
            f.write("3. Throughput Results:\n")
            f.write(f"Total operations per second: {throughput_results['operations_per_second']:.2f}\n\n")
            
            f.write("4. Scalability Results:\n")
            f.write("Maximum throughput achieved: " + 
                   f"{max(r['throughput'] for r in scalability_results):.2f} ops/sec\n")

def main():
    # Example usage
    nodes = ['localhost:5000', 'localhost:5001', 'localhost:5002']
    evaluator = KVStoreEvaluation(nodes)
    evaluator.run_full_evaluation()

if __name__ == '__main__':
    main()