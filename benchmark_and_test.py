import time
import shutil
import os
import threading
import random
import unittest
from typing import List

# Import your existing modules
from enhanced_distributed_node import EnhancedDistributedNode
from enhanced_raft_node import NodeState
from test_suite import FaultToleranceSimulator, TestRaftBasics

class ScalabilityTester:
    """
    Stress tests the cluster with high-volume concurrent traffic.
    Measures Throughput (Req/Sec) and Latency.
    """
    def __init__(self, node_count=3, request_count=500, concurrency=10):
        self.node_count = node_count
        self.request_count = request_count
        self.concurrency = concurrency
        self.nodes = []
        self.base_port = 8000
        self.successful_requests = 0
        self.failed_requests = 0
        self.start_time = 0
        self.end_time = 0

    def setup_cluster(self):
        print(f"\n[SCALABILITY] Starting {self.node_count}-node cluster on ports {self.base_port}+...")
        
        # Clean data
        if os.path.exists("./data"):
            try:
                shutil.rmtree("./data")
            except:
                pass

        cluster_config = {
            f'node{i}': ('localhost', self.base_port + i)
            for i in range(1, self.node_count + 1)
        }

        for node_id, (host, port) in cluster_config.items():
            peers = {k: v for k, v in cluster_config.items() if k != node_id}
            node = EnhancedDistributedNode(node_id, host, port, peers)
            node.start()
            self.nodes.append(node)
        
        print("[SCALABILITY] Waiting 5s for leader election...")
        time.sleep(5)

    def get_leader(self):
        for node in self.nodes:
            if node.raft_node.state == NodeState.LEADER:
                return node
        return None

    def worker_task(self, worker_id, items_per_worker):
        """Worker thread that spams PUT requests"""
        leader = self.get_leader()
        if not leader:
            return

        for i in range(items_per_worker):
            key = f"w{worker_id}_k{i}"
            val = f"data_{random.randint(1, 1000)}"
            try:
                # Retry logic for stability during high load
                for _ in range(3):
                    success, _ = leader.put(key, val)
                    if success:
                        self.successful_requests += 1
                        break
                    time.sleep(0.1)
                    leader = self.get_leader() # Update leader if changed
                    if not leader: break
                else:
                    self.failed_requests += 1
            except:
                self.failed_requests += 1

    def run_benchmark(self):
        self.setup_cluster()
        leader = self.get_leader()
        
        if not leader:
            print("[SCALABILITY] Failed: No leader elected.")
            self.teardown()
            return

        print(f"\n[SCALABILITY] Starting Stress Test: {self.request_count} requests, {self.concurrency} threads")
        print(f"[SCALABILITY] Target Leader: {leader.node_id}")

        threads = []
        items_per_worker = self.request_count // self.concurrency
        
        self.start_time = time.time()
        
        for i in range(self.concurrency):
            t = threading.Thread(target=self.worker_task, args=(i, items_per_worker))
            t.start()
            threads.append(t)
            
        for t in threads:
            t.join()
            
        self.end_time = time.time()
        self.print_results()
        self.teardown()

    def print_results(self):
        duration = self.end_time - self.start_time
        throughput = self.successful_requests / duration
        
        print("\n" + "="*50)
        print("BENCHMARK RESULTS")
        print("="*50)
        print(f"Total Requests:      {self.request_count}")
        print(f"Concurrency:         {self.concurrency} threads")
        print(f"Time Taken:          {duration:.2f} seconds")
        print(f"Successful Writes:   {self.successful_requests}")
        print(f"Failed Writes:       {self.failed_requests}")
        print(f"Throughput:          {throughput:.2f} requests/sec")
        print("="*50 + "\n")

    def teardown(self):
        for node in self.nodes:
            node.stop()

def run_full_suite():
    print("STARTING ULTIMATE TEST SUITE 🚀")
    
    # 1. Clean Slate
    if os.path.exists("./data"):
        try:
            shutil.rmtree("./data")
        except:
            pass

    # 2. Run Unit Tests (Logic Verification)
    print("\n" + "="*60)
    print("PHASE 1: UNIT TESTS (Logic)")
    print("="*60)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestRaftBasics)
    unittest.TextTestRunner(verbosity=1).run(suite)

    # 3. Run Fault Tolerance (Chaos Engineering)
    print("\n" + "="*60)
    print("PHASE 2: FAULT TOLERANCE (Chaos)")
    print("="*60)
    sim = FaultToleranceSimulator(num_nodes=5)
    try:
        sim.setup_cluster()
        sim.simulate_concurrent_writes()
        sim.simulate_leader_failure()
        sim.simulate_network_partition()
        sim.teardown_cluster()
        
        # Recovery Test needs a fresh cluster
        time.sleep(2)
        sim.setup_cluster()
        sim.simulate_node_recovery()
    except Exception as e:
        print(f"Fault Tolerance Error: {e}")
    finally:
        sim.teardown_cluster()

    # 4. Run Scalability (Performance)
    print("\n" + "="*60)
    print("PHASE 3: SCALABILITY (Performance)")
    print("="*60)
    # 500 requests with 10 concurrent threads
    scale_test = ScalabilityTester(node_count=3, request_count=500, concurrency=10)
    scale_test.run_benchmark()

    print("\n ALL TESTS COMPLETE")

if __name__ == "__main__":
    run_full_suite()