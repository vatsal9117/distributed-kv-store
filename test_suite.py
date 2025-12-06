"""
Test Suite for Distributed Key-Value Store
Includes unit tests and fault tolerance simulations
"""

import time
import unittest
import threading
from enhanced_distributed_node import EnhancedDistributedNode
from enhanced_raft_node import RaftNode, NodeState

class TestRaftBasics(unittest.TestCase):
    """Basic Raft functionality tests"""
    
    def test_node_initialization(self):
        """Test node initializes correctly"""
        node = RaftNode(node_id="test1", peers=["test2", "test3"])
        
        self.assertEqual(node.node_id, "test1")
        self.assertEqual(node.state, NodeState.FOLLOWER)
        self.assertEqual(node.current_term, 0)
        self.assertEqual(len(node.log), 0)
        self.assertIsNone(node.voted_for)
    
    def test_term_increment(self):
        """Test term increments during election"""
        node = RaftNode(node_id="test1", peers=["test2", "test3"])
        initial_term = node.current_term
        
        node.convert_to_candidate()
        
        self.assertEqual(node.current_term, initial_term + 1)
        self.assertEqual(node.state, NodeState.CANDIDATE)
        self.assertEqual(node.voted_for, "test1")
    
    def test_vote_granting(self):
        """Test vote is granted to valid candidate"""
        node = RaftNode(node_id="test1", peers=["test2"])
        
        term, granted = node.request_vote(
            term=1,
            candidate_id="test2",
            last_log_index=0,
            last_log_term=0
        )
        
        self.assertTrue(granted)
        self.assertEqual(node.voted_for, "test2")
        self.assertEqual(node.current_term, 1)
    
    def test_vote_rejection_lower_term(self):
        """Test vote is rejected for lower term"""
        node = RaftNode(node_id="test1", peers=["test2"])
        node.current_term = 5
        
        term, granted = node.request_vote(
            term=3,
            candidate_id="test2",
            last_log_index=0,
            last_log_term=0
        )
        
        self.assertFalse(granted)
        self.assertEqual(node.current_term, 5)
    
    def test_single_vote_per_term(self):
        """Test node can only vote once per term"""
        node = RaftNode(node_id="test1", peers=["test2", "test3"])
        
        # First vote
        term, granted = node.request_vote(1, "test2", 0, 0)
        self.assertTrue(granted)
        
        # Second vote in same term (different candidate)
        term, granted = node.request_vote(1, "test3", 0, 0)
        self.assertFalse(granted)
    
    def test_log_replication(self):
        """Test log entry replication"""
        leader = RaftNode(node_id="leader", peers=["follower"])
        follower = RaftNode(node_id="follower", peers=["leader"])
        
        leader.convert_to_leader()
        
        # Leader appends entry
        success, msg = leader.client_request('set', 'key1', 'value1')
        self.assertTrue(success)
        self.assertEqual(len(leader.log), 1)
        
        # Simulate replication to follower
        term, success = follower.append_entries(
            term=leader.current_term,
            leader_id="leader",
            prev_log_index=0,
            prev_log_term=0,
            entries=leader.log,
            leader_commit=leader.commit_index
        )
        
        self.assertTrue(success)
        self.assertEqual(len(follower.log), 1)


class TestDistributedOperations(unittest.TestCase):
    """Test distributed operations across cluster"""
    
    @classmethod
    def setUpClass(cls):
        """Start a 3-node cluster"""
        cls.nodes = []
        
        cluster_config = {
            'node1': ('localhost', 6001),
            'node2': ('localhost', 6002),
            'node3': ('localhost', 6003)
        }
        
        for node_id, (host, port) in cluster_config.items():
            peers = {k: v for k, v in cluster_config.items() if k != node_id}
            node = EnhancedDistributedNode(node_id, host, port, peers)
            node.start()
            cls.nodes.append(node)
        
        # Wait for cluster to stabilize
        time.sleep(2)
    
    @classmethod
    def tearDownClass(cls):
        """Stop all nodes"""
        for node in cls.nodes:
            node.stop()
    
    def test_leader_election(self):
        """Test that exactly one leader is elected"""
        time.sleep(1)  # Wait for election
        
        leaders = [n for n in self.nodes if n.raft_node.state == NodeState.LEADER]
        
        self.assertEqual(len(leaders), 1, "Should have exactly one leader")
        print(f"Leader elected: {leaders[0].node_id}")
    
    def test_distributed_put_get(self):
        """Test put/get operations across cluster"""
        # Find leader
        leader = next(n for n in self.nodes if n.raft_node.state == NodeState.LEADER)
        
        # Put operation
        success, msg = leader.put('test_key', 'test_value')
        self.assertTrue(success, f"Put failed: {msg}")
        
        # Wait for replication
        time.sleep(0.5)
        
        # Get from all nodes
        for node in self.nodes:
            success, value = node.get('test_key')
            self.assertTrue(success)
            self.assertEqual(value, 'test_value')
    
    def test_follower_redirect(self):
        """Test that followers redirect writes"""
        # Find a follower
        follower = next(n for n in self.nodes if n.raft_node.state == NodeState.FOLLOWER)
        
        # Try to write to follower
        success, msg = follower.put('key', 'value')
        self.assertFalse(success)
        self.assertIn("Not the leader", msg)


class FaultToleranceSimulator:
    """Simulate various failure scenarios"""
    
    def __init__(self, num_nodes=5):
        self.num_nodes = num_nodes
        self.nodes = []
        self.base_port = 7000
    
    def setup_cluster(self):
        """Create and start cluster"""
        print(f"\n=== Setting up {self.num_nodes}-node cluster ===")
        
        cluster_config = {
            f'node{i}': ('localhost', self.base_port + i)
            for i in range(1, self.num_nodes + 1)
        }
        
        for node_id, (host, port) in cluster_config.items():
            peers = {k: v for k, v in cluster_config.items() if k != node_id}
            node = EnhancedDistributedNode(node_id, host, port, peers)
            node.start()
            self.nodes.append(node)
        
        print("Waiting for initial leader election...")
        time.sleep(2)
        
        leader = self.get_leader()
        if leader:
            print(f"Initial leader: {leader.node_id}")
        else:
            print("No leader elected yet")
    
    def teardown_cluster(self):
        """Stop all nodes"""
        print("\n=== Shutting down cluster ===")
        for node in self.nodes:
            node.stop()
        self.nodes = []
    
    def get_leader(self):
        """Find current leader"""
        for node in self.nodes:
            if node.raft_node.state == NodeState.LEADER:
                return node
        return None
    
    def simulate_leader_failure(self):
        """Simulate leader crash"""
        print("\n=== Test: Leader Failure ===")
        
        # Find and kill leader
        leader = self.get_leader()
        if not leader:
            print("No leader found!")
            return
        
        print(f"Killing leader: {leader.node_id}")
        leader.stop()
        self.nodes.remove(leader)
        
        # Wait for new election
        print("Waiting for new leader election...")
        time.sleep(1)
        
        new_leader = self.get_leader()
        if new_leader:
            print(f"New leader elected: {new_leader.node_id}")
            print("✓ Leader failure handled successfully")
        else:
            print("✗ No new leader elected")
    
    def simulate_network_partition(self):
        """Simulate network partition"""
        print("\n=== Test: Network Partition ===")
        
        if len(self.nodes) < 3:
            print("Need at least 3 nodes for partition test")
            return
        
        # Partition: isolate one node
        isolated = self.nodes[0]
        print(f"Isolating node: {isolated.node_id}")
        
        # Stop the node (simulates complete network partition)
        isolated.stop()
        self.nodes.remove(isolated)
        
        # Wait for cluster to stabilize
        time.sleep(1)
        
        # Check if majority partition has leader
        leader = self.get_leader()
        if leader:
            print(f"Majority partition has leader: {leader.node_id}")
            print("✓ Network partition handled successfully")
        else:
            print("✗ Majority partition has no leader")
    
    def simulate_concurrent_writes(self):
        """Simulate concurrent writes from multiple clients"""
        print("\n=== Test: Concurrent Writes ===")
        
        leader = self.get_leader()
        if not leader:
            print("No leader found!")
            return
        
        results = []
        
        def write_operation(key, value):
            success, msg = leader.put(key, value)
            results.append((key, success, msg))
        
        # Launch concurrent writes
        threads = []
        for i in range(10):
            thread = threading.Thread(
                target=write_operation,
                args=(f'key{i}', f'value{i}')
            )
            thread.start()
            threads.append(thread)
        
        # Wait for all writes
        for thread in threads:
            thread.join()
        
        # Check results
        successful = sum(1 for _, success, _ in results if success)
        print(f"Successful writes: {successful}/{len(results)}")
        
        # Wait for replication
        time.sleep(1)
        
        # Verify consistency across all nodes
        print("Verifying consistency across nodes...")
        consistent = True
        
        for i in range(10):
            key = f'key{i}'
            values = []
            
            for node in self.nodes:
                success, value = node.get(key)
                if success:
                    values.append(value)
            
            if values and len(set(values)) > 1:
                print(f"✗ Inconsistency detected for {key}: {values}")
                consistent = False
        
        if consistent:
            print("✓ All nodes have consistent state")
    
    def simulate_node_recovery(self):
        """Simulate node crash and recovery"""
        print("\n=== Test: Node Recovery ===")
        
        if len(self.nodes) < 2:
            print("Need at least 2 nodes")
            return
        
        # Stop a follower
        follower = next((n for n in self.nodes if n.raft_node.state == NodeState.FOLLOWER), None)
        if not follower:
            print("No follower found")
            return
        
        print(f"Stopping node: {follower.node_id}")
        stopped_id = follower.node_id
        stopped_host = follower.host
        stopped_port = follower.port
        stopped_peers = follower.peers
        
        follower.stop()
        self.nodes.remove(follower)
        
        # Perform writes while node is down
        leader = self.get_leader()
        if leader:
            print("Performing writes while node is down...")
            for i in range(5):
                leader.put(f'recovery_key{i}', f'recovery_value{i}')
        
        time.sleep(0.5)
        
        # Restart the node
        print(f"Restarting node: {stopped_id}")
        new_node = EnhancedDistributedNode(stopped_id, stopped_host, stopped_port, stopped_peers)
        new_node.start()
        self.nodes.append(new_node)
        
        # Wait for catch-up
        print("Waiting for node to catch up...")
        time.sleep(2)
        
        # Verify it has the data
        print("Verifying recovered node has all data...")
        all_caught_up = True
        
        for i in range(5):
            success, value = new_node.get(f'recovery_key{i}')
            if not success or value != f'recovery_value{i}':
                print(f"Missing data: recovery_key{i}")
                all_caught_up = False
        
        if all_caught_up:
            print(" Node successfully recovered and caught up")


def run_unit_tests():
    """Run unit tests"""
    print("\n" + "="*60)
    print("RUNNING UNIT TESTS")
    print("="*60)
    
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestRaftBasics))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


def run_fault_tolerance_tests():
    """Run fault tolerance simulations"""
    print("\n" + "="*60)
    print("RUNNING FAULT TOLERANCE TESTS")
    print("="*60)
    
    sim = FaultToleranceSimulator(num_nodes=5)
    
    try:
        # Test 1: Basic cluster operation
        sim.setup_cluster()
        time.sleep(1)
        
        # Test 2: Concurrent writes
        sim.simulate_concurrent_writes()
        
        # Test 3: Leader failure
        sim.simulate_leader_failure()
        time.sleep(1)
        
        # Test 4: Network partition
        sim.simulate_network_partition()
        time.sleep(1)
        
        # Cleanup and restart for next test
        sim.teardown_cluster()
        time.sleep(1)
        
        # Test 5: Node recovery
        sim.setup_cluster()
        sim.simulate_node_recovery()
        
    finally:
        sim.teardown_cluster()
    
    print("\n" + "="*60)
    print("FAULT TOLERANCE TESTS COMPLETE")
    print("="*60)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'unit':
        # Run only unit tests
        success = run_unit_tests()
        sys.exit(0 if success else 1)
    
    elif len(sys.argv) > 1 and sys.argv[1] == 'fault':
        # Run only fault tolerance tests
        run_fault_tolerance_tests()
    
    else:
        # Run all tests
        print("\n" + "="*60)
        print("DISTRIBUTED KEY-VALUE STORE - FULL TEST SUITE")
        print("="*60)
        
        unit_success = run_unit_tests()
        
        if unit_success:
            run_fault_tolerance_tests()
        else:
            print("\nSkipping fault tolerance tests due to unit test failures")
        
        print("\n" + "="*60)
        print("ALL TESTS COMPLETE")
        print("="*60)