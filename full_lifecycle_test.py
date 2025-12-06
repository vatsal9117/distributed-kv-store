import unittest
import shutil
import os
import time
import json
import logging
from enhanced_distributed_node import EnhancedDistributedNode
from enhanced_raft_node import NodeState

# --- FORCE LOGGING ---
# This forces logs to verify the nodes are talking
logging.getLogger().handlers = []
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[logging.StreamHandler()] 
)

class TestFullLifecycle(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        print("\n=== SETUP: Cleaning Data & Starting Cluster ===")
        
        # 1. Clean up old persistence data
        if os.path.exists("./data"):
            try:
                shutil.rmtree("./data")
            except:
                pass
            
        # 2. Start 3 Nodes
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
            print(f"Started {node_id} on port {port}")

    @classmethod
    def tearDownClass(cls):
        print("\n=== TEARDOWN: Stopping Cluster ===")
        for node in cls.nodes:
            node.stop()

    def get_leader(self):
        for node in self.nodes:
            if node.raft_node.state == NodeState.LEADER:
                return node
        return None

    def wait_for_healthy_cluster(self):
        """loops until 1 leader and 2 followers exist"""
        print("Waiting for cluster to form (need 1 Leader + 2 Followers)...")
        for i in range(20): # Wait up to 20 seconds
            leader_count = 0
            follower_count = 0
            for node in self.nodes:
                if node.raft_node.state == NodeState.LEADER:
                    leader_count += 1
                elif node.raft_node.state == NodeState.FOLLOWER:
                    follower_count += 1
            
            if leader_count == 1 and follower_count == 2:
                print(f"✓ Cluster Healthy! (1 Leader, {follower_count} Followers)")
                return True
            time.sleep(1)
            
        print(f"❌ Timed out! Status: {leader_count} Leaders, {follower_count} Followers")
        return False

    def test_all_commands_sequence(self):
        # --- Step 1: Wait for Cluster Health ---
        is_healthy = self.wait_for_healthy_cluster()
        if not is_healthy:
            self.fail("Cluster failed to form (Did you fix the RLock bug in enhanced_raft_node.py?)")

        leader = self.get_leader()
        follower = [n for n in self.nodes if n.raft_node.state == NodeState.FOLLOWER][0]
        
        print(f"\n[INFO] Leader: {leader.node_id}")
        print(f"[INFO] Follower: {follower.node_id}")

        # --- Step 2: PUT ---
        print("\n[TEST] 1. PUT 'user:101' -> 'alice'")
        success, msg = leader.put("user:101", "alice")
        self.assertTrue(success, f"Put failed: {msg}")

        time.sleep(1) # Allow replication

        # --- Step 3: GET (from follower) ---
        print("[TEST] 2. GET 'user:101' (from follower)")
        success, val = follower.get("user:101")
        self.assertTrue(success, "Get failed")
        self.assertEqual(val, "alice")
        print("✓ Verified: Follower has the data")

        # --- Step 4: DELETE ---
        print("\n[TEST] 3. DELETE 'user:101'")
        success, msg = leader.delete("user:101")
        self.assertTrue(success, f"Delete failed: {msg}")

        time.sleep(1)

        # --- Step 5: GET (Verify Delete) ---
        print("[TEST] 4. GET 'user:101' (should fail)")
        success, val = follower.get("user:101")
        self.assertFalse(success, "Key should be gone")
        print("✓ Verified: Data is gone")

if __name__ == '__main__':
    unittest.main()