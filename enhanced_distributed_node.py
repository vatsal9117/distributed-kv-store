"""
Enhanced Distributed Node with all improvements
Includes persistence, log compaction, linearizable reads, and membership changes
"""

import time
import threading
import logging
from typing import Dict, List, Optional, Tuple
from enhanced_raft_node import RaftNode, NodeState, LogEntry
from raft_rpc import RPCServer, RPCClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('distributed_node.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedDistributedNode:
    def __init__(self, node_id: str, host: str, port: int, 
                 peers: Dict[str, Tuple[str, int]], data_dir: str = None):
        """
        Initialize enhanced distributed node with all improvements
        
        Args:
            node_id: Unique identifier for this node
            host: Host address for this node
            port: Port for RPC server
            peers: Dictionary mapping peer_id -> (host, port)
            data_dir: Directory for persistent storage
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        
        # Set data directory
        if data_dir is None:
            data_dir = f"./data/{node_id}"
        
        # Create enhanced Raft node with persistence
        self.raft_node = RaftNode(
            node_id=node_id,
            peers=list(peers.keys()),
            data_dir=data_dir
        )
        
        # Create RPC server and client
        self.rpc_server = RPCServer(host, port, self.raft_node)
        self.rpc_client = RPCClient(timeout=2.0)
        
        # Background threads
        self.running = False
        self.election_thread = None
        self.heartbeat_thread = None
        self.snapshot_thread = None
        
        logger.info(f"Enhanced node {node_id} initialized with persistence at {data_dir}")
    
    def start(self):
        """Start the enhanced distributed node"""
        logger.info(f"Starting enhanced node {self.node_id} on {self.host}:{self.port}")
        
        # Start RPC server
        self.rpc_server.start()
        
        # Start background threads
        self.running = True
        self.election_thread = threading.Thread(target=self._election_loop, daemon=True)
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.snapshot_thread = threading.Thread(target=self._snapshot_loop, daemon=True)
        
        self.election_thread.start()
        self.heartbeat_thread.start()
        self.snapshot_thread.start()
        
        logger.info(f"Node {self.node_id} started successfully")
        logger.info(f"Restored state: term={self.raft_node.current_term}, "
                   f"log_entries={len(self.raft_node.log)}")
    
    def stop(self):
        """Stop the distributed node"""
        logger.info(f"Stopping node {self.node_id}")
        self.running = False
        
        # Create final snapshot before shutdown
        self.raft_node.create_snapshot()
        
        self.rpc_server.stop()
        logger.info(f"Node {self.node_id} stopped")
    
    def _snapshot_loop(self):
        """Background thread for periodic snapshots"""
        while self.running:
            time.sleep(10)  # Check every 10 seconds
            
            try:
                self.raft_node.create_snapshot()
            except Exception as e:
                logger.error(f"Error creating snapshot: {e}")
    
    def _election_loop(self):
        """Background thread for election timeout"""
        while self.running:
            time.sleep(0.01)  # Check every 10ms
            
            with self.raft_node.lock:
                # Only followers and candidates check election timeout
                if self.raft_node.state == NodeState.LEADER:
                    continue
                
                elapsed = time.time() - self.raft_node.last_heartbeat
                
                if elapsed > self.raft_node.election_timeout:
                    # Start election
                    self._start_election()
    
    def _start_election(self):
        """Start leader election"""
        self.raft_node.convert_to_candidate()
        
        # Get election parameters
        term = self.raft_node.current_term
        last_log_index = self.raft_node.get_last_log_index()
        last_log_term = self.raft_node.get_last_log_term()
        
        votes_received = 1  # Vote for self
        votes_needed = (len(self.peers) + 1) // 2 + 1
        
        logger.info(f"Node {self.node_id} starting election for term {term} "
                   f"(need {votes_needed} votes)")
        
        # Request votes from all peers in parallel
        vote_threads = []
        vote_lock = threading.Lock()
        
        def request_vote_from_peer(peer_id, peer_addr):
            nonlocal votes_received
            
            host, port = peer_addr
            response = self.rpc_client.request_vote(
                host, port, term, self.node_id,
                last_log_index, last_log_term
            )
            
            if response:
                with vote_lock:
                    # Check if we got the vote
                    if response['vote_granted']:
                        votes_received += 1
                        logger.info(f"Node {self.node_id} received vote from {peer_id} "
                                  f"({votes_received}/{len(self.peers) + 1})")
                    
                    # Check if someone has higher term
                    if response['term'] > term:
                        self.raft_node.convert_to_follower(response['term'])
        
        # Request votes from all peers
        for peer_id, peer_addr in self.peers.items():
            thread = threading.Thread(
                target=request_vote_from_peer,
                args=(peer_id, peer_addr),
                daemon=True
            )
            thread.start()
            vote_threads.append(thread)
        
        # Wait for all vote requests to complete (with timeout)
        for thread in vote_threads:
            thread.join(timeout=1.0)
        
        # Check if we won the election
        with self.raft_node.lock:
            if (self.raft_node.state == NodeState.CANDIDATE and 
                votes_received >= votes_needed):
                self.raft_node.convert_to_leader()
                # Send immediate heartbeat
                self._send_heartbeats()
    
    def _heartbeat_loop(self):
        """Background thread for sending heartbeats (leader only)"""
        while self.running:
            time.sleep(0.05)  # Send heartbeats every 50ms
            
            if self.raft_node.state == NodeState.LEADER:
                self._send_heartbeats()
    
    def _send_heartbeats(self):
        """Send heartbeats to all peers"""
        with self.raft_node.lock:
            term = self.raft_node.current_term
            leader_id = self.node_id
            leader_commit = self.raft_node.commit_index
        
        successful_responses = 0
        response_lock = threading.Lock()
        
        def send_to_peer(peer_id, peer_addr):
            nonlocal successful_responses
            
            host, port = peer_addr
            
            with self.raft_node.lock:
                next_idx = self.raft_node.next_index[peer_id]
                
                # Calculate prev_log_index and prev_log_term
                prev_log_index = next_idx - 1
                
                if prev_log_index == 0:
                    prev_log_term = 0
                elif self.raft_node.snapshot and prev_log_index <= self.raft_node.snapshot.last_included_index:
                    prev_log_term = self.raft_node.snapshot.last_included_term
                else:
                    actual_index = prev_log_index
                    if self.raft_node.snapshot:
                        actual_index = prev_log_index - self.raft_node.snapshot.last_included_index
                    
                    if actual_index > 0 and actual_index <= len(self.raft_node.log):
                        prev_log_term = self.raft_node.log[actual_index - 1].term
                    else:
                        prev_log_term = 0
                
                # Get entries to send (starting from next_index)
                entries = []
                current_index = self.raft_node.get_last_log_index()
                
                if next_idx <= current_index:
                    actual_start = next_idx
                    if self.raft_node.snapshot:
                        actual_start = next_idx - self.raft_node.snapshot.last_included_index
                    
                    if actual_start > 0 and actual_start <= len(self.raft_node.log):
                        entries = self.raft_node.log[actual_start - 1:]
            
            response = self.rpc_client.append_entries(
                host, port, term, leader_id,
                prev_log_index, prev_log_term,
                entries, leader_commit
            )
            
            if response:
                with response_lock:
                    successful_responses += 1
                
                with self.raft_node.lock:
                    # Check term
                    if response['term'] > term:
                        self.raft_node.convert_to_follower(response['term'])
                        return
                    
                    # Update indices on success
                    if response['success']:
                        new_next = self.raft_node.get_last_log_index() + 1
                        self.raft_node.next_index[peer_id] = new_next
                        self.raft_node.match_index[peer_id] = new_next - 1
                        
                        # Update commit index
                        self._update_commit_index()
                    else:
                        # Decrement next_index and retry
                        self.raft_node.next_index[peer_id] = max(1, 
                            self.raft_node.next_index[peer_id] - 1)
        
        # Send to all peers in parallel
        threads = []
        for peer_id, peer_addr in self.peers.items():
            thread = threading.Thread(
                target=send_to_peer,
                args=(peer_id, peer_addr),
                daemon=True
            )
            thread.start()
            threads.append(thread)
        
        # Wait for responses
        for thread in threads:
            thread.join(timeout=0.5)
        
        # Renew leader lease if we got majority response
        majority = (len(self.peers) + 1) // 2 + 1
        if successful_responses + 1 >= majority:  # +1 for self
            self.raft_node.renew_leader_lease(True)
    
    def _update_commit_index(self):
        """Update commit index based on majority replication"""
        current_index = self.raft_node.get_last_log_index()
        
        for n in range(self.raft_node.commit_index + 1, current_index + 1):
            # Count how many nodes have this index
            count = 1  # Leader has it
            for peer_id in self.peers:
                if self.raft_node.match_index[peer_id] >= n:
                    count += 1
            
            # Check if majority
            if count >= (len(self.peers) + 1) // 2 + 1:
                # Get the term of entry at index n
                actual_index = n
                if self.raft_node.snapshot:
                    actual_index = n - self.raft_node.snapshot.last_included_index
                
                if actual_index > 0 and actual_index <= len(self.raft_node.log):
                    entry_term = self.raft_node.log[actual_index - 1].term
                    
                    # Only commit entries from current term
                    if entry_term == self.raft_node.current_term:
                        self.raft_node.commit_index = n
                        self.raft_node.apply_committed_entries()
    
    # ============================================================================
    # CLIENT API
    # ============================================================================
    
    def put(self, key: str, value: str) -> Tuple[bool, str]:
        """Put key-value pair (write operation)"""
        success, msg = self.raft_node.client_request('set', key, value)
        
        if not success and msg == "Not the leader":
            # Try to find the leader
            leader_info = self._find_leader()
            if leader_info:
                return False, f"Not the leader. Try {leader_info}"
            return False, "Not the leader. No known leader."
        
        return success, msg
    
    def get(self, key: str, linearizable: bool = False) -> Tuple[bool, str]:
        """
        Get value for key
        
        Args:
            key: Key to retrieve
            linearizable: If True, use linearizable read (leader only, slower but guaranteed fresh)
                         If False, local read (fast but may be stale on followers)
        """
        if linearizable:
            return self.raft_node.linearizable_read(key)
        else:
            # Fast local read (may be stale)
            with self.raft_node.lock:
                if key in self.raft_node.kv_store:
                    return True, self.raft_node.kv_store[key]
                return False, "Key not found"
    
    def delete(self, key: str) -> Tuple[bool, str]:
        """Delete key (write operation)"""
        success, msg = self.raft_node.client_request('delete', key)
        
        if not success and msg == "Not the leader":
            leader_info = self._find_leader()
            if leader_info:
                return False, f"Not the leader. Try {leader_info}"
            return False, "Not the leader. No known leader."
        
        return success, msg
    
    def add_node(self, node_id: str, host: str, port: int) -> Tuple[bool, str]:
        """Add a new node to the cluster"""
        return self.raft_node.add_node(node_id, (host, port))
    
    def remove_node(self, node_id: str) -> Tuple[bool, str]:
        """Remove a node from the cluster"""
        return self.raft_node.remove_node(node_id)
    
    def _find_leader(self) -> Optional[str]:
        """Try to find the current leader"""
        # In real implementation, would track last known leader
        # For now, just return None
        return None
    
    def get_status(self) -> dict:
        """Get detailed node status"""
        state = self.raft_node.get_state()
        state['address'] = f"{self.host}:{self.port}"
        state['peers'] = {k: f"{v[0]}:{v[1]}" for k, v in self.peers.items()}
        return state
    
    def get_metrics(self) -> dict:
        """Get node metrics for monitoring"""
        with self.raft_node.lock:
            return {
                'node_id': self.node_id,
                'state': self.raft_node.state.value,
                'term': self.raft_node.current_term,
                'log_entries': len(self.raft_node.log),
                'committed_entries': self.raft_node.commit_index,
                'applied_entries': self.raft_node.last_applied,
                'snapshot_index': self.raft_node.snapshot.last_included_index if self.raft_node.snapshot else 0,
                'kv_pairs': len(self.raft_node.kv_store),
                'is_leader': self.raft_node.state == NodeState.LEADER
            }


# Example usage
if __name__ == "__main__":
    import sys
    import json
    
    # Set debug logging for development
    logging.getLogger().setLevel(logging.DEBUG)
    
    if len(sys.argv) < 2:
        print("Usage: python enhanced_distributed_node.py <node_number>")
        sys.exit(1)
    
    node_num = int(sys.argv[1])
    
    # Define cluster configuration
    cluster_config = {
        'node1': ('localhost', 5001),
        'node2': ('localhost', 5002),
        'node3': ('localhost', 5003)
    }
    
    node_id = f'node{node_num}'
    host, port = cluster_config[node_id]
    
    # Create peers dict (exclude self)
    peers = {k: v for k, v in cluster_config.items() if k != node_id}
    
    # Create and start enhanced node
    node = EnhancedDistributedNode(node_id, host, port, peers)
    node.start()
    
    print(f"\n{'='*70}")
    print(f"ENHANCED DISTRIBUTED NODE: {node_id}")
    print(f"{'='*70}")
    print(f"Features: Persistence, Log Compaction, Linearizable Reads, Membership Changes")
    print(f"Data directory: ./data/{node_id}")
    print(f"\nCommands:")
    print("  put <key> <value>     - Set a key-value pair")
    print("  get <key>             - Get value (fast, may be stale)")
    print("  getl <key>            - Get value (linearizable, always fresh)")
    print("  delete <key>          - Delete key")
    print("  add <node_id> <host> <port> - Add node to cluster")
    print("  remove <node_id>      - Remove node from cluster")
    print("  status                - Show detailed node status")
    print("  metrics               - Show node metrics")
    print("  quit                  - Exit")
    print(f"{'='*70}\n")
    
    # Simple CLI
    try:
        while True:
            cmd = input(f"{node_id}> ").strip().split()
            
            if not cmd:
                continue
            
            if cmd[0] == 'put' and len(cmd) == 3:
                success, msg = node.put(cmd[1], cmd[2])
                print(f"Result: {msg}")
            
            elif cmd[0] == 'get' and len(cmd) == 2:
                success, msg = node.get(cmd[1], linearizable=False)
                print(f"Result: {msg}")
            
            elif cmd[0] == 'getl' and len(cmd) == 2:
                success, msg = node.get(cmd[1], linearizable=True)
                print(f"Result: {msg} (linearizable)")
            
            elif cmd[0] == 'delete' and len(cmd) == 2:
                success, msg = node.delete(cmd[1])
                print(f"Result: {msg}")
            
            elif cmd[0] == 'add' and len(cmd) == 4:
                success, msg = node.add_node(cmd[1], cmd[2], int(cmd[3]))
                print(f"Result: {msg}")
            
            elif cmd[0] == 'remove' and len(cmd) == 2:
                success, msg = node.remove_node(cmd[1])
                print(f"Result: {msg}")
            
            elif cmd[0] == 'status':
                print(json.dumps(node.get_status(), indent=2))
            
            elif cmd[0] == 'metrics':
                print(json.dumps(node.get_metrics(), indent=2))
            
            elif cmd[0] == 'quit':
                break
            
            else:
                print("Invalid command")
    
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()
        print("\nNode stopped gracefully")