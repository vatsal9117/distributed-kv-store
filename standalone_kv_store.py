"""
Standalone Distributed Key-Value Store with Raft Consensus
All components in one file - ready to run!
"""

import os
import time
import random
import threading
import json
import socket
import logging
from enum import Enum
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATA STRUCTURES
# ============================================================================

class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

@dataclass
class LogEntry:
    term: int
    command: dict
    index: int
    
    def to_dict(self):
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data):
        return cls(**data)

# ============================================================================
# RAFT NODE - CORE CONSENSUS
# ============================================================================

@dataclass
class RaftNode:
    node_id: str
    peers: List[str]
    data_dir: str = "./data"
    
    # Persistent state
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)
    
    # Volatile state
    commit_index: int = 0
    last_applied: int = 0
    state: NodeState = NodeState.FOLLOWER
    
    # Leader state
    next_index: Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)
    
    # Timing
    election_timeout: float = 0
    last_heartbeat: float = 0
    
    # Data store
    kv_store: Dict[str, str] = field(default_factory=dict)
    
    # Locks
    lock: threading.Lock = field(default_factory=threading.RLock)
    
    def __post_init__(self):
        os.makedirs(self.data_dir, exist_ok=True)
        self.restore_state()
        self.reset_election_timeout()
        self.last_heartbeat = time.time()
        
        for peer in self.peers:
            self.next_index[peer] = len(self.log) + 1
            self.match_index[peer] = 0
    
    def reset_election_timeout(self):
        self.election_timeout = random.uniform(0.15, 0.3)
    
    def persist_state(self):
        """Save state to disk"""
        try:
            state_file = os.path.join(self.data_dir, f"{self.node_id}_state.json")
            temp_file = state_file + ".tmp"
            
            state = {
                'current_term': self.current_term,
                'voted_for': self.voted_for,
                'log': [entry.to_dict() for entry in self.log]
            }
            
            with open(temp_file, 'w') as f:
                json.dump(state, f, indent=2)
            
            os.replace(temp_file, state_file)
            logger.debug(f"Node {self.node_id} persisted state")
        except Exception as e:
            logger.error(f"Failed to persist state: {e}")
    
    def restore_state(self):
        """Load state from disk"""
        state_file = os.path.join(self.data_dir, f"{self.node_id}_state.json")
        
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r') as f:
                    state = json.load(f)
                    self.current_term = state['current_term']
                    self.voted_for = state.get('voted_for')
                    self.log = [LogEntry.from_dict(e) for e in state['log']]
                    logger.info(f"Node {self.node_id} restored: term={self.current_term}, log={len(self.log)}")
            except Exception as e:
                logger.error(f"Failed to restore state: {e}")
    
    def convert_to_follower(self, term: int):
        """Convert to follower"""
        with self.lock:
            logger.info(f"Node {self.node_id}: {self.state.value} -> follower (term {term})")
            self.current_term = term
            self.state = NodeState.FOLLOWER
            self.voted_for = None
            self.reset_election_timeout()
            self.last_heartbeat = time.time()
            self.persist_state()
    
    def convert_to_candidate(self):
        """Start election"""
        with self.lock:
            self.state = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.reset_election_timeout()
            self.persist_state()
            
            logger.info(f"Node {self.node_id} starting election for term {self.current_term}")
            return 1  # Vote for self
    
    def convert_to_leader(self):
        """Become leader"""
        with self.lock:
            self.state = NodeState.LEADER
            logger.info(f"Node {self.node_id} became LEADER for term {self.current_term}")
            
            for peer in self.peers:
                self.next_index[peer] = len(self.log) + 1
                self.match_index[peer] = 0
    
    def request_vote(self, term: int, candidate_id: str, 
                     last_log_index: int, last_log_term: int) -> Tuple[int, bool]:
        """Handle RequestVote RPC"""
        with self.lock:
            if term < self.current_term:
                return self.current_term, False
            
            if term > self.current_term:
                self.convert_to_follower(term)
            
            if self.voted_for is None or self.voted_for == candidate_id:
                my_last_log_term = self.log[-1].term if self.log else 0
                my_last_log_index = len(self.log)
                
                log_ok = (last_log_term > my_last_log_term or 
                         (last_log_term == my_last_log_term and 
                          last_log_index >= my_last_log_index))
                
                if log_ok:
                    self.voted_for = candidate_id
                    self.last_heartbeat = time.time()
                    self.persist_state()
                    logger.info(f"Node {self.node_id} voted for {candidate_id}")
                    return self.current_term, True
            
            return self.current_term, False
    
    def append_entries(self, term: int, leader_id: str, prev_log_index: int,
                      prev_log_term: int, entries: List[LogEntry], 
                      leader_commit: int) -> Tuple[int, bool]:
        """Handle AppendEntries RPC"""
        with self.lock:
            if term < self.current_term:
                return self.current_term, False
            
            if term > self.current_term:
                self.convert_to_follower(term)
            
            self.last_heartbeat = time.time()
            
            if prev_log_index > 0:
                if len(self.log) < prev_log_index:
                    return self.current_term, False
                if prev_log_index > 0 and self.log[prev_log_index - 1].term != prev_log_term:
                    self.log = self.log[:prev_log_index - 1]
                    self.persist_state()
                    return self.current_term, False
            
            for entry in entries:
                if entry.index <= len(self.log):
                    if self.log[entry.index - 1].term != entry.term:
                        self.log = self.log[:entry.index - 1]
                        self.log.append(entry)
                else:
                    self.log.append(entry)
            
            if entries:
                self.persist_state()
            
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log))
                self.apply_committed_entries()
            
            return self.current_term, True
    
    def apply_committed_entries(self):
        """Apply committed entries to state machine"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            
            if entry.command['type'] == 'set':
                self.kv_store[entry.command['key']] = entry.command['value']
                logger.info(f"Applied: SET {entry.command['key']}={entry.command['value']}")
            elif entry.command['type'] == 'delete':
                self.kv_store.pop(entry.command['key'], None)
                logger.info(f"Applied: DELETE {entry.command['key']}")
    
    def client_request(self, operation: str, key: str, value: str = None) -> Tuple[bool, str]:
        """Handle client request"""
        with self.lock:
            if self.state != NodeState.LEADER:
                return False, "Not the leader"
            
            if operation == 'set':
                command = {'type': 'set', 'key': key, 'value': value}
            elif operation == 'delete':
                command = {'type': 'delete', 'key': key}
            elif operation == 'get':
                if key in self.kv_store:
                    return True, self.kv_store[key]
                return False, "Key not found"
            else:
                return False, "Unknown operation"
            
            entry = LogEntry(
                term=self.current_term,
                command=command,
                index=len(self.log) + 1
            )
            self.log.append(entry)
            self.persist_state()
            
            # Immediately commit for single-node demo
            self.commit_index = len(self.log)
            self.apply_committed_entries()
            
            return True, "Success"
    
    def get_state(self) -> dict:
        with self.lock:
            return {
                'node_id': self.node_id,
                'state': self.state.value,
                'term': self.current_term,
                'log_length': len(self.log),
                'commit_index': self.commit_index,
                'kv_store': self.kv_store.copy()
            }

# ============================================================================
# RPC LAYER
# ============================================================================

class RPCServer:
    def __init__(self, host: str, port: int, node):
        self.host = host
        self.port = port
        self.node = node
        self.server_socket = None
        self.running = False
        
    def start(self):
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.running = True
            
            logger.info(f"RPC Server started on {self.host}:{self.port}")
            threading.Thread(target=self._accept_connections, daemon=True).start()
        except Exception as e:
            logger.error(f"Failed to start RPC server: {e}")
    
    def _accept_connections(self):
        while self.running:
            try:
                client_socket, _ = self.server_socket.accept()
                threading.Thread(target=self._handle_client, args=(client_socket,), daemon=True).start()
            except:
                if self.running:
                    break
    
    def _handle_client(self, client_socket: socket.socket):
        try:
            data = client_socket.recv(4096)
            if not data:
                return
            
            request = json.loads(data.decode())
            method = request.get('method')
            params = request['params']
            
            if method == 'RequestVote':
                term, granted = self.node.request_vote(
                    params['term'], params['candidate_id'],
                    params['last_log_index'], params['last_log_term']
                )
                response = {'term': term, 'vote_granted': granted}
            
            elif method == 'AppendEntries':
                entries = [LogEntry.from_dict(e) for e in params['entries']]
                term, success = self.node.append_entries(
                    params['term'], params['leader_id'],
                    params['prev_log_index'], params['prev_log_term'],
                    entries, params['leader_commit']
                )
                response = {'term': term, 'success': success}
            
            else:
                response = {'success': False, 'error': 'Unknown method'}
            
            client_socket.sendall(json.dumps(response).encode() + b"\n")
        except Exception as e:
            logger.debug(f"RPC error: {e}")
        finally:
            client_socket.close()
    
    def stop(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()

class RPCClient:
    def __init__(self, timeout: float = 1.0):
        self.timeout = timeout
    
    def call(self, host: str, port: int, method: str, params: dict) -> Optional[dict]:
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.settimeout(self.timeout)
            client_socket.connect((host, port))
            
            request = {'method': method, 'params': params}
            client_socket.sendall(json.dumps(request).encode() + b"\n")
            
            data = client_socket.recv(4096)
            response = json.loads(data.decode())
            client_socket.close()
            
            return response
        except:
            return None

# ============================================================================
# DISTRIBUTED NODE
# ============================================================================

class DistributedNode:
    def __init__(self, node_id: str, host: str, port: int, 
                 peers: Dict[str, Tuple[str, int]], data_dir: str = None):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        
        if data_dir is None:
            data_dir = f"./data/{node_id}"
        
        self.raft_node = RaftNode(
            node_id=node_id,
            peers=list(peers.keys()),
            data_dir=data_dir
        )
        
        self.rpc_server = RPCServer(host, port, self.raft_node)
        self.rpc_client = RPCClient(timeout=1.0)
        
        self.running = False
        self.election_thread = None
        self.heartbeat_thread = None
    
    def start(self):
        logger.info(f"Starting node {self.node_id}")
        self.rpc_server.start()
        
        self.running = True
        self.election_thread = threading.Thread(target=self._election_loop, daemon=True)
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        
        self.election_thread.start()
        self.heartbeat_thread.start()
        
        logger.info(f"Node {self.node_id} started successfully")
    
    def stop(self):
        logger.info(f"Stopping node {self.node_id}")
        self.running = False
        self.rpc_server.stop()
    
    def _election_loop(self):
        while self.running:
            time.sleep(0.01)
            
            with self.raft_node.lock:
                if self.raft_node.state == NodeState.LEADER:
                    continue
                
                elapsed = time.time() - self.raft_node.last_heartbeat
                
                if elapsed > self.raft_node.election_timeout:
                    self._start_election()
    
    def _start_election(self):
        self.raft_node.convert_to_candidate()
        
        term = self.raft_node.current_term
        last_log_index = len(self.raft_node.log)
        last_log_term = self.raft_node.log[-1].term if self.raft_node.log else 0
        
        votes_received = 1
        votes_needed = (len(self.peers) + 1) // 2 + 1
        
        vote_lock = threading.Lock()
        
        def request_vote(peer_id, peer_addr):
            nonlocal votes_received
            host, port = peer_addr
            response = self.rpc_client.call(host, port, 'RequestVote', {
                'term': term,
                'candidate_id': self.node_id,
                'last_log_index': last_log_index,
                'last_log_term': last_log_term
            })
            
            if response:
                with vote_lock:
                    if response['vote_granted']:
                        votes_received += 1
                    if response['term'] > term:
                        self.raft_node.convert_to_follower(response['term'])
        
        threads = []
        for peer_id, peer_addr in self.peers.items():
            t = threading.Thread(target=request_vote, args=(peer_id, peer_addr), daemon=True)
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join(timeout=0.5)
        
        with self.raft_node.lock:
            if self.raft_node.state == NodeState.CANDIDATE and votes_received >= votes_needed:
                self.raft_node.convert_to_leader()
                self._send_heartbeats()
    
    def _heartbeat_loop(self):
        while self.running:
            time.sleep(0.05)
            if self.raft_node.state == NodeState.LEADER:
                self._send_heartbeats()
    
    def _send_heartbeats(self):
        with self.raft_node.lock:
            term = self.raft_node.current_term
            leader_commit = self.raft_node.commit_index
        
        def send_to_peer(peer_id, peer_addr):
            host, port = peer_addr
            
            with self.raft_node.lock:
                prev_log_index = self.raft_node.next_index[peer_id] - 1
                prev_log_term = self.raft_node.log[prev_log_index - 1].term if prev_log_index > 0 else 0
                entries = self.raft_node.log[prev_log_index:]
            
            response = self.rpc_client.call(host, port, 'AppendEntries', {
                'term': term,
                'leader_id': self.node_id,
                'prev_log_index': prev_log_index,
                'prev_log_term': prev_log_term,
                'entries': [e.to_dict() for e in entries],
                'leader_commit': leader_commit
            })
            
            if response:
                with self.raft_node.lock:
                    if response['term'] > term:
                        self.raft_node.convert_to_follower(response['term'])
                    elif response['success']:
                        self.raft_node.next_index[peer_id] = len(self.raft_node.log) + 1
                        self.raft_node.match_index[peer_id] = len(self.raft_node.log)
        
        threads = []
        for peer_id, peer_addr in self.peers.items():
            t = threading.Thread(target=send_to_peer, args=(peer_id, peer_addr), daemon=True)
            t.start()
            threads.append(t)
    
    def put(self, key: str, value: str) -> Tuple[bool, str]:
        return self.raft_node.client_request('set', key, value)
    
    def get(self, key: str) -> Tuple[bool, str]:
        with self.raft_node.lock:
            if key in self.raft_node.kv_store:
                return True, self.raft_node.kv_store[key]
            return False, "Key not found"
    
    def delete(self, key: str) -> Tuple[bool, str]:
        return self.raft_node.client_request('delete', key)
    
    def get_status(self) -> dict:
        return self.raft_node.get_state()

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python standalone_kv_store.py <node_number>")
        sys.exit(1)
    
    node_num = int(sys.argv[1])
    
    cluster_config = {
        'node1': ('localhost', 5001),
        'node2': ('localhost', 5002),
        'node3': ('localhost', 5003)
    }
    
    node_id = f'node{node_num}'
    host, port = cluster_config[node_id]
    peers = {k: v for k, v in cluster_config.items() if k != node_id}
    
    node = DistributedNode(node_id, host, port, peers)
    node.start()
    
    print(f"\n{'='*70}")
    print(f"DISTRIBUTED KEY-VALUE STORE: {node_id}")
    print(f"{'='*70}")
    print(f"Address: {host}:{port}")
    print(f"Data directory: ./data/{node_id}")
    print(f"\nCommands:")
    print("  put <key> <value>  - Set a key-value pair")
    print("  get <key>          - Get value")
    print("  delete <key>       - Delete key")
    print("  status             - Show node status")
    print("  quit               - Exit")
    print(f"{'='*70}\n")
    
    try:
        while True:
            try:
                cmd = input(f"{node_id}> ").strip().split()
                
                if not cmd:
                    continue
                
                if cmd[0] == 'put' and len(cmd) >= 3:
                    key = cmd[1]
                    value = ' '.join(cmd[2:])  # Support values with spaces
                    success, msg = node.put(key, value)
                    print(f"Result: {msg}")
                
                elif cmd[0] == 'get' and len(cmd) == 2:
                    success, msg = node.get(cmd[1])
                    print(f"Result: {msg}")
                
                elif cmd[0] == 'delete' and len(cmd) == 2:
                    success, msg = node.delete(cmd[1])
                    print(f"Result: {msg}")
                
                elif cmd[0] == 'status':
                    print(json.dumps(node.get_status(), indent=2))
                
                elif cmd[0] == 'quit':
                    break
                
                else:
                    print("Invalid command")
            
            except EOFError:
                break
            except KeyboardInterrupt:
                print()
                break
    
    finally:
        node.stop()
        print("\nNode stopped")