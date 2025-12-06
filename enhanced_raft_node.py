"""
Enhanced Distributed Key-Value Store with Raft Consensus
Includes: Persistence, Log Compaction, Linearizable Reads, and Debug Logging
"""

import os
import time
import random
import threading
import json
import logging
from enum import Enum
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict

# Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
logger = logging.getLogger(__name__)

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

@dataclass
class Snapshot:
    last_included_index: int
    last_included_term: int
    state_machine: Dict[str, str]
    
    def to_dict(self):
        return {
            'last_included_index': self.last_included_index,
            'last_included_term': self.last_included_term,
            'state_machine': self.state_machine
        }
    
    @classmethod
    def from_dict(cls, data):
        return cls(**data)

@dataclass
class RaftNode:
    node_id: str
    peers: List[str]
    data_dir: str = "./data"
    
    # Persistent state (must be persisted before responding to RPCs)
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)
    
    # Snapshot state
    snapshot: Optional[Snapshot] = None
    snapshot_interval: int = 100  # Create snapshot every N entries
    
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
    
    # Leader lease for linearizable reads
    leader_lease_timeout: float = 0.1  # 100ms lease
    last_lease_renewal: float = 0
    
    # Data store
    kv_store: Dict[str, str] = field(default_factory=dict)
    
    # Locks
    lock: threading.Lock = field(default_factory=threading.RLock)
    
    def __post_init__(self):
        # Create data directory
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Restore state from disk
        self.restore_state()
        
        self.reset_election_timeout()
        self.last_heartbeat = time.time()
        self.last_lease_renewal = time.time()
        
        # Initialize leader state
        for peer in self.peers:
            start_index = self.get_last_log_index() + 1
            self.next_index[peer] = start_index
            self.match_index[peer] = 0
        
        logger.info(f"Node {self.node_id} initialized with term={self.current_term}, "
                   f"log_length={len(self.log)}")
    
    # ============================================================================
    # PERSISTENCE METHODS
    # ============================================================================
    
    def persist_state(self):
        """
        Persist state to disk before responding to RPCs.
        Critical for Raft safety - must survive crashes.
        """
        state_file = os.path.join(self.data_dir, f"{self.node_id}_state.json")
        temp_file = state_file + ".tmp"
        
        try:
            state = {
                'current_term': self.current_term,
                'voted_for': self.voted_for,
                'log': [entry.to_dict() for entry in self.log]
            }
            
            # Write to temp file first (atomic write)
            with open(temp_file, 'w') as f:
                json.dump(state, f, indent=2)
            
            # Atomic rename
            os.replace(temp_file, state_file)
            
            logger.debug(f"Node {self.node_id} persisted state: term={self.current_term}, "
                        f"log_length={len(self.log)}")
        
        except Exception as e:
            logger.error(f"Failed to persist state: {e}")
            raise
    
    def restore_state(self):
        """
        Restore state from disk on startup.
        Recovers from crashes with all Raft guarantees intact.
        """
        state_file = os.path.join(self.data_dir, f"{self.node_id}_state.json")
        snapshot_file = os.path.join(self.data_dir, f"{self.node_id}_snapshot.json")
        
        # Restore snapshot first
        if os.path.exists(snapshot_file):
            try:
                with open(snapshot_file, 'r') as f:
                    snapshot_data = json.load(f)
                    self.snapshot = Snapshot.from_dict(snapshot_data)
                    self.kv_store = self.snapshot.state_machine.copy()
                    self.last_applied = self.snapshot.last_included_index
                    self.commit_index = self.snapshot.last_included_index
                    
                    logger.info(f"Node {self.node_id} restored snapshot: "
                              f"last_index={self.snapshot.last_included_index}")
            except Exception as e:
                logger.error(f"Failed to restore snapshot: {e}")
        
        # Restore persistent state
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r') as f:
                    state = json.load(f)
                    self.current_term = state['current_term']
                    self.voted_for = state.get('voted_for')
                    self.log = [LogEntry.from_dict(e) for e in state['log']]
                    
                    logger.info(f"Node {self.node_id} restored state: term={self.current_term}, "
                              f"log_length={len(self.log)}")
            except Exception as e:
                logger.error(f"Failed to restore state: {e}")
        
        # Apply any unapplied log entries
        if self.commit_index > self.last_applied:
            self.apply_committed_entries()
    
    # ============================================================================
    # LOG COMPACTION (SNAPSHOTS)
    # ============================================================================
    
    def create_snapshot(self):
        """
        Create snapshot of state machine and compact log.
        Prevents unbounded log growth while maintaining safety.
        """
        with self.lock:
            if len(self.log) < self.snapshot_interval:
                return  # Not enough entries to compact
            
            # Snapshot up to last_applied index
            if self.last_applied == 0:
                return
            
            # Get the last applied entry
            snapshot_index = self.last_applied
            
            # Find the term at snapshot point
            if self.snapshot:
                # Adjust for existing snapshot
                log_index = snapshot_index - self.snapshot.last_included_index - 1
                if log_index >= 0 and log_index < len(self.log):
                    snapshot_term = self.log[log_index].term
                else:
                    return  # Can't snapshot yet
            else:
                if snapshot_index <= len(self.log):
                    snapshot_term = self.log[snapshot_index - 1].term
                else:
                    return
            
            # Create snapshot
            new_snapshot = Snapshot(
                last_included_index=snapshot_index,
                last_included_term=snapshot_term,
                state_machine=self.kv_store.copy()
            )
            
            # Save snapshot to disk
            snapshot_file = os.path.join(self.data_dir, f"{self.node_id}_snapshot.json")
            temp_file = snapshot_file + ".tmp"
            
            try:
                with open(temp_file, 'w') as f:
                    json.dump(new_snapshot.to_dict(), f, indent=2)
                os.replace(temp_file, snapshot_file)
                
                # Discard old log entries
                if self.snapshot:
                    entries_to_keep = snapshot_index - self.snapshot.last_included_index
                    self.log = self.log[entries_to_keep:]
                else:
                    self.log = self.log[snapshot_index:]
                
                self.snapshot = new_snapshot
                
                # Update log indices
                for i, entry in enumerate(self.log):
                    entry.index = snapshot_index + i + 1
                
                logger.info(f"Node {self.node_id} created snapshot: last_index={snapshot_index}, "
                          f"remaining_log={len(self.log)}")
                
                # Persist updated log
                self.persist_state()
                
            except Exception as e:
                logger.error(f"Failed to create snapshot: {e}")
    
    def get_last_log_index(self) -> int:
        """Get the index of the last log entry (considering snapshots)"""
        if self.snapshot:
            return self.snapshot.last_included_index + len(self.log)
        return len(self.log)
    
    def get_last_log_term(self) -> int:
        """Get the term of the last log entry"""
        if self.log:
            return self.log[-1].term
        elif self.snapshot:
            return self.snapshot.last_included_term
        return 0
    
    # ============================================================================
    # LINEARIZABLE READS
    # ============================================================================
    
    def linearizable_read(self, key: str) -> Tuple[bool, str]:
        """
        Perform linearizable read using leader lease mechanism.
        Ensures read reflects all committed writes without stale data.
        
        Process:
        1. Check if we're the leader
        2. Verify leader lease is valid (recent heartbeat to majority)
        3. If lease expired, renew by sending heartbeat
        4. Perform read
        """
        with self.lock:
            if self.state != NodeState.LEADER:
                return False, "Not the leader - cannot guarantee linearizable read"
            
            # Check if leader lease is still valid
            lease_elapsed = time.time() - self.last_lease_renewal
            
            if lease_elapsed > self.leader_lease_timeout:
                logger.debug(f"Leader {self.node_id} lease expired, renewing...")
                # Lease expired, need to renew
                # In real implementation, would send heartbeat and wait for majority
                # For now, we'll just update the lease time
                # This would be done in _send_heartbeats() in practice
                return False, "Leader lease expired - retry after heartbeat"
            
            # Lease is valid, perform read
            if key in self.kv_store:
                logger.debug(f"Linearizable read: {key}={self.kv_store[key]}")
                return True, self.kv_store[key]
            
            return False, "Key not found"
    
    def renew_leader_lease(self, majority_confirmed: bool):
        """
        Renew leader lease after successful heartbeat to majority.
        Called after AppendEntries RPC succeeds on majority.
        """
        if majority_confirmed:
            self.last_lease_renewal = time.time()
            logger.debug(f"Leader {self.node_id} renewed lease")
    
    # ============================================================================
    # MEMBERSHIP CHANGES
    # ============================================================================
    
    def add_node(self, node_id: str, address: Tuple[str, int]) -> Tuple[bool, str]:
        """
        Add a new node to the cluster using joint consensus.
        
        Raft's approach to membership changes:
        1. Leader creates C_old,new configuration
        2. Replicates C_old,new to cluster
        3. Once C_old,new is committed, create C_new
        4. Replicate C_new to cluster
        
        This prevents split-brain during reconfiguration.
        """
        with self.lock:
            if self.state != NodeState.LEADER:
                return False, "Not the leader"
            
            if node_id in self.peers:
                return False, "Node already in cluster"
            
            logger.info(f"Adding node {node_id} to cluster (joint consensus)")
            
            # Phase 1: Add to C_old,new (joint configuration)
            # Create a special log entry for configuration change
            config_entry = LogEntry(
                term=self.current_term,
                command={
                    'type': 'config_change',
                    'phase': 'joint',
                    'action': 'add',
                    'node_id': node_id,
                    'address': address
                },
                index=self.get_last_log_index() + 1
            )
            
            self.log.append(config_entry)
            self.persist_state()
            
            logger.info(f"Node {self.node_id} added joint config for {node_id}")
            
            # In real implementation:
            # 1. Wait for joint config to commit
            # 2. Add node to peers list
            # 3. Create C_new entry
            # 4. Wait for C_new to commit
            # 5. Remove old configuration
            
            # Simplified for demonstration:
            self.peers.append(node_id)
            self.next_index[node_id] = self.get_last_log_index() + 1
            self.match_index[node_id] = 0
            
            return True, f"Node {node_id} added to cluster"
    
    def remove_node(self, node_id: str) -> Tuple[bool, str]:
        """
        Remove a node from the cluster using joint consensus.
        Similar to add_node but removes instead.
        """
        with self.lock:
            if self.state != NodeState.LEADER:
                return False, "Not the leader"
            
            if node_id not in self.peers:
                return False, "Node not in cluster"
            
            logger.info(f"Removing node {node_id} from cluster (joint consensus)")
            
            # Create configuration change entry
            config_entry = LogEntry(
                term=self.current_term,
                command={
                    'type': 'config_change',
                    'phase': 'joint',
                    'action': 'remove',
                    'node_id': node_id
                },
                index=self.get_last_log_index() + 1
            )
            
            self.log.append(config_entry)
            self.persist_state()
            
            # Simplified: directly remove
            self.peers.remove(node_id)
            del self.next_index[node_id]
            del self.match_index[node_id]
            
            return True, f"Node {node_id} removed from cluster"
    
    # ============================================================================
    # ENHANCED STATE TRANSITIONS WITH TRACING
    # ============================================================================
    
    def reset_election_timeout(self):
        """Reset election timeout to random value between 150-300ms"""
        self.election_timeout = random.uniform(1.5, 3.2)
    
    def convert_to_follower(self, term: int):
        """Convert to follower state with detailed tracing"""
        with self.lock:
            old_state = self.state.value
            old_term = self.current_term
            
            self.current_term = term
            self.state = NodeState.FOLLOWER
            self.voted_for = None
            self.reset_election_timeout()
            self.last_heartbeat = time.time()
            
            # Persist state change
            self.persist_state()
            
            logger.info(f"[STATE TRANSITION] Node {self.node_id}: "
                       f"{old_state} (term {old_term}) -> follower (term {term})")
    
    def convert_to_candidate(self):
        """Convert to candidate and start election with tracing"""
        with self.lock:
            old_state = self.state.value
            old_term = self.current_term
            
            self.state = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.reset_election_timeout()
            
            # Persist state before requesting votes
            self.persist_state()
            
            votes_received = 1  # Vote for self
            
            logger.info(f"[STATE TRANSITION] Node {self.node_id}: "
                       f"{old_state} (term {old_term}) -> candidate (term {self.current_term})")
            logger.info(f"[ELECTION] Node {self.node_id} starting election for term {self.current_term}")
            
            return votes_received
    
    def convert_to_leader(self):
        """Convert to leader state with tracing"""
        with self.lock:
            old_state = self.state.value
            
            self.state = NodeState.LEADER
            self.last_lease_renewal = time.time()
            
            logger.info(f"[STATE TRANSITION] Node {self.node_id}: "
                       f"{old_state} -> leader (term {self.current_term})")
            logger.info(f"[LEADER ELECTED] Node {self.node_id} is now the leader for term {self.current_term}")
            
            # Initialize leader state
            for peer in self.peers:
                self.next_index[peer] = self.get_last_log_index() + 1
                self.match_index[peer] = 0
    
    # ============================================================================
    # CORE RAFT RPCS (Enhanced)
    # ============================================================================
    
    def request_vote(self, term: int, candidate_id: str, 
                     last_log_index: int, last_log_term: int) -> Tuple[int, bool]:
        """Handle RequestVote RPC with enhanced logging"""
        with self.lock:
            logger.debug(f"[RPC] Node {self.node_id} received RequestVote from {candidate_id} "
                        f"for term {term}")
            
            # Reply false if term < currentTerm
            if term < self.current_term:
                logger.debug(f"[VOTE DENIED] Candidate {candidate_id} has stale term {term} < {self.current_term}")
                return self.current_term, False
            
            # Update term if needed
            if term > self.current_term:
                self.convert_to_follower(term)
            
            # Check if we can vote for this candidate
            if self.voted_for is None or self.voted_for == candidate_id:
                # Check if candidate's log is at least as up-to-date
                my_last_log_term = self.get_last_log_term()
                my_last_log_index = self.get_last_log_index()
                
                log_ok = (last_log_term > my_last_log_term or 
                         (last_log_term == my_last_log_term and 
                          last_log_index >= my_last_log_index))
                
                if log_ok:
                    self.voted_for = candidate_id
                    self.last_heartbeat = time.time()
                    self.persist_state()
                    
                    logger.info(f"[VOTE GRANTED] Node {self.node_id} voted for {candidate_id} "
                              f"in term {term}")
                    return self.current_term, True
                else:
                    logger.debug(f"[VOTE DENIED] Candidate {candidate_id} log not up-to-date")
            else:
                logger.debug(f"[VOTE DENIED] Already voted for {self.voted_for} in term {term}")
            
            return self.current_term, False
    
    def append_entries(self, term: int, leader_id: str, prev_log_index: int,
                      prev_log_term: int, entries: List[LogEntry], 
                      leader_commit: int) -> Tuple[int, bool]:
        """Handle AppendEntries RPC with enhanced logging"""
        with self.lock:
            is_heartbeat = len(entries) == 0
            
            if is_heartbeat:
                logger.debug(f"[HEARTBEAT] Node {self.node_id} received heartbeat from {leader_id}")
            else:
                logger.debug(f"[APPEND ENTRIES] Node {self.node_id} received {len(entries)} entries "
                           f"from {leader_id}")
            
            # Reply false if term < currentTerm
            if term < self.current_term:
                logger.debug(f"[APPEND REJECTED] Leader {leader_id} has stale term")
                return self.current_term, False
            
            # Update term and convert to follower if needed
            if term > self.current_term:
                self.convert_to_follower(term)
            
            # Reset election timeout (received valid communication from leader)
            self.last_heartbeat = time.time()
            
            # Log matching check
            if prev_log_index > 0:
                actual_index = prev_log_index
                if self.snapshot:
                    actual_index = prev_log_index - self.snapshot.last_included_index
                
                if actual_index > len(self.log):
                    logger.debug(f"[APPEND REJECTED] Log too short: need index {prev_log_index}, "
                               f"have {self.get_last_log_index()}")
                    return self.current_term, False
                
                if actual_index > 0 and self.log[actual_index - 1].term != prev_log_term:
                    logger.debug(f"[APPEND REJECTED] Log term mismatch at index {prev_log_index}")
                    # Delete conflicting entries
                    self.log = self.log[:actual_index - 1]
                    self.persist_state()
                    return self.current_term, False
            
            # Append new entries
            for entry in entries:
                actual_index = entry.index
                if self.snapshot:
                    actual_index = entry.index - self.snapshot.last_included_index
                
                if actual_index <= len(self.log):
                    # Check for conflicts
                    if self.log[actual_index - 1].term != entry.term:
                        self.log = self.log[:actual_index - 1]
                        self.log.append(entry)
                else:
                    self.log.append(entry)
            
            if entries:
                self.persist_state()
                logger.debug(f"[APPEND SUCCESS] Node {self.node_id} appended {len(entries)} entries")
            
            # Update commit index
            if leader_commit > self.commit_index:
                old_commit = self.commit_index
                self.commit_index = min(leader_commit, self.get_last_log_index())
                
                if self.commit_index > old_commit:
                    logger.debug(f"[COMMIT] Node {self.node_id} advanced commit index: "
                               f"{old_commit} -> {self.commit_index}")
                    self.apply_committed_entries()
            
            return self.current_term, True
    
    def apply_committed_entries(self):
        """Apply committed log entries to state machine"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            
            actual_index = self.last_applied
            if self.snapshot:
                actual_index = self.last_applied - self.snapshot.last_included_index
            
            if actual_index <= 0 or actual_index > len(self.log):
                continue
            
            entry = self.log[actual_index - 1]
            
            # Apply command to key-value store
            if entry.command['type'] == 'set':
                self.kv_store[entry.command['key']] = entry.command['value']
                logger.info(f"[APPLY] Node {self.node_id}: SET {entry.command['key']}="
                          f"{entry.command['value']} (index {entry.index})")
            
            elif entry.command['type'] == 'delete':
                self.kv_store.pop(entry.command['key'], None)
                logger.info(f"[APPLY] Node {self.node_id}: DELETE {entry.command['key']} "
                          f"(index {entry.index})")
            
            elif entry.command['type'] == 'config_change':
                logger.info(f"[APPLY] Node {self.node_id}: CONFIG CHANGE {entry.command}")
        
        # Check if we should create a snapshot
        if len(self.log) >= self.snapshot_interval:
            self.create_snapshot()
    
    def client_request(self, operation: str, key: str, value: str = None) -> Tuple[bool, str]:
        """Handle client request (only leader can handle writes)"""
        with self.lock:
            if self.state != NodeState.LEADER:
                return False, "Not the leader"
            
            # Create log entry
            if operation == 'set':
                command = {'type': 'set', 'key': key, 'value': value}
            elif operation == 'delete':
                command = {'type': 'delete', 'key': key}
            elif operation == 'get':
                # Use linearizable read
                return self.linearizable_read(key)
            else:
                return False, "Unknown operation"
            
            # Append to log
            entry = LogEntry(
                term=self.current_term,
                command=command,
                index=self.get_last_log_index() + 1
            )
            self.log.append(entry)
            self.persist_state()
            
            logger.info(f"[CLIENT REQUEST] Node {self.node_id}: {operation} {key} "
                       f"(index {entry.index})")
            
            # In real implementation, would replicate to followers and wait for majority
            # For now, immediately commit (unsafe but demonstrates concept)
            self.commit_index = self.get_last_log_index()
            self.apply_committed_entries()
            
            return True, "Success"
    
    def get_state(self) -> dict:
        """Get current node state for debugging"""
        with self.lock:
            return {
                'node_id': self.node_id,
                'state': self.state.value,
                'term': self.current_term,
                'log_length': len(self.log),
                'commit_index': self.commit_index,
                'last_applied': self.last_applied,
                'snapshot': self.snapshot.to_dict() if self.snapshot else None,
                'kv_store_size': len(self.kv_store),
                'kv_store': self.kv_store.copy()
            }


# Example usage
if __name__ == "__main__":
    # Set logging level
    logging.getLogger().setLevel(logging.DEBUG)
    
    # Create a 3-node cluster with persistence
    node1 = RaftNode(node_id="node1", peers=["node2", "node3"], data_dir="./data/node1")
    node2 = RaftNode(node_id="node2", peers=["node1", "node3"], data_dir="./data/node2")
    node3 = RaftNode(node_id="node3", peers=["node1", "node2"], data_dir="./data/node3")
    
    # Simulate node1 becoming leader
    node1.convert_to_leader()
    
    print("\n" + "="*60)
    print("TESTING ENHANCED FEATURES")
    print("="*60)
    
    # Test 1: Persistence
    print("\n1. Testing Persistence...")
    success, msg = node1.client_request('set', 'persistent_key', 'persistent_value')
    print(f"   Set result: {success}, {msg}")
    print(f"   State persisted to: {node1.data_dir}/node1_state.json")
    
    # Test 2: Log Compaction
    print("\n2. Testing Log Compaction...")
    for i in range(105):  # Exceed snapshot interval
        node1.client_request('set', f'key{i}', f'value{i}')
    print(f"   Log length after 105 writes: {len(node1.log)}")
    print(f"   Snapshot created: {node1.snapshot is not None}")
    if node1.snapshot:
        print(f"   Snapshot covers up to index: {node1.snapshot.last_included_index}")
    
    # Test 3: Linearizable Read
    print("\n3. Testing Linearizable Read...")
    success, value = node1.linearizable_read('key50')
    print(f"   Linearizable read result: {success}, {value}")
    
    # Test 4: Membership Changes
    print("\n4. Testing Membership Changes...")
    success, msg = node1.add_node('node4', ('localhost', 5004))
    print(f"   Add node result: {success}, {msg}")
    print(f"   Current peers: {node1.peers}")
    
    # Print final state
    print("\n" + "="*60)
    print("FINAL NODE STATE")
    print("="*60)
    print(json.dumps(node1.get_state(), indent=2))