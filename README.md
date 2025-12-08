# Distributed Key-Value Store with Raft Consensus

<p align="center">
  <img src="https://img.shields.io/badge/python-3.7+-blue.svg" alt="Python">
  <img src="https://img.shields.io/badge/consensus-Raft-orange.svg" alt="Raft">
  <img src="https://img.shields.io/badge/license-MIT-green.svg" alt="License">
  <img src="https://img.shields.io/badge/status-production--ready-brightgreen.svg" alt="Status">
</p>

<p align="center">
  <b>Built with ❤️ by Vatsal</b><br>
  <sub>A production-ready distributed key-value storage system implementing the Raft consensus algorithm in pure Python.</sub>
</p>

---

## 🌟 Overview

A **robust, fault-tolerant distributed database** that provides strong consistency guarantees without sacrificing availability. This project implements the complete Raft Consensus Algorithm from scratch in Python, demonstrating deep understanding of distributed systems concepts including leader election, log replication, persistence, and crash recovery.

### Why This Project Matters

- **Production-Grade Implementation**: Not a toy example - handles real-world challenges like network partitions, concurrent writes, and node failures
- **Zero External Dependencies**: Pure Python implementation using only standard library
- **Educational Value**: Extensively documented with detailed explanations of every design decision
- **Battle-Tested**: Comprehensive test suite covering unit tests, fault injection, and performance benchmarks

---

## 🚀 Features

### Core Functionality
✅ **Raft Consensus Algorithm** - Full implementation of leader election, log replication, and safety guarantees  
✅ **Strong Consistency** - Linearizable reads and writes across all nodes  
✅ **Fault Tolerance** - Cluster continues operating with `(N/2) + 1` nodes alive  
✅ **Automatic Recovery** - Failed nodes rejoin and catch up automatically  
✅ **Persistence** - Data survives crashes via Write-Ahead Log (WAL)  

### Advanced Capabilities
💾 **Crash Recovery** - Nodes restore complete state after restart  
📦 **Log Compaction** - Automatic snapshotting prevents unbounded growth  
🔒 **Linearizable Reads** - Optional strong consistency for read operations  
🔄 **Dynamic Membership** - Add/remove nodes without downtime (joint consensus)  
🐛 **Comprehensive Logging** - Debug, trace, and monitor every operation  
⚡ **High Performance** - ~350+ requests/sec with concurrent client threads  

---

## 📋 Table of Contents

- [Quick Start](#-quick-start)
- [File Structure](#-file-structure)
- [Installation](#-installation)
- [Usage Guide](#-usage-guide)
- [Testing & Benchmarking](#-testing--benchmarking)
- [Performance Metrics](#-performance-metrics)
- [Architecture Deep Dive](#-architecture-deep-dive)
- [Configuration](#-configuration)
- [Troubleshooting](#-troubleshooting)
- [API Reference](#-api-reference)
- [Contributing](#-contributing)
- [License](#-license)

---

## ⚡ Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/vatsal9117/distributed-kv-store.git
cd distributed-kv-store

# No pip install needed - pure Python!
```

### 2. Run a 3-Node Cluster

Open 3 separate terminals and run:

```bash
# Terminal 1 - Node 1
python enhanced_distributed_node.py 1

# Terminal 2 - Node 2
python enhanced_distributed_node.py 2

# Terminal 3 - Node 3
python enhanced_distributed_node.py 3
```

**What happens next:**
- Nodes start and load any persisted state
- Election timeout triggers (1.5-3 seconds)
- One node wins election and becomes LEADER
- Cluster is ready to accept requests!

### 3. Interact with the Cluster

In any terminal (writes go to leader, reads work on any node):

```bash
# Write data (must be on leader)
node1> put username alice
Result: Success

# Read data (works on any node)
node2> get username
Result: alice

# Linearizable read (always fresh, leader only)
node1> getl username
Result: alice

# Delete data
node1> delete username
Result: Success

# Check node status
node1> status
{
  "node_id": "node1",
  "state": "leader",
  "term": 3,
  "log_length": 15,
  "commit_index": 15
}
```

---

## 📂 File Structure

```text
distributed-kv-store/
├── standalone_kv_store.py          # ⭐ ALL-IN-ONE version (easiest to run)
├── enhanced_distributed_node.py    # 🚀 Main entry point (production version)
├── enhanced_raft_node.py           # 🧠 Core Raft consensus logic
├── raft_rpc.py                     # 🌐 Network communication layer
├── benchmark_and_test.py           # 🧪 Ultimate test suite (unit + chaos + perf)
├── full_lifecycle_test.py          # ✅ End-to-end functional tests
├── test_suite.py                   # 📝 Legacy unit tests
├── data/                           # 💾 Persistent storage (auto-created)
│   ├── node1/
│   │   ├── node1_state.json       # Persisted term, vote, log
│   │   └── node1_snapshot.json    # State machine snapshot
│   ├── node2/
│   └── node3/
├── distributed_node.log            # 📋 Application logs
└── README.md                       # 📖 This file
```

### Which Files to Use?

| File | Use Case | Recommended For |
|------|----------|-----------------|
| `standalone_kv_store.py` | ⭐ Single file, works immediately | Quick demos, learning |
| `enhanced_distributed_node.py` | 🚀 Production system with all features | Real deployments |
| `enhanced_raft_node.py` | 🧠 Core consensus logic | Understanding Raft |
| `benchmark_and_test.py` | 🧪 Complete test suite | Validation, CI/CD |

---

## 💻 Installation

### Prerequisites

- **Python 3.7+** (tested on 3.7, 3.8, 3.9, 3.10, 3.11)
- **No external dependencies** - uses only Python standard library
- **OS**: Linux, macOS, Windows (with proper file permissions)

### Setup

```bash
# Clone the repository
git clone https://github.com/vatsal9117/distributed-kv-store.git
cd distributed-kv-store

# Verify Python version
python --version  # Should be 3.7 or higher

# Run immediately - no installation needed!
python standalone_kv_store.py 1
```

### Optional: Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

---

## 📖 Usage Guide

### Basic Operations

#### Starting a Node

```bash
python enhanced_distributed_node.py <node_number>

# Examples:
python enhanced_distributed_node.py 1
python enhanced_distributed_node.py 2
python enhanced_distributed_node.py 3
```

#### CLI Commands

Once a node is running, you can use these commands:

```bash
# Write operations (leader only)
put <key> <value>          # Set a key-value pair
delete <key>               # Delete a key

# Read operations (any node)
get <key>                  # Fast read (may be stale on followers)
getl <key>                 # Linearizable read (always fresh, leader only)

# Administrative
status                     # Show detailed node status
metrics                    # Show performance metrics
quit                       # Gracefully shutdown node
```

### Advanced Operations

#### Dynamic Membership Changes

```bash
# Add a new node to the cluster (leader only)
node1> add node4 localhost 5004
Result: Node node4 added to cluster

# Start the new node
# Terminal 4
python enhanced_distributed_node.py 4

# Remove a node from the cluster (leader only)
node1> remove node2
Result: Node node2 removed from cluster
```

#### Checking Cluster Health

```bash
node1> status
{
  "node_id": "node1",
  "state": "leader",              # Current state: leader/follower/candidate
  "term": 5,                      # Current election term
  "log_length": 234,              # Number of log entries
  "commit_index": 234,            # Last committed index
  "last_applied": 234,            # Last applied to state machine
  "snapshot": {
    "last_included_index": 100,   # Last index in snapshot
    "last_included_term": 3       # Term of last snapshot entry
  },
  "kv_store_size": 42,            # Number of key-value pairs
  "peers": {
    "node2": "localhost:5002",
    "node3": "localhost:5003"
  }
}
```

### Understanding Read Consistency

#### Fast Read (get)
```bash
node2> get mykey
Result: myvalue
```
- ✅ **Fast** - No network communication
- ✅ **Works on any node** - Followers can serve reads
- ⚠️ **May be stale** - Followers might be behind
- **Use case**: High throughput, eventual consistency acceptable

#### Linearizable Read (getl)
```bash
node1> getl mykey
Result: myvalue
```
- ✅ **Always fresh** - Guaranteed up-to-date
- ✅ **Sees all writes** - No stale data
- ⚠️ **Leader only** - Must query the leader
- ⚠️ **Slower** - Waits for leader lease confirmation
- **Use case**: Critical reads requiring strong consistency

---

## 🧪 Testing & Benchmarking

### Run the Ultimate Test Suite

This comprehensive suite validates correctness, fault tolerance, and performance:

```bash
python benchmark_and_test.py
```

### What Gets Tested?

#### 1. Unit Tests (Correctness)
- ✅ Vote granting logic
- ✅ Term incrementing
- ✅ Log appending and consistency
- ✅ State transitions
- ✅ Persistence and recovery

#### 2. Chaos Engineering (Fault Tolerance)
- 🔥 **Leader Failure** - Kills leader, verifies new election
- 🔥 **Network Partition** - Isolates nodes, checks split-brain prevention
- 🔥 **Concurrent Writes** - Floods system with parallel requests
- 🔥 **Crash & Recovery** - Kills node, restarts, verifies data integrity

#### 3. Performance Benchmark
- 📊 Concurrent client threads (10+ threads)
- 📊 Sustained write throughput
- 📊 Latency measurements
- 📊 Success rate tracking

### Sample Test Output

```text
======================================================================
RUNNING ULTIMATE TEST SUITE
======================================================================

[✓] Unit Tests: 15/15 passed
[✓] Leader Election: New leader in 1.8s
[✓] Network Partition: Majority partition operational
[✓] Crash Recovery: Node restored all data
[✓] Concurrent Writes: 500/500 successful

PERFORMANCE BENCHMARK:
  Total Requests: 500
  Successful: 500 (100%)
  Failed: 0 (0%)
  Throughput: 352.11 req/sec
  Time Taken: 1.42 seconds

======================================================================
ALL TESTS PASSED ✓
======================================================================
```

### Manual Testing Scenarios

#### Scenario 1: Basic Cluster Operation

```bash
# Start 3 nodes in separate terminals
python enhanced_distributed_node.py 1 &
python enhanced_distributed_node.py 2 &
python enhanced_distributed_node.py 3 &

# Wait for leader election
sleep 3

# Test basic operations
echo "put test_key test_value" | nc localhost 5001
echo "get test_key" | nc localhost 5001
# Expected: test_value
```

#### Scenario 2: Leader Failure & Recovery

```bash
# Identify the leader
ps aux | grep enhanced_distributed_node

# Kill the leader process
kill -9 <leader_pid>

# Observe: New leader elected within 1.5-3 seconds
# System continues operating

# Test writes still work
sleep 2
echo "put recovery_test success" | nc localhost 5002
# Expected: Success
```

#### Scenario 3: Data Persistence

```bash
# Write data to cluster
echo "put persistent_key persistent_value" | nc localhost 5001

# Kill all nodes
pkill -f enhanced_distributed_node

# Restart all nodes
python enhanced_distributed_node.py 1 &
python enhanced_distributed_node.py 2 &
python enhanced_distributed_node.py 3 &

# Wait for startup
sleep 3

# Verify data persisted
echo "get persistent_key" | nc localhost 5001
# Expected: persistent_value
```

---

## 📊 Performance Metrics

### Benchmark Results (3-Node Cluster, Localhost)

Based on `benchmark_and_test.py` running on standard hardware:

| Metric | Value | Notes |
|--------|-------|-------|
| **Write Throughput** | **352 req/sec** | 10 concurrent threads |
| **Total Requests** | 500 writes | 100% success rate |
| **Latency (avg)** | ~3ms | Local network |
| **Leader Election Time** | 1.5-3s | Tuned for stability |
| **Recovery Time** | <2s | Node restart to ready |
| **Cluster Size** | 3-5 nodes | Optimal range |

### Performance Factors

**What affects throughput:**
- ✅ Network latency between nodes (RTT)
- ✅ Disk I/O speed (persistence writes)
- ✅ Number of concurrent clients
- ✅ Log size (affects snapshot creation)

**Tuning for performance:**
```python
# In enhanced_raft_node.py

# Reduce election timeout for faster recovery (risky on slow networks)
self.election_timeout = random.uniform(0.15, 0.3)  # Fast

# Increase for stability on congested networks
self.election_timeout = random.uniform(1.5, 3.0)   # Stable (default)

# Adjust heartbeat interval
time.sleep(0.05)  # 50ms heartbeats (in heartbeat_loop)
```

### Scalability Characteristics

| Cluster Size | Fault Tolerance | Throughput | Latency | Recommendation |
|--------------|-----------------|------------|---------|----------------|
| 3 nodes | 1 failure | High | Low | ✅ Development |
| 5 nodes | 2 failures | Medium | Medium | ✅ Production |
| 7 nodes | 3 failures | Lower | Higher | ⚠️ High availability only |

**Why not more nodes?**
- Majority (N/2 + 1) required for commits
- More nodes = more network overhead
- Diminishing returns after 5 nodes
- 5 nodes is the sweet spot for most use cases

---

## 🏗 Architecture Deep Dive

### System Components

```
┌─────────────────────────────────────────────────────────┐
│                    Client Applications                   │
└────────────────┬──────────────┬──────────────┬──────────┘
                 │              │              │
                 ▼              ▼              ▼
         ┌───────────┐  ┌───────────┐  ┌───────────┐
         │  Node 1   │  │  Node 2   │  │  Node 3   │
         │ (Leader)  │  │(Follower) │  │(Follower) │
         │           │  │           │  │           │
         │ ┌───────┐ │  │ ┌───────┐ │  │ ┌───────┐ │
         │ │  Raft │ │  │ │  Raft │ │  │ │  Raft │ │
         │ │ Engine│ │  │ │ Engine│ │  │ │ Engine│ │
         │ └───┬───┘ │  │ └───┬───┘ │  │ └───┬───┘ │
         │     │     │  │     │     │  │     │     │
         │ ┌───▼───┐ │  │ ┌───▼───┐ │  │ ┌───▼───┐ │
         │ │  KV   │ │  │ │  KV   │ │  │ │  KV   │ │
         │ │ Store │ │  │ │ Store │ │  │ │ Store │ │
         │ └───────┘ │  │ └───────┘ │  │ └───────┘ │
         └─────┬─────┘  └─────┬─────┘  └─────┬─────┘
               │              │              │
               └──────────────┴──────────────┘
                 Raft Consensus Protocol
             (Heartbeats & Log Replication)
```

### Data Flow: Write Operation

```
1. Client → Leader: PUT(key="user", value="alice")
   │
2. Leader: Append to WAL (Write-Ahead Log)
   │ Log: [..., {index:10, term:3, cmd:"SET user=alice"}]
   │
3. Leader → Followers: AppendEntries RPC
   │ Message: "Add entry 10 after entry 9"
   │
4. Followers: Consistency check
   │ ✓ Have entry 9 with correct term?
   │ ✓ Append entry 10 to local log
   │ ✓ Persist to disk
   │
5. Followers → Leader: Success
   │
6. Leader: Count responses
   │ Self (1) + Follower1 (1) + Follower2 (1) = 3
   │ Majority (3 ≥ 3) ✓
   │
7. Leader: COMMIT entry 10
   │ commit_index = 10
   │
8. Leader: Apply to state machine
   │ kv_store["user"] = "alice"
   │
9. Leader → Client: Success
   │
10. Leader → Followers: "Entry 10 is committed"
    │ (in next heartbeat)
    │
11. Followers: Apply to state machines
```

### Data Flow: Leader Election

```
1. Follower: No heartbeat for timeout period (1.5-3s)
   │
2. Follower → Candidate
   │ - Increment term (3 → 4)
   │ - Vote for self
   │ - Reset election timer
   │
3. Candidate → All Peers: RequestVote RPC
   │ "Vote for me in term 4!"
   │ "My log: last_index=10, last_term=3"
   │
4. Peers: Evaluate request
   │ ✓ Term 4 > my term 3? Yes
   │ ✓ Haven't voted yet? Yes
   │ ✓ Candidate's log up-to-date? Yes
   │ → Grant vote
   │
5. Candidate: Count votes
   │ Self (1) + Peer1 (1) + Peer2 (1) = 3
   │ Majority (3 ≥ 3) ✓
   │
6. Candidate → Leader
   │
7. Leader → All Followers: Heartbeat
   │ "I'm the leader for term 4"
   │
8. Followers → Leader: Acknowledge
   │
9. System operational with new leader!
```

### Raft Consensus - The Three Pillars

#### 1. Leader Election

**Purpose**: Ensure exactly one leader per term

**Mechanism**:
- Followers have randomized timeouts (1.5-3s)
- First to timeout becomes candidate
- Candidates request votes from all peers
- Peer grants vote if:
  - Candidate's term ≥ peer's term
  - Peer hasn't voted yet this term
  - Candidate's log is at least as up-to-date
- Candidate with majority votes → Leader

**Safety**: Random timeouts prevent split votes

#### 2. Log Replication

**Purpose**: Ensure all nodes have identical logs

**Mechanism**:
- Leader appends client requests to its log
- Leader sends AppendEntries RPC to all followers
- Followers check consistency:
  - Must have previous entry with matching term
  - If not, reject and leader retries with earlier entry
- Once majority acknowledges, entry is committed
- Leader applies to state machine
- Followers apply once leader confirms commit

**Safety**: Log Matching Property ensures consistency

#### 3. Safety Guarantees

**Five key invariants**:

1. **Election Safety**: ≤ 1 leader per term
2. **Leader Append-Only**: Leaders never delete/overwrite
3. **Log Matching**: Same index + term → identical prefix
4. **Leader Completeness**: Committed entries in all future leaders
5. **State Machine Safety**: Same commands applied in same order

### Persistence Layer

**Why persistence matters**:
```
Without persistence:
Node votes for A in term 5
→ Crash
→ Restart (forgets vote)
→ Votes for B in term 5
→ TWO VOTES! Split brain! 💥

With persistence:
Node votes for A in term 5
→ Persist to disk
→ Crash
→ Restart
→ Restore vote from disk
→ Cannot vote again ✓
```

**What we persist**:
```python
{
  "current_term": 5,           # Must increase monotonically
  "voted_for": "node2",        # Prevents double-voting
  "log": [                     # All committed & uncommitted entries
    {"index": 1, "term": 1, "cmd": "SET x=1"},
    {"index": 2, "term": 2, "cmd": "SET y=2"},
    ...
  ]
}
```

**How we persist** (crash-safe):
```python
1. Write to temporary file (.tmp)
2. Sync to disk (fsync)
3. Atomic rename to actual file
4. Old file replaced atomically
→ Either old version OR new version exists
→ Never corrupted partial write!
```

---

## ⚙️ Configuration

### Cluster Configuration

Edit in `enhanced_distributed_node.py`:

```python
cluster_config = {
    'node1': ('localhost', 5001),
    'node2': ('localhost', 5002),
    'node3': ('localhost', 5003),
    'node4': ('192.168.1.100', 5004),  # Remote node
    'node5': ('192.168.1.101', 5005),  # Remote node
}
```

### Timing Parameters

**Election Timeout** (most important tuning parameter):

```python
# In enhanced_raft_node.py

# For localhost testing (prevents election storms due to CPU contention)
self.election_timeout = random.uniform(1.5, 3.0)  # RECOMMENDED

# For low-latency LAN (< 1ms RTT)
# self.election_timeout = random.uniform(0.15, 0.3)

# For high-latency WAN (> 50ms RTT)
# self.election_timeout = random.uniform(5.0, 10.0)
```

**Heartbeat Interval**:

```python
# In enhanced_distributed_node.py - _heartbeat_loop()
time.sleep(0.05)  # 50ms (default)

# Recommendations:
# - Fast networks: 25-50ms
# - Slow networks: 100-200ms
# Rule: heartbeat << election_timeout
```

**Leader Lease** (for linearizable reads):

```python
# In enhanced_raft_node.py
self.leader_lease_timeout = 0.1  # 100ms

# Trade-off:
# - Shorter: More network overhead
# - Longer: Stale reads possible after leader change
```

### Snapshot Configuration

```python
# In enhanced_raft_node.py
self.snapshot_interval = 100  # Create snapshot every 100 entries

# Recommendations:
# - Small datasets: 100-500
# - Large datasets: 1000-10000
# - Memory constrained: Lower value
```

### Logging Configuration

```python
# In enhanced_distributed_node.py

# Production: INFO level
logging.basicConfig(level=logging.INFO)

# Development: DEBUG level
logging.basicConfig(level=logging.DEBUG)

# Enable file logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('distributed_node.log'),
        logging.StreamHandler()
    ]
)
```

---

## 🐛 Troubleshooting

### Problem: No Leader Elected

**Symptoms**:
- All nodes stuck in FOLLOWER or CANDIDATE state
- No node becomes LEADER
- Write operations fail with "Not the leader"

**Diagnosis**:
```bash
# Check node status
node1> status
# Look at "state" field

# Check logs
tail -f distributed_node.log | grep "election"
```

**Common Causes & Solutions**:

1. **Election timeouts too short (election storm)**
   ```python
   # Solution: Increase timeout
   self.election_timeout = random.uniform(1.5, 3.0)
   ```

2. **Network connectivity issues**
   ```bash
   # Test connectivity
   ping <node2_ip>
   telnet localhost 5002
   
   # Check firewall
   sudo iptables -L
   ```

3. **Majority not available**
   ```bash
   # Check: Are (N/2)+1 nodes running?
   ps aux | grep enhanced_distributed_node
   
   # Need 2/3 for 3-node cluster
   # Need 3/5 for 5-node cluster
   ```

4. **Clock skew between nodes**
   ```bash
   # Check time on all nodes
   date
   
   # Sync clocks
   sudo ntpdate pool.ntp.org
   ```

### Problem: Split Brain (Two Leaders)

**This should NEVER happen with correct Raft!**

If you see two leaders:
```bash
# Check terms - should be different
node1> status  # term: 5, state: leader
node2> status  # term: 7, state: leader  # ← Higher term should win
```

**Solutions**:
1. Restart all nodes (nuclear option)
2. Check for bugs in election logic
3. Verify persistence is working
4. Check network partitions are healing correctly

### Problem: Data Loss After Crash

**Expected Behavior**:
- Committed data: NEVER lost
- Uncommitted data: MAY be lost

**Diagnosis**:
```bash
# Was the write committed?
# Check logs for "Applied: SET key=value"

# Check persistence files exist
ls -la data/node1/
# Should see: node1_state.json

# Verify file permissions
stat data/node1/node1_state.json
```

**Solutions**:
1. **Persistence not working**
   ```bash
   # Check directory is writable
   touch data/node1/test.txt
   rm data/node1/test.txt
   ```

2. **Write not committed before crash**
   - This is expected! Uncommitted = not durable
   - Retry the write

### Problem: Slow Performance

**Symptoms**:
- High write latency (> 100ms)
- Low throughput (< 100 req/sec)

**Diagnosis**:
```bash
# Check network latency
ping <node2_ip>

# Check CPU usage
top

# Check disk I/O
iostat -x 1

# Check log size
du -sh data/node1/
```

**Solutions**:

1. **Network latency**
   ```python
   # Increase timeouts
   self.rpc_client = RPCClient(timeout=5.0)
   ```

2. **Disk I/O bottleneck**
   - Use SSD instead of HDD
   - Reduce snapshot frequency
   - Batch writes if possible

3. **Log too large**
   ```python
   # Reduce snapshot interval
   self.snapshot_interval = 50  # More frequent snapshots
   ```

4. **CPU contention**
   - Reduce heartbeat frequency
   - Run on dedicated hardware

### Problem: Node Can't Rejoin Cluster

**Symptoms**:
- Node starts but can't sync
- Followers reject AppendEntries
- Node stuck with old term

**Solutions**:

1. **Delete persistent state (last resort)**
   ```bash
   rm -rf data/node1/*
   # Node will start fresh and catch up
   ```

2. **Network connectivity**
   ```bash
   # Verify node can reach cluster
   telnet localhost 5002
   telnet localhost 5003
   ```

---

## 📚 API Reference

### Client Operations

#### put(key: str, value: str) → (bool, str)

Write a key-value pair to the distributed store.

**Parameters**:
- `key`: String identifier
- `value`: String value to store

**Returns**:
- `(True, "Success")` - Write successful
- `(False, "Not the leader")` - Must retry on leader

**Example**:
```python
success, message = node.put("user:1001", "Alice Smith")
if not success:
    print(f"Error: {message}")
```

**Notes**:
- Only succeeds on leader
- Blocks until committed (majority replication)
- Persisted to disk before returning

#### get(key: str, linearizable: bool = False) → (bool, str)

Read a value from the store.

**Parameters**:
- `key`: String identifier to retrieve
- `linearizable`: If True, use leader lease (slower but guaranteed fresh)

**Returns**:
- `(True, value)` - Key found
- `(False, "Key not found")` - Key doesn't exist

**Example**:
```python
# Fast read (may be stale on followers)
success, value = node.get("user:1001")

# Linearizable read (always fresh)
success, value = node.get("user:1001", linearizable=True)
```

**Notes**:
- Local read: Fast but may be stale
- Linearizable: Slower but always fresh

#### delete(key: str) → (bool, str)

Delete a key from the store.

**Parameters**:
- `key`: String identifier to delete

**Returns**:
- `(True, "Success")` - Deletion successful
- `(False, "Not the leader")` - Must retry on leader

**Example**:
```python
success, message = node.delete("user:1001")
```

### Administrative Operations

#### add_node(node_id: str, host: str, port: int) → (bool, str)

Add a new node to the cluster using joint consensus.

**Parameters**:
- `node_id`: Unique identifier for new node
- `host`: IP address or hostname
- `port`: Port number

**Returns**:
- `(True, message)` - Node added successfully
- `(False, error)` - Operation failed

**Example**:
```python
success, msg = node.add_node("node4", "192.168.1.100", 5004)
```

**Notes**:
- Leader only
- Uses two-phase commit (joint consensus)
- Safe - prevents split brain

#### remove_node(node_id: str) → (bool, str)

Remove a node from the cluster.

Distributed KV Store with Raft Consensus
Version: 1.0.0    Status: Production Ready
Last Updated: December 2025

API Reference
=============

remove_node(node_id: str) -> tuple[bool, str]
    Remove a node from the cluster (leader only).

    Parameters:
        node_id: Identifier of node to remove

    Returns:
        (True, message)  - Node removed successfully
        (False, error)   - Operation failed

    Example:
        success, msg = node.remove_node("node2")
        print(msg)


get_status() -> dict
    Get detailed node status and cluster information.

    Returns: Dictionary with keys
        node_id         - This node's identifier
        state           - "leader", "follower" or "candidate"
        term            - Current election term
        log_length      - Number of log entries
        commit_index    - Last committed index
        last_applied    - Last applied index
        snapshot        - Snapshot metadata (if exists)
        kv_store_size   - Number of key-value pairs
        peers           - Connected peer information

    Example:
        status = node.get_status()
        print(f"Node is {status['state']} in term {status['term']}")


get_metrics() -> dict
    Get performance metrics for monitoring systems.
    Returns: Dictionary with monitoring metrics


Contributing
============
Contributions are welcome! Please follow these steps:

1. Fork the repo
   git fork https://github.com/vatsal9117/distributed-kv-store

2. Create feature branch
   git checkout -b feature/amazing-feature

3. Make changes and run tests
   python benchmark_and_test.py

4. Commit with clear message
   git commit -m "Add amazing feature: detailed description"

5. Push and open Pull Request
   git push origin feature/amazing-feature

Code Style
- Follow PEP 8
- Add docstrings to all public methods
- Use type hints
- Write tests for new features
- Update docs on API changes

Testing Requirements
- Pass all existing tests
- Add tests for new features
- Maintain or improve coverage
- Pass lint checks


License
=======
MIT License

Copyright (c) 2024 Vatsal

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


Learning Resources
==================
Essential Raft Reading
- Raft Paper              https://raft.github.io/raft.pdf
- MIT 6.824 Raft Lecture   https://www.youtube.com/watch?v=YbZ3zDzD2nk
- Interactive Raft Demo    https://thesecretlivesofdata.com/raft/

Related Projects
- etcd (Go)         - Used by Kubernetes
- Consul            - Service mesh with Raft
- CockroachDB       - Distributed SQL
- MIT 6.824 Labs    - Build your own Raft


Contact & Support
=================
Author : Vatsal
GitHub : @vatsal9117
Repo   : https://github.com/vatsal9117/distributed-kv-store

Having issues? Check docs -> search issues -> open new issue with logs

Star this repo if you find it useful!

Built with passion for distributed systems
Quick Start • Testing • Architecture • API • Contributing
