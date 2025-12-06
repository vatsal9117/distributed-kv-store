# Distributed Key-Value Store with Raft Consensus

<p align="center">
  <b>Built with ❤️ by Vatsal</b><br>
  <sub>A production-ready distributed key-value storage system implementing the Raft consensus algorithm in pure Python.</sub>
</p>

---



A robust distributed key-value storage system that provides **strong consistency**, **fault tolerance**, and **automatic recovery**. This project implements the Raft Consensus Algorithm from scratch in Python, featuring leader election, log replication, and persistence.

## Features

### Core Functionality
* **Raft Consensus:** Full implementation of Leader Election, Log Replication, and Safety.
* **Strong Consistency:** Linearizable reads and writes; stale reads are prevented.
* **Fault Tolerance:** The cluster operates continuously as long as `(N/2) + 1` nodes are up.
* **Persistence:** Data survives node crashes and restarts (WAL - Write Ahead Log).
* **Zero Dependencies:** Runs on standard Python 3 libraries.

### Advanced Capabilities
* **Election Stability:** Tuned timeouts to prevent "Election Storms" on local hardware.
* **Thread Safety:** Re-entrant locking (`RLock`) to prevent internal deadlocks.
* **Automated Benchmarking:** Integrated suite for stress testing and performance metrics.
* **Log Compaction:** Automatic snapshotting to manage log size.

---

## Table of Contents
1.  [Quick Start](#-quick-start)
2.  [File Structure](#-file-structure)
3.  [Usage](#-usage)
4.  [Testing & Benchmarking](#-testing--benchmarking)
5.  [Performance](#-performance)
6.  [Configuration](#-configuration)
7.  [Architecture](#-architecture)

---

## Quick Start

### 1. Run a 3-Node Cluster
Open 3 separate terminals and run the following commands:

```bash
# Terminal 1
python enhanced_distributed_node.py 1

# Terminal 2
python enhanced_distributed_node.py 2

# Terminal 3
python enhanced_distributed_node.py 3
2. Interact with the Cluster
In any of the terminals (writes must go to the Leader, reads can go anywhere):

Bash

# Write data (Leader only)
node1> put user:1001 "Alice Smith"
Result: Success

# Read data (Any node - linearizable)
node2> get user:1001
Result: Alice Smith

# Delete data
node1> delete user:1001
Result: Success

File Structure
Plaintext

distributed-kv-store/
├── benchmark_and_test.py       # 🚀 ULTIMATE TEST SUITE (Unit, Fault, & Perf)
├── full_lifecycle_test.py      # End-to-End functional verification script
├── enhanced_distributed_node.py# Main entry point (Node implementation)
├── enhanced_raft_node.py       # Core Raft logic (Consensus, Log, State Machine)
├── raft_rpc.py                 # Network communication layer
├── test_suite.py               # Legacy unit tests
├── data/                       # Persistent storage (Auto-generated)
│   ├── node1/
│   ├── node2/
│   └── node3/
└── README.md

Testing & Benchmarking
This project includes a comprehensive testing suite that verifies logic, fault tolerance, and performance.

Run the Ultimate Test Suite
This single script runs Unit Tests, Chaos/Fault Tolerance simulation, and a Scalability Benchmark.

Bash

python benchmark_and_test.py
What happens during this test?
Unit Tests: Verifies vote logic, term increments, and log appending.

Chaos Engineering:

Simulates concurrent writes.

Kills the Leader to verify election speed.

Partitions the Network to ensure safety.

Crashes & Recovers nodes to verify persistence.

Scalability Benchmark: Floods the cluster with concurrent threads.

Performance
Based on the latest benchmark run (3-Node Cluster, Localhost):

Metric	Result
Throughput	~352 requests/sec
Concurrency	10 Threads
Total Requests	500 Writes
Success Rate	100% (0 Failed)
Time Taken	1.42 Seconds

Note: Performance varies based on hardware and network latency. The system is tuned for correctness over raw speed.

Configuration
You can tune the system in enhanced_raft_node.py and enhanced_distributed_node.py.

Stability Settings
To prevent "Election Storms" on local machines (where CPU contention causes lag), use higher timeouts:

Python

# enhanced_raft_node.py

# Recommended for Localhost testing:
self.election_timeout = random.uniform(1.5, 3.0) 

# Recommended for low-latency LAN:
# self.election_timeout = random.uniform(0.15, 0.3)
Logging
Logs are written to both distributed_node.log and standard output.

INFO: Heartbeats, Commits, State Changes.

DEBUG: Detailed RPC tracing (Enable in enhanced_distributed_node.py).

🏗 Architecture

Getty Images
Explore
Data Flow
Client sends PUT to Leader.

Leader appends entry to local Log (WAL).

Leader sends AppendEntries RPC to Followers.

Followers append to local Log and acknowledge.

Once Majority acknowledges, Leader Commits and applies to State Machine.

Leader responds to Client.

Consensus Invariants
Election Safety: At most one leader can be elected in a given term.

Leader Append-Only: A leader never overwrites or deletes entries in its log; it only appends new entries.

Log Matching: If two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index.

Contributing
Fork the repo.

Create your feature branch (git checkout -b feature/cool-feature).

Commit your changes (git commit -m 'Add some cool feature').

Push to the branch (git push origin feature/cool-feature).

Open a Pull Request.

License
This project is licensed under the MIT License.