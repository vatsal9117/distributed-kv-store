Distributed Key-Value Store (Python • Raft Consensus)
<p align="center"> <img src="https://img.shields.io/badge/Consensus-Raft-orange?style=for-the-badge" /> <img src="https://img.shields.io/badge/Python-3.8%2B-blue?style=for-the-badge" /> <img src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge" /> </p> <p align="center"> A strongly-consistent, fault-tolerant distributed key-value store implementing the full Raft consensus algorithm in pure Python — including leader election, log replication, snapshots, persistence, and automatic fault recovery. </p>

## Features
Core Distributed Systems Functionality

Full Raft Consensus — Leader election, log replication, safety
Linearizable Writes & Reads
Persistent Write-Ahead Log (WAL)
Fault-Tolerant — Survives up to ⌊N/2⌋ failures
Automatic Node Recovery
Snapshotting & Log Compaction
Thread-Safe Internals (RLock)

## Developer Experience

Integrated test & benchmark suite
Built-in cluster metrics
Simple CLI for put/get/delete
Zero external dependencies (pure Python)

## Installation

Clone the repository:

git clone https://github.com/<YOUR_USERNAME>/<REPO_NAME>.git
cd <REPO_NAME>


No dependencies required beyond Python 3.8+.

## Quick Start
Start a 3-node local cluster
python enhanced_distributed_node.py 1
python enhanced_distributed_node.py 2
python enhanced_distributed_node.py 3

Write and Read
# On leader
put user:1 "Alice"

# On any node
get user:1
getl user:1     # linearizable read

Other commands
delete <key>
status
metrics
quit

🏗 Architecture
High-Level Overview
Client → Leader → Followers
           │          │
           └─────Replicate Log Entries──────┘

Internal Node Architecture
┌──────────────────────────────────────────────┐
│ DistributedNode                               │
│  ├── RaftNode (state machine + WAL + term)    │
│  ├── RPC server & client                      │
│  ├── Heartbeat loop                           │
│  ├── Election timer                           │
│  └── Snapshot manager                          │
└──────────────────────────────────────────────┘

📚 File Structure
.
├── benchmark_and_test.py        # Stress tests + benchmarks
├── full_lifecycle_test.py       # End-to-end cluster test
├── enhanced_distributed_node.py # Node executable
├── enhanced_raft_node.py        # Core Raft implementation
├── raft_rpc.py                  # RPC layer
├── test_suite.py                # Unit tests
└── data/                        # WAL + snapshots

⚙ Configuration

Election timeouts tuned for localhost:

self.election_timeout = random.uniform(1.5, 3.0)


For real networks:

self.election_timeout = random.uniform(0.15, 0.30)

📊 Benchmark Results

From benchmark_and_test.py:

Metric	Value
Throughput	~352 ops/sec
Concurrency	10 threads
Writes Tested	500
Success Rate	100%
Snapshot Speed	<100ms
🧪 Testing

Run the complete validation suite:

python benchmark_and_test.py


Includes:

✔ Unit tests

✔ Recovery tests

✔ Random failure injection

✔ Network partition simulation

✔ Performance benchmarks

Monitoring
metrics


Example output:

{
  "node_id": "node1",
  "state": "leader",
  "term": 17,
  "log_entries": 1234,
  "committed_entries": 1234,
  "snapshot_index": 900,
  "kv_pairs": 48
}

## Troubleshooting
"No leader elected"

Increase election timeout or check if nodes are overloaded.

Writes not replicating

Ensure majority of nodes are reachable.

Slow performance

Increase heartbeat frequency

Reduce snapshot interval

Contributing

Contributions welcome!
Feel free to open issues & pull requests.

License

MIT License — free to use and modify.

🎓 References

Raft Paper (Ongaro & Ousterhout)

MIT 6.824 — Distributed Systems

etcd & Consul architecture references
