# Distributed Key-Value Store with Raft Consensus

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.8%2B-blue.svg" alt="Python 3.8+"/>
  <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT License"/>
  <img src="https://img.shields.io/badge/Consensus-Raft-orange.svg" alt="Raft"/>
  <img src="https://img.shields.io/badge/Status-Production%20Ready-brightgreen" alt="Production Ready"/>
</p>

<p align="center">
  <b>Built with love by Vatsal</b><br>
  <sub>A production-grade, strongly consistent distributed key-value store implementing the full Raft consensus algorithm — in pure Python, zero external dependencies.</sub>
</p>

---

## Features

| Feature                        | Description                                                                 |
|-------------------------------|-----------------------------------------------------------------------------|
| **Full Raft Implementation**  | Leader election, log replication, commit rules, safety guarantees           |
| **Strong Consistency**        | Linearizable reads/writes (`getl`, `put`, `delete`)                         |
| **Fault Tolerance**           | Survives `(N-1)/2` crashes; automatic leader election and recovery         |
| **Persistence & Crash Recovery** | WAL + snapshots; data survives restarts                                  |
| **Log Compaction**            | Automatic snapshotting to prevent unbounded log growth                     |
| **Dynamic Membership**        | Add/remove nodes at runtime (cluster reconfiguration)                      |
| **Zero Dependencies**         | Pure Python standard library only                                           |
| **Thread-Safe Design**        | Re-entrant locks prevent deadlocks under high concurrency                  |
| **Comprehensive Testing**     | Unit, integration, chaos engineering, and performance benchmarks           |

---

## Quick Start

### Start a 3-Node Cluster

```bash
python enhanced_distributed_node.py 1
python enhanced_distributed_node.py 2
python enhanced_distributed_node.py 3
Interact (CLI)
Bashnode1> put username alice
Result: Success

node1> getl username        # Linearizable read (always fresh)
Result: alice

node2> get username         # Fast local read (any node)
Result: alice

File Structure
textdistributed-kv-store/
├── enhanced_distributed_node.py   # Main entry point
├── enhanced_raft_node.py          # Core Raft + state machine
├── raft_rpc.py                    # Network layer
├── benchmark_and_test.py          # Ultimate test suite + benchmark
├── full_lifecycle_test.py         # End-to-end verification
├── test_suite.py                  # Legacy tests
├── data/                          # Persistent storage
└── README.md

Testing & Benchmarking
Bashpython benchmark_and_test.py
Runs unit tests, chaos (kills, partitions, crashes), and performance benchmark.
Latest Result (3-node localhost):

Throughput: ~352 writes/sec
Success Rate: 100%
Time: 1.42s for 500 concurrent writes


Performance






























OperationLatency (LAN)ThroughputWrite5–15 ms500–2000 ops/sRead (fast)< 1 ms10,000+ ops/sRead (linearizable)5–15 ms500–1500 ops/sLeader Failover300–800 ms—

Configuration (Local Testing Recommended)
Python# enhanced_raft_node.py
self.election_timeout = random.uniform(1.5, 3.0)  # Prevents election storms locally
self.heartbeat_interval = 0.5
self.snapshot_interval = 100

CLI Commands













































CommandDescriptionLeader Only?put k vWriteYesget kFast read (any node)Nogetl kLinearizable readYesdelete kDeleteYesstatusNode stateNoadd id host portAdd nodeYesremove idRemove nodeYes

Contributing

Fork → Create branch → Commit → Push → PR
All changes must pass python benchmark_and_test.py


License
MIT License — free to use, modify, and distribute.


  A deep dive into distributed systems — built for learning, correctness, and resilience.

```
