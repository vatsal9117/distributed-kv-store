# Distributed Key-Value Store (Python, Raft Consensus)

A strongly consistent, fault-tolerant distributed key-value store implementing the full Raft consensus algorithm in pure Python, including leader election, log replication, snapshots, persistence, linearizable reads, and automatic recovery.

## Features

### Core Functionality
- Full Raft consensus implementation
- Leader election
- Log replication
- Safety and consistency guarantees
- Linearizable reads and writes
- Persistent write-ahead log (WAL)
- Fault tolerance for up to floor(N/2) failures
- Automatic node recovery
- Snapshotting and log compaction
- Thread-safe internals using RLock
- Zero external dependencies

### Developer Features
- Integrated benchmark and test suite
- Built-in metrics
- Simple CLI for put/get/delete operations

## Installation

Clone the repository:

```bash
git clone https://github.com/<YOUR_USERNAME>/<REPO_NAME>.git
cd <REPO_NAME>
```

Requires Python 3.8+ and no external dependencies.

## Quick Start

### Start a three-node cluster

```bash
python enhanced_distributed_node.py 1
python enhanced_distributed_node.py 2
python enhanced_distributed_node.py 3
```

### Basic Commands

```
put <key> <value>
get <key>
getl <key>
delete <key>
status
metrics
quit
```

### Example Usage

```
put user:1 "Alice"
get user:1
getl user:1
delete user:1
```

## Architecture

### High-Level Flow

Client -> Leader -> Followers  
           |            |  
           +---- Log Replication ----+

### Node Components

DistributedNode  
  RaftNode (state machine, WAL, term and index state)  
  RPC server and RPC client  
  Heartbeat sender  
  Election timer  
  Snapshot manager  

## File Structure

.
├── benchmark_and_test.py
├── full_lifecycle_test.py
├── enhanced_distributed_node.py
├── enhanced_raft_node.py
├── raft_rpc.py
├── test_suite.py
└── data/

## Configuration

Election timeouts for localhost:

```python
self.election_timeout = random.uniform(1.5, 3.0)
```

Election timeouts for real networks:

```python
self.election_timeout = random.uniform(0.15, 0.30)
```

## Benchmark Results

| Metric            | Value |
|------------------|--------|
| Throughput        | ~352 ops/sec |
| Concurrency       | 10 threads |
| Writes Tested     | 500 |
| Success Rate      | 100 percent |
| Snapshot Speed    | under 100 ms |

## Testing

Run the complete test suite:

```bash
python benchmark_and_test.py
```

## Monitoring

Example metrics output:

```json
{
  "node_id": "node1",
  "state": "leader",
  "term": 17,
  "log_entries": 1234,
  "committed_entries": 1234,
  "snapshot_index": 900,
  "kv_pairs": 48
}
```

## Troubleshooting

No leader elected: increase election timeout.  
Writes not replicating: ensure majority available.  
Slow performance: increase heartbeat or reduce snapshot interval.

## Contributing

Pull requests welcome.

## License

MIT License
