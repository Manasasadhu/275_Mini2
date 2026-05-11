# CMPE 275 Mini 2 — Distributed Parking Violations Query System

Multi-node distributed query system for NYC parking violations data. 9 nodes form a tree overlay network using gRPC. Supports year-based and county-based sharding strategies.

## Quick Start

```bash
# Build
bash build.sh

# Run all nodes (single-host, year-based)
bash scripts/run_all.sh configs/single-host/year-based/nodes_chunk500.json

# Execute queries
build/client -c configs/single-host/year-based/nodes_chunk500.json

# Extract latency metrics
python3 scripts/extract_latency.py logs latency_data.csv

# Stop all nodes
bash scripts/stop_all.sh
```

## Architecture

### Network Topology (Single-Host)

```
                     A (Gateway)
                /    |    \    \
               B     H     G    I
             / | \
            C  D   E
                  / | \
                 F  D   G
```

### Node Details

| Node | Role | Port | Shard | Language |
|------|------|------|-------|----------|
| A | Gateway | 50051 | — | C++ |
| B | Relay | 50052 | — | C++ |
| C | Worker | 50053 | 2018-07-01 to 2019-06-30 | C++ |
| D | Worker | 50054 | 2022-07-01 to 2022-12-31 | C++ |
| E | Relay | 50055 | — | C++ |
| F | Worker | 50056 | 2023-07-01 to 2024-06-30 | Python |
| G | Worker | 50057 | 2024-07-01 to 2025-06-30 | C++ |
| H | Worker | 50058 | 2025-07-01 to 2026-06-30 | Python |
| I | Worker | 50059 | 2023-01-01 to 2023-06-30 | Python |

## Configurations

Located in `configs/`:

- **single-host/year-based/**: Year-based date-range sharding
  - `nodes_chunk100.json`, `nodes_chunk500.json`, `nodes_chunk1000.json` (mixed C++/Python)
  - `nodes_all_cpp_chunk500.json` (all C++ nodes for performance testing)
  - `nodes_single.json` (default configuration)
  
- **single-host/county/**: County-based sharding
  - `nodes_county.json` (shards by violation county)
  
- **multi-host/**: Distributed across 2-3 machines
  - `nodes_2host.json`, `nodes_3host.json`

## Key Features

### Query Types

1. **Plate Violation History**: Find all violations for a plate, state, date range
2. **Precinct Vehicle Analysis**: Analyze violations by precinct, vehicle year, body type
3. **Violation Code Date Range**: Find violations by code within date range
4. **Unregistered Vehicle Lookup**: Find unregistered vehicles by state and distance from curb

### Sharding Strategies

- **Year-Based (issue_date_range)**: Split data by Issue Date field
  - Each worker node handles violations in a specific date range
  - Default strategy for performance analysis

- **County-Based (county_range)**: Split data by Violation County field
  - Each worker node handles violations from specific counties (NY, BX, BK, etc.)
  - Alternative strategy for geographic distribution

### Client-Side Chunk Control

```bash
# Fetch 3 chunks at a time (default: 5)
./build/client -c <config> 
```

Clients pull results incrementally:
1. `SubmitQuery` — gateway processes across all nodes, returns metadata
2. `FetchChunks` — client pulls results on demand

### Parallel Async Forwarding

Nodes forward queries to neighbors in parallel using:
- C++: `std::async` with thread pool
- Python: `ThreadPoolExecutor`

### Performance Metrics

All nodes log:
- **Shard scan time** (C++): How long to scan and filter CSV
- **Network latency**: RPC round-trip time to each neighbor
- **Query cache** hits/misses

Extract latency data:
```bash
python3 scripts/extract_latency.py logs latency_data.csv
```

## Testing

### Run Single Query Suite
```bash
build/client -c configs/single-host/year-based/nodes_chunk500.json | tee results_chunk500.txt
```

### Test Different Configurations
```bash
# All C++
bash scripts/run_all.sh configs/single-host/year-based/nodes_all_cpp_chunk500.json

# County-based sharding
bash scripts/run_all.sh configs/single-host/county/nodes_county.json

# Different chunk sizes
bash scripts/run_all.sh configs/single-host/year-based/nodes_chunk100.json
bash scripts/run_all.sh configs/single-host/year-based/nodes_chunk1000.json
```

### Multi-Host Testing
Update IP addresses in config and run:
```bash
bash scripts/run_all.sh configs/multi-host/nodes_3host.json
```

## Performance Characteristics

- **Network latency**: ~1ms (pure network, no processing)
- **End-to-end latency**: 60-100 seconds (includes CSV scanning and filtering)
- **Bottleneck**: CSV file I/O and scanning, not network
- **Python vs C++**: After optimization, comparable performance (~100s per query)

## Project Structure

```
├── src/
│   ├── server/        # C++ gRPC server
│   ├── client/        # C++ gRPC client
│   └── common/        # Shared headers (config, sharding)
├── python/server/     # Python gRPC server
├── proto/             # Protocol buffer definitions
├── configs/           # Node configurations (single-host, multi-host)
├── scripts/           # Utilities (run_all.sh, stop_all.sh, extract_latency.py)
└── logs/              # Runtime logs (auto-generated)
```

## Dependencies

- C++17, CMake 3.13+
- gRPC 1.45+
- Python 3.8+, protobuf, grpcio

## Known Issues

- Python workers were slow due to repeated date parsing in loops (fixed in latest version)
- Set appropriate RPC timeouts for large scans (default: 600 seconds)

## Troubleshooting

**Nodes won't connect:**
- Check all nodes are running: `ls logs/*.log`
- Verify ports (50051-50059) are free
- Check network connectivity on multi-host setups

**Zero records returned:**
- Query date range may not overlap with shard ranges
- Check logs for matching records: `grep "Plate ID" parking_violations_combined.csv | grep "YOUR_ID"`

**Python workers slow:**
- Ensure venv is activated with latest protobuf
- Run `bash build.sh` to regenerate protobuf stubs
