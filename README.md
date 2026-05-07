# CMPE 275 Mini 2 — Distributed Parking Violations Query System

A multi-node distributed query system for NYC parking violations data. 9 nodes form a fixed tree overlay network using gRPC. Clients submit queries through a gateway node (A), which propagates them through the tree. Worker nodes filter and stream results in chunks back to the client.

---

## Architecture

### Network Topology

```
Tree edges: AB, BC, BD, BE, EF, ED, EG, AH, AG, AI

                     A (Gateway)
                /    |    \    \
               B     H     G    I
             / | \
            C  D   E
                  / | \
                 F  D   G
```

### Node Details

| Node | Team | Role | Port | Shard (Issue Date) | Language |
|------|------|------|------|---|---|
| A | Blue | Gateway | 50051 | — | C++ |
| B | Blue | Relay | 50052 | — | C++ |
| C | Yellow | Worker | 50053 | 2018-07-01 to 2019-06-30 | C++ |
| D | Blue | Worker | 50054 | 2022-07-01 to 2022-12-31 | C++ |
| E | Yellow | Relay | 50055 | — | C++ |
| F | Yellow | Worker | 50056 | 2023-07-01 to 2024-06-30 | Python |
| G | Yellow | Worker | 50057 | 2024-07-01 to 2025-06-30 | C++ |
| H | Blue | Worker | 50058 | 2025-07-01 to 2026-06-30 | Python |
| I | Yellow | Worker | 50059 | 2023-01-01 to 2023-06-30 | Python |

### Multi-Host Deployment

```
Machine 1 (10.0.0.200): Nodes A, B, C, D, E
Machine 2 (10.0.0.154): Nodes F, G, H, I
```

Connected via ethernet switch on a private 10.0.0.0/24 network.

---

## Query Types

| # | Query | Fields |
|---|-------|--------|
| 1 | PlateViolationHistory | plate_id + violation_code + date_range + registration_state |
| 2 | ViolationCodeDateRange | violation_code + date_range |
| 3 | PrecinctVehicleAnalysis | county + precinct + vehicle_year_range + body_type |

---

## Prerequisites

- C++17 compiler
- CMake >= 3.15
- gRPC and Protocol Buffers (C++ libraries)
- Python 3.x with `grpcio` (`pip install grpcio grpcio-tools`)

---

## Setup

### 1. Clone and Build

```bash
git clone <repo-url>
cd 275_Mini2
git checkout feat/multi-host-v2
./build.sh
```

### 2. Data File

Place `combined_parking_violations.csv` in the project root on every machine. The config uses a relative path — no hardcoded paths needed.

### 3. Python Dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install grpcio grpcio-tools
bash scripts/gen_proto.sh
```

---

## Running

### Single-Host (all 9 nodes on one machine)

```bash
# Start all nodes
bash scripts/start_local.sh configs/nodes_single.json

# Run queries
./build/client -c configs/nodes_single.json

# Stop all nodes
bash scripts/stop_local.sh
```

### Multi-Host (2 machines via switch)

**Network setup (one-time per session):**

```bash
# On Machine 1:
sudo ip addr add 10.0.0.200/24 dev <ethernet-interface>

# On Machine 2:
sudo ip addr add 10.0.0.154/24 dev <ethernet-interface>

# Verify:
ping 10.0.0.200   # from Machine 2
ping 10.0.0.154   # from Machine 1
```

Find your ethernet interface name with `ip link show` (look for `enp*` or `eth*`, not `lo` or `wlan`).

**Start nodes:**

```bash
# On Machine 2 (10.0.0.154) — start workers first:
bash scripts/start_local.sh configs/nodes_multi.json 10.0.0.154

# On Machine 1 (10.0.0.200) — start gateway + relays:
bash scripts/start_local.sh configs/nodes_multi.json 10.0.0.200
```

**Run queries (from Machine 1):**

```bash
./build/client -c configs/nodes_multi.json
```

**Stop nodes on each machine:**

```bash
bash scripts/stop_local.sh
```

---

## Configuration

Two config files are provided:

| File | Purpose |
|------|---------|
| `configs/nodes_single.json` | All nodes on localhost |
| `configs/nodes_multi.json` | Nodes A-E on 10.0.0.200, F-I on 10.0.0.154 |

Config structure:
```json
{
  "global": {
    "default_chunk_size": 500,
    "shared_data_file": "combined_parking_violations.csv",
    "date_field": "Issue Date"
  },
  "nodes": {
    "A": {
      "host": "10.0.0.200",
      "port": 50051,
      "neighbors": ["B", "G", "H", "I"],
      "role": "gateway",
      "language": "cpp",
      "data_file": null,
      "shard": null
    }
  }
}
```

- `shared_data_file` is resolved relative to the project root
- `data_file: null` means use the global shared file
- `host` is what other nodes use to connect; servers bind to `0.0.0.0`

---

## Project Structure

```
.
├── proto/parking_violation_query.proto    # gRPC service + messages
├── src/
│   ├── common/
│   │   ├── config.hpp                    # JSON config loader
│   │   └── sharding.hpp                  # CSV scanning + query filtering
│   ├── server/server.cpp                 # C++ server (gateway/relay/worker)
│   └── client/client.cpp                 # C++ client
├── python/server/server.py               # Python server (nodes F, H, I)
├── configs/
│   ├── nodes_single.json                 # Single-host config
│   └── nodes_multi.json                  # Multi-host config
├── scripts/
│   ├── start_local.sh                    # Start nodes belonging to this machine
│   ├── stop_local.sh                     # Stop local nodes
│   ├── run_all.sh                        # Start all 9 on localhost (legacy)
│   ├── stop_all.sh                       # Stop all (legacy)
│   └── gen_proto.sh                      # Regenerate Python proto stubs
├── build.sh                              # CMake build script
└── CMakeLists.txt
```

---

## How It Works

1. Client sends query to Node A (gateway) via `SubmitQuery` RPC
2. A converts to `ForwardRequest` and forwards to neighbors (B, G, H, I) in parallel
3. Relay nodes (B, E) propagate to their downstream neighbors, skipping the sender
4. Worker nodes scan the CSV, filter by their shard date range, then apply the query filter
5. Results are chunked and returned upstream
6. Gateway aggregates all chunks into a single `QueryResponse`
7. Nodes D and G have multiple parents — deduplication prevents re-processing

---

## Debugging

```bash
# View logs
tail -f logs/A.log    # Gateway
tail -f logs/F.log    # Python worker

# Check which nodes are running
for f in logs/*.pid; do
  node=$(basename $f .pid)
  pid=$(cat $f)
  kill -0 $pid 2>/dev/null && echo "$node: running" || echo "$node: stopped"
done

# Test connectivity between machines
nc -zv 10.0.0.200 50051
nc -zv 10.0.0.154 50056
```
