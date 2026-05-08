# CMPE 275 Mini 2 — Distributed Parking Violations Query System

A multi-node distributed query system for NYC parking violations data. 9 nodes form a fixed tree overlay network using gRPC. Clients submit queries through a gateway node (A), which propagates them through the tree. Worker nodes filter results by their shard and return chunked results that the client pulls on demand.

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
Machine 1 (172.16.0.200): Nodes A, B, C, D, E
Machine 2 (172.16.0.154): Nodes F, G, H, I
```

Connected via ethernet switch on a private 172.16.0.0/24 network.

---

## Key Features

### Client-Side Chunk Control

The client controls memory and bandwidth by pulling results incrementally:

1. `SubmitQuery` — gateway processes the query across all nodes, stores results server-side, returns metadata (`total_chunks`, `request_id`)
2. `FetchChunks(request_id, offset, limit)` — client pulls chunks on demand in configurable batches

```bash
# Fetch 3 chunks at a time (default: 5)
./build/client -c configs/nodes_multi.json -f 3
```

### LRU Query Cache

Every node (gateway, relay, worker) maintains an in-memory LRU cache (64 entries). Cache keys are derived from query parameters, so repeated identical queries return instantly without re-scanning the CSV or forwarding downstream. Logs show `CACHE HIT` vs `CACHE MISS` with lookup times.

### Cancel Propagation

`CancelQuery(request_id)` propagates through the entire tree:
- Marks the request as cancelled on each node
- Removes stored results from the gateway
- Nodes receiving a `ForwardQuery` for a cancelled request abort immediately

### Performance Timing

All nodes log detailed timing metrics:
- **Gateway**: total query processing time, per-neighbor RPC latency
- **Workers**: CSV shard scan duration
- **All nodes**: cache lookup time (microseconds for hits)

Example log output:
```
[14:32:01.234][A] Received 3 chunks from B (rpc_time=1204 ms)
[14:32:01.234][D] Own shard returned 1 chunks (scan_time=890 ms)
[14:32:01.235][A] CACHE HIT at gateway (lookup_time=12 us)
```

### Async Parallel Forwarding

Both C++ (`std::async`) and Python (`ThreadPoolExecutor`) servers forward queries to neighbors in parallel, minimizing latency through the tree.

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
- Python 3.x (venv + grpcio created automatically by `build.sh`)

---

## Setup

### 1. Clone and Build

```bash
git clone <repo-url>
cd 275_Mini2
git checkout feat/multi-host-v2
./build.sh
```

`build.sh` compiles the C++ binaries and automatically creates a Python venv with grpcio if one doesn't exist.

### 2. Data File

Place `combined_parking_violations.csv` in the project root on every machine. The config uses a relative path — no hardcoded paths needed.

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

### Quick Multi-Host Setup (auto-configure)

Instead of manually editing config files, use the `configure_hosts.sh` script to generate the config from IP addresses:

```bash
# 2 machines:
bash scripts/configure_hosts.sh <IP1> <IP2>

# 3 machines:
bash scripts/configure_hosts.sh <IP1> <IP2> <IP3>

# Auto-detect this machine's IP as Machine 1:
bash scripts/configure_hosts.sh auto <IP2>
```

Examples:
```bash
# 2-host: Machine 1 runs A,B,C,D,E — Machine 2 runs F,G,H,I
bash scripts/configure_hosts.sh 172.16.0.200 172.16.0.154

# 3-host: Machine 1 runs A,B,C — Machine 2 runs D,E,F — Machine 3 runs G,H,I
bash scripts/configure_hosts.sh 172.16.0.100 172.16.0.200 172.16.0.154
```

This overwrites `configs/nodes_multi.json`. Copy the generated file to all machines, then start nodes on each machine:
```bash
bash scripts/start_local.sh configs/nodes_multi.json <this-machines-IP>
```

---

### Multi-Host (2 machines via network switch)

#### Hardware Setup

You need:
- 1 unmanaged network switch (any port count)
- 2 ethernet cables

```
[Machine 1] ──ethernet──> [SWITCH] <──ethernet── [Machine 2]
 (172.16.0.200)                                   (172.16.0.154)
 Nodes A, B, C, D, E                              Nodes F, G, H, I
```

### Multi-Host (3 machines via network switch)

#### Hardware Setup

You need:
- 1 unmanaged network switch (any port count)
- 3 ethernet cables

```
[Machine 1] ──ethernet──┐
 (172.16.0.100)          │
 Nodes A, B, C           │
                      [SWITCH]
[Machine 2] ──ethernet──┤
 (172.16.0.200)          │
 Nodes D, E, F           │
                         │
[Machine 3] ──ethernet──┘
 (172.16.0.154)
 Nodes G, H, I
```

#### Network Configuration (3-host)

```bash
# Machine 1:
sudo ifconfig <eth-interface> 172.16.0.100 netmask 255.255.255.0 up

# Machine 2:
sudo ifconfig <eth-interface> 172.16.0.200 netmask 255.255.255.0 up

# Machine 3:
sudo ifconfig <eth-interface> 172.16.0.154 netmask 255.255.255.0 up
```

#### Start Nodes (3-host)

```bash
# Machine 3 (172.16.0.154) — start workers first:
bash scripts/start_local.sh configs/nodes_multi_3host.json 172.16.0.154

# Machine 2 (172.16.0.200) — start relay + workers:
bash scripts/start_local.sh configs/nodes_multi_3host.json 172.16.0.200

# Machine 1 (172.16.0.100) — start gateway + relay + worker:
bash scripts/start_local.sh configs/nodes_multi_3host.json 172.16.0.100
```

**Run queries (from Machine 1):**

```bash
./build/client -c configs/nodes_multi_3host.json
```

**Stop nodes on each machine:**

```bash
bash scripts/stop_local.sh
```

---

#### 2-Host Network Configuration and Start (below)

1. Power on the switch
2. Connect Machine 1 to any switch port via ethernet cable
3. Connect Machine 2 to another switch port via ethernet cable
4. Verify link LEDs light up on the switch for both ports

#### Network Configuration (one-time per session)

The two machines communicate over a private `172.16.0.0/24` network. WiFi/internet remains unaffected.

**On Machine 1 (gateway + relays):**

```bash
# Find your ethernet interface name
ip link show
# Look for enp*, eth*, or eno* (NOT lo, wlan, or docker)

# Assign static IP
sudo ip addr add 172.16.0.200/24 dev <ethernet-interface>

# Verify interface is UP
ip link show <ethernet-interface>
# Should show: state UP
```

**On Machine 2 (workers):**

```bash
# Find ethernet interface
ip link show

# Assign static IP
sudo ip addr add 172.16.0.154/24 dev <ethernet-interface>
```

**Verify connectivity:**

```bash
# From Machine 1:
ping 172.16.0.154

# From Machine 2:
ping 172.16.0.200
```

If ping fails:
- Check switch has power and link LEDs are on
- Re-seat ethernet cables on both ends
- Try a different switch port or cable
- Run `ip link set <interface> up` if state shows DOWN

#### Code Setup (both machines)

```bash
git fetch origin
git checkout feat/multi-host-v2
./build.sh
```

Ensure `combined_parking_violations.csv` is present in the project root on both machines.

#### Start Nodes

```bash
# On Machine 2 (172.16.0.154) — start workers FIRST:
bash scripts/start_local.sh configs/nodes_multi.json 172.16.0.154

# On Machine 1 (172.16.0.200) — start gateway + relays:
bash scripts/start_local.sh configs/nodes_multi.json 172.16.0.200
```

Start workers before the gateway so that when A tries to connect to its neighbors, they're already listening.

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

Config files provided:

| File | Purpose |
|------|---------|
| `configs/nodes_single.json` | All nodes on localhost |
| `configs/nodes_multi.json` | 2-host: A-E on 172.16.0.200, F-I on 172.16.0.154 |
| `configs/nodes_multi_3host.json` | 3-host: A-C on 172.16.0.100, D-F on 172.16.0.200, G-I on 172.16.0.154 |

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
      "host": "172.16.0.200",
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

## Log & Result Archiving

Every time nodes are started (via `start_local.sh` or `run_all.sh`), existing logs and results are automatically archived before being cleared. Archives are stored in `archives/` with timestamps.

```
archives/
├── 20260507_225530/       # auto-timestamped
│   ├── A.log
│   ├── B.log
│   ├── ...
│   ├── results_2host.txt
│   └── results_2host_v2.txt
└── 20260508_143000_before_cache_test/   # with custom label
```

**Manual archiving:**

```bash
# Archive current logs/results with auto-timestamp
bash scripts/archive_logs.sh

# Archive with a custom label
bash scripts/archive_logs.sh "before_cache_test"
```

Archives are git-ignored and stay local to each machine.

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
│   └── client/client.cpp                 # C++ client (pull-based chunk retrieval)
├── python/server/server.py               # Python server (nodes F, H, I)
├── configs/
│   ├── nodes_single.json                 # Single-host config
│   └── nodes_multi.json                  # Multi-host config
├── scripts/
│   ├── start_local.sh                    # Start nodes belonging to this machine
│   ├── stop_local.sh                     # Stop local nodes
│   ├── run_all.sh                        # Start all 9 on localhost (legacy)
│   ├── stop_all.sh                       # Stop all (legacy)
│   ├── archive_logs.sh                   # Archive logs + results with timestamp
│   └── gen_proto.sh                      # Regenerate Python proto stubs
├── archives/                             # Timestamped log/result snapshots (git-ignored)
├── build.sh                              # CMake build + Python venv setup
└── CMakeLists.txt
```

---

## How It Works

1. Client calls `SubmitQuery` on Node A (gateway)
2. A converts to `ForwardRequest` and forwards to neighbors (B, G, H, I) in parallel
3. Relay nodes (B, E) propagate to their downstream neighbors in parallel, skipping the sender
4. Worker nodes scan the CSV, filter by their shard date range, then apply the query filter
5. Results are chunked and aggregated back up the tree
6. Gateway stores all chunks and returns metadata to the client
7. Client pulls chunks on demand via `FetchChunks(request_id, offset, limit)`
8. Nodes D and G have multiple parents — deduplication prevents re-processing
9. Repeated queries are served from LRU cache at every level

---

## gRPC Service API

| RPC | Purpose |
|-----|---------|
| `SubmitQuery(QueryRequest) → QueryResponse` | Submit query, get metadata (total_chunks, request_id) |
| `FetchChunks(FetchChunksRequest) → FetchChunksResponse` | Pull chunks by offset/limit |
| `ForwardQuery(ForwardRequest) → ForwardResponse` | Internal: propagate query through tree |
| `CancelQuery(CancelRequest) → CancelResponse` | Cancel in-flight query, propagates to all nodes |
| `HealthCheck(HealthRequest) → HealthResponse` | Node health check |

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
nc -zv 172.16.0.200 50051
nc -zv 172.16.0.154 50056
```
