# CMPE 275 Mini 2 — NYC Parking Violations Distributed Query System

A multi-process distributed query system built with C++ and gRPC over the NYC Parking Violations 2025 dataset. Processes form a tree overlay network where clients query a single portal (Process A), which delegates work across a 9-node tree and streams chunked results back.

---

## Dataset

**NYC Parking Violations Issuance — FY 2025**
- Source: NYC Open Data
- File: `parking_violations_2025.csv`
- Size: ~16.5 million records, 43 fields per record
- Partition key: `Violation County` (NYC county codes)

### County codes in the dataset

| County Code(s) | Borough |
|---|---|
| `NY`, `MN` | Manhattan |
| `BK`, `K`, `Kings` | Brooklyn |
| `QN`, `Q`, `Qns` | Queens |
| `BX`, `Bronx` | Bronx |
| `ST`, `R` | Staten Island |

> Multiple codes map to the same borough due to inconsistencies in the source data. All variants are listed in each process's `owned_counties` config field.

---

## Project Structure

```
.
├── proto/
│   └── parking_violation_query.proto       # gRPC service + message definitions
├── src/
│   ├── common/
│   │   ├── config.hpp                      # JSON config parser (no hardcoded settings)
│   │   ├── parking_violations_loader.hpp   # CSV parser and data loader
│   │   ├── thread_safe_queue.hpp           # Bounded thread-safe queue (backpressure)
│   │   ├── metrics.hpp                     # Metrics logging interface
│   │   └── metrics.cpp                     # Metrics implementation (stdout + log file)
│   ├── servers/
│   │   ├── leader/
│   │   │   └── leader_server.cpp           # Process A — accepts client queries, fans out
│   │   ├── team_leader/
│   │   │   └── team_leader_server.cpp      # Processes B, E — relay and multiplex
│   │   └── worker/
│   │       ├── worker_server.cpp           # Processes C, D, F, G, H — scan CSV, stream results
│   │       └── worker_server.py            # Process I — Python gRPC worker
│   ├── client/
│   │   └── client.cpp                      # C++ gRPC client (connects to Process A)
│   └── tools/
│       └── load_test.cpp                   # Standalone smoke-test for the CSV loader
├── configs/
│   ├── process_a.json                      # Leader / portal
│   ├── process_b.json                      # Blue team leader
│   ├── process_c.json                      # Worker — Manhattan (NY, MN)
│   ├── process_d.json                      # Worker — Bronx (BX, Bronx)
│   ├── process_e.json                      # Yellow team leader
│   ├── process_f.json                      # Worker — Brooklyn (BK, K, Kings)
│   ├── process_g.json                      # Worker — Queens (QN, Q, Qns)
│   ├── process_h.json                      # Worker — Staten Island (ST, R)
│   └── process_i.json                      # Python worker (no data assigned)
├── generate_proto.sh                       # Generates C++ and Python proto stubs
├── start_all.sh                            # Starts all 9 processes (single host)
├── stop_all.sh                             # Stops all running processes
├── CMakeLists.txt
└── README.md
```

---

## Network Topology

Tree overlay with 9 processes. A non-overlapping spanning tree is used for delegation so no worker is queried twice.

```
                        A  (leader — port 50051)
                      / | \
                     B  H   I
                   / | \
                  C  D   E
                        / \
                       F   G
```

**Delegation tree:** A → B, H, I &nbsp;|&nbsp; B → C, D, E &nbsp;|&nbsp; E → F, G

| Process | Team | Role | Owned Counties | Port |
|---|---|---|---|---|
| A | Blue | Leader / portal | — | 50051 |
| B | Blue | Team leader | — | 50052 |
| C | Yellow | Worker | NY, MN (Manhattan) | 50053 |
| D | Blue | Worker | BX, Bronx (Bronx) | 50054 |
| E | Yellow | Team leader | — | 50055 |
| F | Yellow | Worker | BK, K, Kings (Brooklyn) | 50056 |
| G | Yellow | Worker | QN, Q, Qns (Queens) | 50057 |
| H | Blue | Worker | ST, R (Staten Island) | 50058 |
| I | Yellow | Python worker | — (no data) | 50059 |

**Teams and physical machines (two-host deployment):**
- **Blue team** (A, B, D, H) → Computer 1
- **Yellow team** (C, E, F, G, I) → Computer 2

---

## Data Partitioning

Each worker process owns one borough's county codes. No data is shared or replicated between workers. Routing nodes (A, B, E) own no data.

```
parking_violations_2025.csv  (16.5M rows)
            │
   ┌────────┼──────────┬──────────┬──────────┐
   C         D          F          G          H
 NY/MN    BX/Bronx  BK/K/Kings  QN/Q/Qns   ST/R
Manhattan  Bronx     Brooklyn    Queens  Staten Island
```

When a query arrives with `county=BK`, only Process F opens the CSV. All other workers return an empty stream immediately without touching disk.

---

## How a Query Flows

```
Client → A: QueryViolations(county=BK, max=500)
  A serializes query → DelegationRequest
  A opens parallel streams to B, H, I

  H: owns [ST, R]       → BK not matched → empty
  I: owns []            → empty
  B: fans out to C, D, E
    C: owns [NY, MN]    → BK not matched → empty
    D: owns [BX, Bronx] → BK not matched → empty
    E: fans out to F, G
      F: owns [BK, K, Kings] → match ✓ → scans CSV → streams chunks
      G: owns [QN, Q, Qns]   → BK not matched → empty

Chunks flow: F → E → B → A → Client
```

---

## Prerequisites

- C++17 compiler (clang++ or g++)
- CMake ≥ 3.15
- Protobuf
- gRPC
- Python 3 + grpcio + grpcio-tools (for Process I)

```bash
# macOS
brew install protobuf grpc cmake

# Python dependencies
pip install grpcio grpcio-tools
```

---

## Steps to Run (Single Host)

### Step 1 — Go to the project folder

```bash
cd "/path/to/275-mini2-DOB-Permit-Issuance"
```

### Step 2 — Generate proto stubs (run once only)

```bash
./generate_proto.sh
```

### Step 3 — Build C++ binaries (run once only)

```bash
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j4
cd ..
```

This produces five binaries in `build/`:
- `leader_server` — Process A
- `team_leader_server` — Processes B, E
- `worker_server` — Processes C, D, F, G, H
- `client` — query client
- `load_test` — standalone CSV loader test

### Step 4 — Start all 9 processes

```bash
./start_all.sh
```

Processes start leaf-first (workers → team leaders → leader). Logs go to `logs/`.

### Step 5 — Run queries

```bash
# Health check
./build/client --health

# Query by borough
./build/client --county NY --max 100     # Manhattan
./build/client --county BK --max 200     # Brooklyn
./build/client --county BX --max 100     # Bronx
./build/client --county QN --max 100     # Queens
./build/client --county ST --max 100     # Staten Island

# All counties in parallel
./build/client --max 500

# With date range
./build/client --county NY --start 01/01/2025 --end 06/30/2025

# Print every record
./build/client --county ST --max 50 --verbose
```

### Step 6 — Stop all processes

```bash
./stop_all.sh
```

---

## Client Options

| Flag | Description | Default |
|---|---|---|
| `--host HOST` | Leader hostname | `localhost` |
| `--port PORT` | Leader port | `50051` |
| `--county COUNTY` | Filter by violation county | all |
| `--code CODE` | Filter by violation code | all |
| `--start MM/DD/YYYY` | Issue date range start | none |
| `--end MM/DD/YYYY` | Issue date range end | none |
| `--max N` | Max records to return | 1000 |
| `--health` | Health check only | — |
| `--verbose` | Print every record | — |

---

## Running the Load Test (CSV only, no gRPC)

```bash
# Load all records
./build/load_test "/path/to/parking_violations_2025.csv"

# Filter by county
./build/load_test "/path/to/parking_violations_2025.csv" NY

# Filter by county + violation code
./build/load_test "/path/to/parking_violations_2025.csv" BK 38

# Filter by county + violation code + max records
./build/load_test "/path/to/parking_violations_2025.csv" QN 36 1000
```

---

## Measuring Performance

### 1. Client latency

The client prints elapsed time after every query:
```
[Client] query complete
  total_records = 100
  total_chunks  = 2
  elapsed_ms    = 6
```

### 2. Metrics logs

Every process writes structured event logs to `logs/<process_id>_<role>.log`:
```bash
tail -f logs/*.log
```

Key events: `RECV_QUERY`, `RECV_DELEGATION`, `LOADED`, `CHUNK_SENT`, `RELAY_CHUNK`, `QUERY_DONE`, `CANCELLED`.

### 3. Concurrent clients

```bash
for i in 1 2 3 4 5; do
    ./build/client --county NY --max 1000 &
done
wait
```

---

## Configuration

Each process reads its settings from a JSON config file. No settings are hardcoded.

```json
{
  "process_id": "F",
  "role": "worker",
  "listen_host": "0.0.0.0",
  "listen_port": 50056,
  "data_path": "/path/to/parking_violations_2025.csv",
  "team": "yellow",
  "is_team_leader": false,
  "edges": [],
  "data_partitioning": {
    "strategy": "county",
    "owned_counties": ["BK", "K", "Kings"]
  },
  "chunk_config": {
    "default_chunk_size": 500,
    "max_chunk_size": 2000,
    "min_chunk_size": 100
  }
}
```

For two-host deployment, update the `host` fields in the `edges` arrays to the actual IP addresses of the remote machines.

---

## Proto Definition

Service defined in [proto/parking_violation_query.proto](proto/parking_violation_query.proto).

| Message | Purpose |
|---|---|
| `QueryRequest` | Client filter: county, violation code, date range, max records |
| `QueryResponse` | Chunked streaming response to client (`is_final` on last chunk) |
| `DelegationRequest` | Internal: carries serialized `QueryRequest` as bytes |
| `DelegationResponse` | Internal: chunk of `ViolationRecord` messages flowing back up |
| `ViolationRecord` | Full typed record — 43 fields (int64, int32, bool, string) |
| `HealthRequest/Response` | Liveness check |
| `CancelRequest/Response` | Cancellation acknowledgement |

---

## Troubleshooting

| Error | Fix |
|---|---|
| `grpc_cpp_plugin not found` | `brew install grpc` |
| `No module named grpc_tools` | `pip install grpcio grpcio-tools` |
| `Binary not found in build/` | Run `make -j4` inside `build/` |
| `Connection refused on 50051` | Run `./start_all.sh` first |
| `Failed to parse query` | Re-run `./generate_proto.sh` and rebuild |
| CSV path wrong | Update `data_path` in all `configs/*.json` |
| Segmentation fault on startup | Ensure gRPC is installed via `brew install grpc` and rebuild with `cmake .. && make -j4` |