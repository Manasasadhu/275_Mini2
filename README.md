# CMPE 275 Mini 2 — Distributed Parking Violations Query System

A multi-node distributed query system for NYC parking violations data. 9 nodes form a fixed tree overlay network using gRPC. Clients submit queries through a gateway node (A), which propagates them through the tree. Worker nodes filter and stream results in chunks back to the client.

---

## What We're Building

**Distributed Query System Over Tree Network**
- **9 Nodes (A-I)**: Fixed tree topology, two teams (Blue and Yellow)
- **Query Gateway**: Node A accepts client queries via gRPC
- **Worker Sharding**: Workers (C, D, F, G, H, I) logically shard the combined dataset by **Issue Date ranges**
- **Streaming Results**: All responses chunked and streamed back to client
- **Team Organization**: Blue team (A,B,D,H) with leader B; Yellow team (C,E,F,G,I) with leader E

**Query Types Supported**:
1. **Plate ID**: Find all violations for a specific license plate
2. **Violation Code**: Find all violations of a specific type
3. **Date Range**: Find all violations within a date window
4. **PlateViolationHistory**: Plate + violation code + date range + registration state
5. **ViolationCodeDateRange**: Violation code + date range
6. **PrecinctVehicleAnalysis**: County + precinct + vehicle year range + body type
7. **UnregisteredVehicleLookup**: Unregistered flag + state + feet-from-curb threshold

---

## Implementation Status

### Completed
- [x] Shared JSON config system (`configs/nodes.json`)
- [x] Config loader with global defaults + per-node overrides (`src/common/config.hpp`)
- [x] Proto definitions for gRPC service with 7 query types (`proto/parking_violation_query.proto`)
- [x] C++ server with gateway forwarding, relay aggregation, worker shard processing (`src/server/server.cpp`)
- [x] C++ client for all query types with timing (`src/client/client.cpp`)
- [x] Python server for nodes F, H, I with full query support (`python/server/server.py`)
- [x] Sharding engine: date filtering, CSV parsing, query matching for all 7 types (`src/common/sharding.hpp`)
- [x] Gateway forwarding: Node A forwards queries to all neighbors (B, G, H, I)
- [x] Multi-hop aggregation: Relay nodes (B, E) aggregate chunks from downstream
- [x] Deduplication: Nodes D and G skip already-processed requests
- [x] Multi-host support: nodes read host+port from config, servers bind to 0.0.0.0
- [x] Smart startup scripts for single-host and multi-host deployment

---

## Architecture & System Flow

### Network Topology (Fixed Tree)

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

| Node | Team | Role | Host | Port | Shard (Issue Date) | Language |
|------|------|------|------|------|---|---|
| A | Blue | Gateway | localhost | 50051 | — | C++ |
| B | Blue | Relay | localhost | 50052 | — | C++ |
| C | Yellow | Worker | localhost | 50053 | 2022-01-01 to 2022-06-30 | C++ |
| D | Blue | Worker | localhost | 50054 | 2022-07-01 to 2022-12-31 | C++ |
| E | Yellow | Relay | localhost | 50055 | — | C++ |
| F | Yellow | Worker | localhost | 50056 | 2023-01-01 to 2023-03-31 | Python |
| G | Yellow | Worker | localhost | 50057 | 2023-04-01 to 2023-06-30 | C++ |
| H | Blue | Worker | localhost | 50058 | 2023-07-01 to 2023-09-30 | Python |
| I | Yellow | Worker | localhost | 50059 | 2023-10-01 to 2023-12-31 | C++ |

**Key**: 
- **Gateway** (A): Entry point for all client queries
- **Relay** (B, E): Forward queries, aggregate results
- **Worker** (C, D, F, G, H, I): Filter local shard of combined dataset, return chunks

### Query Flow (Example: Plate ID Search)

**Step-by-step execution:**

```
Step 1: CLIENT → GATEWAY (A)
┌─────────────────────────────────────────────────────┐
│ Client: QueryByPlateId("req1", "ABC123")            │
│         → RPC call: SubmitQuery to A:50051          │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│ Node A (Gateway):                                   │
│ SubmitQuery(request_id="req1", plate_id="ABC123")   │
│ └─ Receives query from client                       │
│ └─ Logs: "Gateway received query..."                │
│ └─ [TODO: Forward to neighbors B, G, H, I]          │
└─────────────────────────────────────────────────────┘

Step 2: GATEWAY FORWARDS TO NEIGHBORS (NOT YET IMPLEMENTED)
                         ↓
        ┌────────────┬──────────┬────────────┬─────────┐
        ↓            ↓          ↓            ↓         ↓
    ┌────────┐  ┌────────┐ ┌────────┐ ┌────────┐
    │ Node B │  │ Node G │ │ Node H │ │ Node I │
    │(relay) │  │(worker)│ │(worker)│ │(worker)│
    │no shard│  │shard Q4│ │shard Q3│ │shard Q4│
    └────────┘  └────────┘ └────────┘ └────────┘
        ↓            ↓          ↓            ↓
        │            │          │     (processes locally)
        │
        └─ Relays downstream to C, D, E

Step 3: CASCADING FORWARDING THROUGH TREE
        ┌─────────────────┬─────────────────┐
        ↓                 ↓                 ↓
    ┌────────┐       ┌────────┐       ┌────────┐
    │ Node C │       │ Node D │       │ Node E │
    │(worker)│       │(worker)│       │(relay) │
    │shard Q1│       │shard Q2│       │no shard│
    └────────┘       └────────┘       └────────┘
        ↓                 ↓                 ↓
   (processes)       (processes)      (relays F,G)
                                           ↓
                                    ┌──────────────┐
                                    ↓              ↓
                                ┌────────┐    ┌────────┐
                                │ Node F │    │ Node G │
                                │(worker)│    │(worker)│
                                │shard Q3│    │shard Q4│
                                └────────┘    └────────┘
                                    ↓              ↓
                               (processes)   (processes)

Step 4: WORKER PROCESSING — sharding.hpp IS CALLED HERE ⬅️
        Each worker invokes: process_query_on_shard()
        
        process_query_on_shard(data_file, shard_start, shard_end, "ABC123", chunk_size):
        ├─ Open CSV: parking_violations_combined.csv (11GB)
        ├─ Skip header
        ├─ FOR EACH ROW:
        │  ├─ Extract Issue Date
        │  ├─ Check: date_in_range(issue_date, shard_start, shard_end)?
        │  │  └─ Node C checks: 01/01/2022 ≤ date ≤ 06/30/2022
        │  │  └─ Node D checks: 07/01/2022 ≤ date ≤ 12/31/2022
        │  │  └─ Node F checks: 01/01/2023 ≤ date ≤ 03/31/2023
        │  │  └─ Node G checks: 04/01/2023 ≤ date ≤ 06/30/2023
        │  │  └─ Node H checks: 07/01/2023 ≤ date ≤ 09/30/2023
        │  │  └─ Node I checks: 10/01/2023 ≤ date ≤ 12/31/2023
        │  │
        │  ├─ If date in shard:
        │  │  ├─ Parse CSV line → ViolationRecord
        │  │  ├─ Check plate_id == "ABC123"?
        │  │  ├─ If yes: add to current chunk
        │  │  └─ If chunk_size reached: yield chunk, start new chunk
        │  │
        │  └─ Else: skip row
        │
        ├─ Return all chunks with last chunk marked is_last=true
        └─ Response sent back via gRPC ForwardResponse

Step 5: RESULTS AGGREGATE UPSTREAM
        
        Worker responses → Relay → Gateway → Client
        
        Node C chunk(s) →─┐
        Node D chunk(s) →─┤  
        Node F chunk(s) →─┼→ Node E aggregates ──→ Node B aggregates ──→ Node A ──→ CLIENT
        Node G chunk(s) →─┤  (shard Q3, Q4)                (all results)      (final)
        Node H chunk(s) →─┤
        Node I chunk(s) →─┘
        
        A deduplicates results from D and G
        (both have multiple parent paths in tree)

Step 6: CLIENT RECEIVES RESULTS
        
        QueryResponse contains:
        ├─ chunks[0]: {records: [ViolationRecord, ...], is_last: false}
        ├─ chunks[1]: {records: [ViolationRecord, ...], is_last: false}
        └─ chunks[N]: {records: [ViolationRecord, ...], is_last: true}
        
        Output:
        "Query successful. Received X chunks."
        "Chunk: Y records (is_last=false)"
        "  Plate: ABC123, Date: 03/15/2022, Code: 36"
        "  Plate: ABC123, Date: 05/22/2022, Code: 14"
        ...
        "Total records: sum(all records from all chunks)"
```

**Key Processing Points:**

| Component | When Called | What It Does |
|-----------|-------------|---|
| `SubmitQuery()` | Client → Node A | Receives query, should forward to neighbors |
| `ForwardQuery()` | Node → Node | Routes query downstream, or processes locally |
| `process_query_on_shard()` | Worker node | Scans CSV, filters by date/shard, filters by query, chunks results |
| `date_in_range()` | Called per CSV row | Checks if Issue Date is within shard range |
| `parse_csv_line()` | Called per matching row | Converts CSV fields to protobuf ViolationRecord |

### Sharding Strategy

**Logical Partitioning by Issue Date**:
- No physical file split
- All workers read the **same combined CSV** from shared path
- Each worker's Issue Date shard determines which rows it processes:
  - Node C: Jan 1, 2022 - Jun 30, 2022
  - Node D: Jul 1, 2022 - Dec 31, 2022
  - Node F: Jan 1, 2023 - Mar 31, 2023
  - Node G: Apr 1, 2023 - Jun 30, 2023
  - Node H: Jul 1, 2023 - Sep 30, 2023
  - Node I: Oct 1, 2023 - Dec 31, 2023

**Advantage**: Simple configuration, no data duplication, easy rebalancing

---

## Data Setup

### Dataset Path
```
/Users/aravindreddy/Downloads/SJSU\ ClassWork/275\ EAD/275_Mini2_Dataset/parking_violations_combined.csv
```

**CSV Format** (from parking_violations_combined.csv):
```
Summons Number,Plate ID,Registration State,Plate Type,Issue Date,Violation Code,Violation Description,Issuing Agency,...
```

**Important Fields**:
- `Plate ID` (column 2): License plate number
- `Issue Date` (column 5): Date in MM/DD/YYYY format
- `Violation Code` (column 6): Code for violation type
- File size: ~11 GB

### Regenerate Python protobuf stubs
If the generated Python gRPC files are missing or need to be recreated, run:

```bash
bash scripts/gen_proto.sh
```

This regenerates:
- `python/server/parking_violation_query_pb2.py`
- `python/server/parking_violation_query_pb2_grpc.py`
- `python/server/parking_violation_query_pb2.pyi`

These files are generated from `proto/parking_violation_query.proto` and are intentionally not tracked in Git.

---

## Configuration System

### Single Shared Config: `configs/nodes.json`

All 9 nodes configured in one file with:
1. Global defaults section
2. Per-node overrides

**Example Structure**:
```json
{
  "global": {
    "default_chunk_size": 500,
    "shared_data_file": "/path/to/parking_violations_combined.csv",
    "date_field": "Issue Date"
  },
  "nodes": {
    "A": {
      "node_id": "A",
      "host": "localhost",
      "port": 50051,
      "neighbors": ["B", "G", "H", "I"],
      "language": "cpp",
      "role": "gateway",
      "team": "blue",
      "team_leader": false,
      "client_facing": true,
      "data_file": null,
      "shard": null
    },
    "C": {
      "node_id": "C",
      "host": "localhost",
      "port": 50053,
      "neighbors": ["B"],
      "language": "cpp",
      "role": "worker",
      "team": "yellow",
      "team_leader": false,
      "chunk_size": 500,
      "data_file": null,
      "shard": {
        "type": "issue_date_range",
        "start": "2022-01-01",
        "end": "2022-06-30"
      }
    }
  }
}
```

**Key Features**:
- `global.shared_data_file`: All workers read from this path
- `global.default_chunk_size`: Response chunk size (records per chunk)
- `nodes[X].shard`: Date range this worker processes (workers only)
- `nodes[X].neighbors`: Tree overlay connections (fixed)
- `nodes[X].role`: gateway | relay | worker
- `nodes[X].language`: cpp | python (for future polyglot support)

---

## gRPC Service Definition

**File**: `proto/parking_violation_query.proto`

### RPCs
```protobuf
service ParkingViolationService {
  rpc SubmitQuery(QueryRequest) returns (QueryResponse);      // Client → Gateway (A)
  rpc ForwardQuery(QueryRequest) returns (Chunk);             // Node → Node (streaming)
  rpc CancelQuery(CancelRequest) returns (CancelResponse);    // Cancel in-flight queries
  rpc HealthCheck(Empty) returns (HealthStatus);              // Node health probe
}
```

### Message Types
```protobuf
message QueryRequest {
  string request_id = 1;
  int32 chunk_size = 2;  // desired chunk size
  oneof query_type {
    string plate_id = 3;
    string violation_code = 4;
    DateRange issue_date_range = 5;
  }
}

message Chunk {
  string request_id = 1;
  repeated ViolationRecord records = 2;
  bool is_last = 3;  // marks final chunk
}

message ViolationRecord {
  int64 summons_number = 1;
  string plate_id = 2;
  string registration_state = 3;
  string plate_type = 4;
  string issue_date = 5;
  string violation_code = 6;
  string violation_description = 7;
  string issuing_agency = 8;
  // ... additional fields as needed
}
```

---

## Build & Deployment

### Prerequisites
- C++17 compiler (clang on macOS)
- CMake ≥ 3.15
- Protocol Buffers (protoc)
- gRPC C++ libraries
- Python 3.8+ (for Python nodes F, H)

### Install Dependencies (macOS)
```bash
brew install protobuf grpc cmake
```

### Build
```bash
cd /Users/aravindreddy/IdeaProjects/275_Mini2

# Options 1: Use build script
./build.sh

# Or manually:
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j4
```

**Build Outputs**:
- `build/server` — C++ server (compiled from src/server/server.cpp)
- `build/client` — C++ client (compiled from src/client/client.cpp)
- `build/parking_violation_query.pb.cc/.pb.h` — Generated proto files
- `build/parking_violation_query.grpc.pb.cc/.grpc.pb.h` — Generated gRPC files

### CMakeLists.txt
Configured to:
- Find protobuf and gRPC via CMake
- Generate proto code automatically
- Link server and client against gRPC libraries

---

## Running the System

### Single-Host (all 9 nodes on one machine)

```bash
# Start all nodes
bash scripts/start_node.sh localhost

# Run a query
./build/client -c configs/nodes.json

# Stop all nodes
bash scripts/stop_node.sh
```

### Multi-Host (2 or more machines)

**Step 1: Update `configs/nodes.json` on ALL machines**

Set each node's `host` to the IP of the machine it will run on:
```json
"A": { "host": "192.168.1.10", "port": 50051, ... },
"B": { "host": "192.168.1.10", "port": 50052, ... },
"F": { "host": "192.168.1.20", "port": 50056, ... },
"G": { "host": "192.168.1.20", "port": 50057, ... }
```

Also update `shared_data_file` to the CSV path on each machine.

All machines must have the same `nodes.json`.

**Step 2: Start nodes on each machine**

The script auto-detects which nodes belong to this machine by matching the local IP:
```bash
# On Machine 1 (192.168.1.10) — start remote workers FIRST
bash scripts/start_node.sh

# On Machine 2 (192.168.1.20)
bash scripts/start_node.sh

# Or specify IP manually if auto-detect doesn't work:
bash scripts/start_node.sh 192.168.1.10
```

Start machines with worker/leaf nodes first, then the machine with the gateway (A).

**Step 3: Run the client (from the machine where A runs)**
```bash
./build/client -c configs/nodes.json
```

**Step 4: Stop nodes on each machine**
```bash
bash scripts/stop_node.sh
```

### What `start_node.sh` does automatically
- Detects this machine's IP and finds matching nodes in config
- Builds C++ server if binary is missing
- Generates Python proto stubs if missing
- Installs grpcio if missing
- Stops any previously running nodes
- Starts nodes in dependency order: workers first, then relays, then gateway
- Logs to `logs/<node_id>.log`, PIDs in `logs/<node_id>.pid`

### 3-Machine Example

```
Machine 1 (192.168.1.10): A, B, C
Machine 2 (192.168.1.20): D, E, F
Machine 3 (192.168.1.30): G, H, I
```

Update `nodes.json` with the IPs above, copy to all 3 machines, then run `bash scripts/start_node.sh` on each. The script handles the rest.

### Legacy Scripts

- `scripts/run_all.sh` — starts all 9 nodes on localhost (single-host only)
- `scripts/stop_all.sh` — stops all nodes by PID

### Verify connectivity between machines
```bash
# From Machine 1, check Machine 2's ports:
nc -zv 192.168.1.20 50056
nc -zv 192.168.1.20 50057
```

---

## Project Structure

```
.
├── proto/
│   └── parking_violation_query.proto          # gRPC service definition
├── src/
│   ├── common/
│   │   ├── config.hpp                         # JSON config parser + NodeConfig struct
│   │   ├── sharding.hpp                       # Worker CSV scanning + filtering logic
│   │   └── parking_violations_loader.hpp      # Legacy data loader (may refactor)
│   ├── server/
│   │   └── server.cpp                         # gRPC server (gateway/relay/worker logic)
│   ├── client/
│   │   └── client.cpp                         # gRPC client for submitting queries
│   └── tools/
│       └── load_test.cpp                      # Standalone CSV loader test
├── configs/
│   └── nodes.json                             # Single shared config for all 9 nodes
├── scripts/
│   ├── start_node.sh                          # Smart startup (single-host + multi-host)
│   ├── stop_node.sh                           # Stop local nodes
│   ├── run_all.sh                             # Legacy: start all on localhost
│   ├── stop_all.sh                            # Legacy: stop all servers
│   └── gen_proto.sh                           # Regenerate Python protobuf stubs
├── build/                                     # CMake output (auto-generated)
├── logs/                                      # Node log files + PIDs
├── CMakeLists.txt
├── build.sh
└── README.md (this file)
```

---

## Testing Strategy

### Phase 1: Single Worker Test (Unit)
```bash
# Spin up just Node C (worker with 2022-H1 shard)
# Send plate_id query
# Verify results come back with correct date range
```

### Phase 2: Gateway + Single Worker (Integration)
```bash
# Start nodes A (gateway) and C (worker)
# Client submits query through A
# A forwards to B, G, H, I neighbors
# Verify C returns results, others return empty
```

### Phase 3: Full Tree Query (System)
```bash
# Start all 9 nodes
# Submit plate_id query
# Verify results aggregate from multiple workers
# Check for deduplication at D and G (multi-parent nodes)
```

### Phase 4: Multi-Query Types (Feature)
```bash
# Test violation_code queries
# Test date_range queries
# Test mixed team queries (Blue vs Yellow)
```

---

## Key Implementation Details

### Worker Query Processing (`src/common/sharding.hpp`)

**Function**: `process_query_on_shard()`
- Opens combined CSV file from config
- Skips header row
- Iterates through all rows:
  1. Quick date extraction from column 4 for shard filtering
  2. Normalize date (MM/DD/YYYY → YYYYMMDD) and check shard range
  3. If in range, parse full CSV line into ViolationRecord protobuf
  4. Apply query-specific filter via `matches_query()` (supports all 7 query types)
  5. If match, add record to current chunk buffer
  6. When buffer reaches `chunk_size`, yield chunk with `is_last=false`
  7. At end of file, yield final chunk with `is_last=true`

### Server RPC Handlers (`src/server/server.cpp`)

**SubmitQuery** (Gateway RPC):
- Called by client on node A
- Converts QueryRequest to ForwardRequest, forwards to all neighbors
- Aggregates chunks from all neighbors into final QueryResponse

**ForwardQuery** (Node-to-Node RPC):
- Deduplication check via `processed_requests_` set
- If worker: calls `process_query_on_shard()` on local CSV data
- If relay/gateway: forwards to all neighbors except sender
- Aggregates and returns all collected chunks

**CancelQuery**: Placeholder for canceling in-flight queries

**HealthCheck**: Returns `healthy=true`

### Deduplication Strategy

Nodes D and G have two parents in tree:
- D: reachable via A→B→D and A→B→E→D
- G: reachable via A→G and A→B→E→G

Each node tracks `processed_requests_` (set of request IDs). If a query arrives that was already processed, it returns an empty response immediately.

### Multi-Host Support

Servers bind to `0.0.0.0` (configurable via `listen_host` in JSON) so they accept connections from any machine. Neighbor connections use the `host` field from `nodes.json` — set to `localhost` for single-host, or real IPs for multi-host.

---

## Known Limitations

1. **Sequential forwarding**: Gateway and relays forward to neighbors one at a time (not parallel)
2. **No cancel propagation**: CancelQuery is a stub, doesn't stop in-flight processing
3. **Dedup set grows forever**: `processed_requests_` never cleaned up
4. **No dynamic chunk sizing**: Chunk size is fixed from config

---

## Debugging

### View Node Logs
```bash
tail -f logs/A.log    # Gateway
tail -f logs/C.log    # Worker C
tail -f logs/D.log    # Worker D (has 2 parents)
```

### Enable Detailed Logging
Edit `src/server/server.cpp` to add verbose output:
```cpp
std::cout << "[" << node_config_.node_id << "] Received ForwardQuery: " 
          << request.plate_id() << std::endl;
```

### Test CSV Parsing Standalone
```bash
# Load a few records from combined CSV (no proto/gRPC)
# Useful for debugging date_in_range or CSV field parsing issues
./build/load_test /path/to/parking_violations_combined.csv
```

---

## Contact & Questions

For questions about:
- **Architecture**: See "Network Topology" and "Query Flow" sections
- **Configuration**: See "Configuration System" section
- **Sharding**: See "Sharding Strategy" and "Worker Query Processing" sections
- **Building**: See "Build & Deployment" section

### Sample output

```
=== NYC Parking Violations Loader ===
CSV path       : parking_violations_2025.csv
County         : (all)
Max records    : 2000

Total data rows (excl. header): 16559243  (3241 ms)
Loaded 2000 records in 5 ms

--- County distribution ---
BK       : 438
BX       : 335
NY       : 591
QN       : 524
ST       : 112

--- Top violation codes ---
  code   38 : 412
  code   36 : 287
  ...

--- Chunk split preview (chunk_size=500) ---
  Total records : 2000
  Num chunks    : 4
  Chunk 0 : rows 0 - 499 (500 records)
  ...
```

---

## Proto Definition

The service is defined in [proto/parking_violation_query.proto](proto/parking_violation_query.proto).

Key message types:
- `QueryRequest` — 7 query types via oneof (plate_id, violation_code, issue_date, plate_violation_history, violation_code_date_range, precinct_vehicle_analysis, unregistered_vehicle_lookup)
- `ForwardRequest` — same query types + from_node field for tree routing
- `ViolationRecord` — 35 typed fields (int64 summons, int32 codes/precinct/year, bool unregistered, strings)
- `Chunk` — batch of records with is_last flag
- `QueryResponse` / `ForwardResponse` — repeated chunks