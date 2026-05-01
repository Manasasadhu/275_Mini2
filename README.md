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

---

## Implementation Status

### ✅ Completed
- [x] Shared JSON config system (`configs/nodes.json`)
- [x] Config loader with global defaults + per-node overrides (`src/common/config.hpp`)
- [x] Proto definitions for gRPC service (`proto/parking_violation_query.proto`)
- [x] Server skeleton with gRPC service setup (`src/server/server.cpp`)
- [x] Client implementation for submitting queries (`src/client/client.cpp`)
- [x] Compact sharding utility for worker CSV scanning (`src/common/sharding.hpp`)
  - Date range filtering by shard
  - Plate ID search
  - CSV parsing and chunking
- [x] Client implementation for executing queries against gateway (`src/client/client.cpp`)

### 🟡 In Progress / Pending
- [ ] **Gateway Forwarding**: Node A forwards queries to all neighbors (B, G, H, I)
- [ ] **Multi-hop Aggregation**: Intermediate nodes (B, E) aggregate chunks from downstream and forward upstream
- [ ] **Deduplication**: Nodes D and G handle duplicate processing on converging paths (multiple parents)
- [ ] **Full end-to-end testing**: Build, start all nodes, test tree queries
- [ ] Support for violation_code and date_range query types (plate_id working)

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

### 1. Start All Nodes
```bash
cd /Users/aravindreddy/IdeaProjects/275_Mini2

# Start all 9 nodes (A-I) in background
./scripts/run_all.sh

# Logs will appear in logs/<node_id>.log
tail -f logs/A.log
tail -f logs/C.log
```

**What run_all.sh does**:
- Compiles server code if needed
- Starts C++ servers for nodes A, B, C, D, E, G (cpp=true)
- Starts Python servers for nodes F, H, I (python support planned)
- Each process runs with: `server -n <node_id> -c configs/nodes.json`
- Writes logs to `logs/<node_id>.log`
- Stores PIDs in `logs/<node_id>.pid`

### 2. Run a Test Query
```bash
# In another terminal, submit a query through gateway (A)
./build/client -c configs/nodes.json

# Expected output:
# Query successful. Received X chunks.
# Chunk: Y records (is_last=false)
#   Plate: ABC123, Date: 03/15/2022, Code: 36
#   ...
# Chunk: Z records (is_last=true)
# Total records: X+Y+Z
```

### 3. Stop All Nodes
```bash
./scripts/stop_all.sh

# Cleans up PIDs, kills all processes
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
│   ├── run_all.sh                             # Start all node servers
│   ├── stop_all.sh                            # Stop all servers
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

## Development Roadmap

| Priority | Feature | Status | Owner |
|----------|---------|--------|-------|
| P0 | Build + proto generation | 🟢 Ready | — |
| P1 | Single worker CSV scan | 🟢 Ready | sharding.hpp |
| P2 | Gateway neighbor forwarding | 🟡 TODO | server.cpp |
| P3 | Multi-hop aggregation | 🟡 TODO | server.cpp |
| P4 | Deduplication at D, G | 🟡 TODO | server.cpp |
| P5 | violation_code queries | 🟡 TODO | sharding.hpp |
| P6 | date_range queries | 🟡 TODO | sharding.hpp |
| P7 | Python node support (F, H) | 🟡 TODO | python/server.py |
| P8 | Compression + optimization | ⚪️ Backlog | — |

---

## Key Implementation Details

### Worker Query Processing (`src/common/sharding.hpp`)

**Function**: `process_query_on_shard()`
- Opens combined CSV file from config
- Skips header row
- Iterates through all rows:
  1. Parse CSV line into ViolationRecord protobuf
  2. Extract `Issue Date` field, normalize to YYYYMMDD format
  3. Check if date falls within node's shard range
  4. If yes, apply query filters (e.g., plate_id matching)
  5. If match, add record to current chunk buffer
  6. When buffer reaches `chunk_size`, yield chunk with `is_last=false`
  7. At end of file, yield final chunk with `is_last=true`

**Why Single CSV?**
- No replication overhead
- Simple sharding config (just date ranges)
- Workers only read their date range → I/O efficient
- Easy to rebalance shards later

### Server RPC Handlers (`src/server/server.cpp`)

**SubmitQuery** (Gateway RPC):
- Called by client on node A
- Currently: logs query receipt, stub implementation
- TODO: Forward to all neighbors (B, G, H, I), aggregate results

**ForwardQuery** (Node-to-Node RPC):
- Called by upstream node to downstream node
- Streams chunks back
- Currently: calls `process_query_on_shard()` to filter local data
- Returns matching records in chunks

**CancelQuery** (Async Cancel):
- Placeholder for canceling in-flight queries

**HealthCheck** (Liveness Probe):
- Returns `healthy=true`
- Used to verify node is running

### Deduplication Strategy

Nodes D and G have **two parents** in tree, so may receive same query from two paths:
- Path 1: A → B → D
- Path 2: A → B → E → D

**Solution** (TODO):
- Track `seen_request_ids` in each node
- Skip processing if request already seen
- Still return results from first processing

---

## Known Limitations & TODOs

1. **Gateway Forwarding Not Implemented**: Node A currently logs queries but doesn't forward to neighbors
2. **Aggregation at Relay Nodes Not Implemented**: B and E don't combine results from downstream
3. **No Deduplication Yet**: D and G will process same query twice if both parents forward
4. **Plate ID Only**: violation_code and date_range query types not yet wired up
5. **Python Server Stub**: F and H not yet implemented (use Python gRPC server)
6. **No Error Handling**: CSV parsing, network errors assumed won't happen
7. **Single Machine Testing**: All nodes run on localhost (ready for distribution to 2 machines if needed)

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
- `QueryRequest` — filter by county, violation code, date range, agency
- `ViolationRecord` — full typed record (int64 summons, int32 codes, bool flags, doubles for future geo fields)
- `QueryResponse` — chunked streaming response
- `DelegationRequest` / `DelegationResponse` — internal inter-process messages

---

## Configuration

Each process reads its settings from a JSON config file at startup. **No settings are hardcoded in source code.**

```json
{
  "process_id": "C",
  "role": "worker",
  "listen_host": "0.0.0.0",
  "listen_port": 50053,
  "data_path": "/path/to/parking_violations_2025.csv",
  "team": "yellow",
  "is_team_leader": false,
  "edges": [],
  "data_partitioning": {
    "strategy": "county",
    "owned_counties": ["NY", "MN"]
  },
  "chunk_config": {
    "default_chunk_size": 500,
    "max_chunk_size": 2000,
    "min_chunk_size": 100
  }
}
```

For multi-computer deployment, update `host` fields in the `edges` arrays to the actual IP addresses of the remote machines.