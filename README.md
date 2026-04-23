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

> Note: Multiple codes map to the same borough due to inconsistencies in the source data. All variants are listed in each process's `owned_counties` config field.

---

## Project Structure

```
.
├── proto/
│   └── parking_violation_query.proto   # gRPC service + message definitions
├── src/
│   ├── common/
│   │   ├── config.hpp                  # JSON config parser (no hardcoded settings)
│   │   └── parking_violations_loader.hpp  # CSV parser and data loader
│   └── tools/
│       └── load_test.cpp               # Standalone smoke-test for the loader
├── configs/
│   ├── process_a.json                  # Leader / portal
│   ├── process_b.json                  # Blue team leader
│   ├── process_c.json                  # Worker — Manhattan (NY, MN)
│   ├── process_d.json                  # Worker — Bronx (BX)
│   ├── process_e.json                  # Yellow team leader
│   ├── process_f.json                  # Worker — Brooklyn (BK, K, Kings)
│   ├── process_g.json                  # Worker — Queens (QN, Q)
│   ├── process_h.json                  # Worker — Staten Island (ST)
│   └── process_i.json                  # Worker — unassigned (future)
├── CMakeLists.txt
├── build.sh
└── README.md
```

---

## Network Topology

Tree overlay with 9 processes across 2 computers.

```
Tree edges: AB, BC, BD, BE, EF, ED, EG, AH, AG, AI

                    A  (leader/portal — no data)
                  / | \ \
                 B  H  G  I
               / | \
              C  D   E
                    / | \
                   F  D   G
```

| Process | Team | Role | Owns (county codes) | Port |
|---|---|---|---|---|
| A | blue | leader / portal | — | 50051 |
| B | blue | team leader | — | 50052 |
| C | yellow | worker | NY, MN (Manhattan) | 50053 |
| D | blue | worker | BX (Bronx) | 50054 |
| E | yellow | team leader | — | 50055 |
| F | yellow | worker | BK, K, Kings (Brooklyn) | 50056 |
| G | yellow | worker | QN, Q (Queens) | 50057 |
| H | blue | worker | ST (Staten Island) | 50058 |
| I | yellow | worker | — (unassigned) | 50059 |

**Teams and physical machines:**
- **Blue team** (A, B, D, H) → Computer 1
- **Yellow team** (C, E, F, G, I) → Computer 2

---

## Data Partitioning

Each worker process loads only its assigned county slice from the flat CSV at startup. No data is shared or replicated between teams.

```
parking_violations_2025.csv  (16.5M rows)
         │
    ┌────┴────┬─────────┬──────────┬──────────┐
    C         D         F          G          H
  NY/MN      BX      BK/K/Kings  QN/Q        ST
Manhattan   Bronx    Brooklyn   Queens  Staten Island
```

---

## Build

**Prerequisites:** C++17 compiler, CMake ≥ 3.15, Protobuf, gRPC (via Homebrew on macOS)

```bash
# Install dependencies (macOS)
brew install protobuf grpc cmake

# Build
./build.sh
```

Or manually:

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make load_test
```

---

## Running the Loader Test

```bash
# Load all records
./build/load_test "/path/to/parking_violations_2025.csv"

# Filter by county (Manhattan)
./build/load_test "/path/to/parking_violations_2025.csv" NY

# Filter by county + violation code
./build/load_test "/path/to/parking_violations_2025.csv" BK 38

# Filter by county + violation code + max records
./build/load_test "/path/to/parking_violations_2025.csv" QN 36 1000
```

Override the CSV path at runtime without editing configs:

```bash
export PARKING_DATA_PATH="/path/to/parking_violations_2025.csv"
./build/load_test $PARKING_DATA_PATH
```

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