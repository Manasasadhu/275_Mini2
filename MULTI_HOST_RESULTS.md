# Multi-Host Testing Results

## Setup

### Network
- **Switch:** Netgear unmanaged switch (Layer 2)
- **Subnet:** 172.16.0.0/24 (static IP assignment)
- **Connection:** Ethernet cables between both machines and switch

### Machine 1 — Linux (172.16.0.200)
- **Nodes:** A, B, C, D, E (all C++)
- **Roles:** A (gateway), B (relay), C (worker), D (worker), E (relay)
- **Interface:** enp0s31f6

### Machine 2 — Mac (172.16.0.154)
- **Nodes:** F, G, H, I
- **Roles:** F (Python worker), G (C++ worker), H (Python worker), I (Python worker)

### Config File
- `configs/nodes_multi.json`

## Startup Commands

### Linux (172.16.0.200)
```bash
sudo ip addr add 172.16.0.200/24 dev enp0s31f6
sudo ip link set enp0s31f6 up
bash scripts/start_local.sh configs/nodes_multi.json 172.16.0.200
```

### Mac (172.16.0.154)
```bash
sudo ifconfig en5 172.16.0.154 netmask 255.255.255.0 up
bash scripts/start_local.sh configs/nodes_multi.json 172.16.0.154
```

### Run Client
```bash
./build/client -c configs/nodes_multi.json
```

## Results (chunk_size=100)

| Query | Type | Records | Time (ms) | Chunks |
|-------|------|---------|-----------|--------|
| q1 | PlateViolationHistory (plate=JER1863, state=NY) | 1 | 414,562 | 6 |
| q2 | PrecinctVehicleAnalysis (county=NY, year=2022-2023) | 1,013,134 | 511,307 | 10,134 |
| q3 | ViolationCodeDateRange (code=36, 2022-06-25 to 2022-07-10, cross-shard C+D) | 105,508 | 429,491 | 1,061 |
| q4 | ViolationCodeDateRange (code=36, 2022-07-01 to 2022-07-03, D-only) | 22,885 | 438,041 | 234 |

### Sample Output

**q1 - PlateViolationHistory:**
```
[1] plate=JER1863 date=07/18/2022 code=5 state=NY county=MN
```

**q2 - PrecinctVehicleAnalysis:**
```
[1] plate=22499MJ date=05/09/2019 body=VAN year=2022 county=NY
[2] plate=KTR2943 date=07/01/2022 body=SUBN year=2022 county=NY
[3] plate=KRR2099 date=07/04/2022 body=SUBN year=2022 county=NY
[4] plate=KVW9962 date=07/03/2022 body=SUBN year=2022 county=NY
[5] plate=KRV3643 date=07/03/2022 body=SDN year=2022 county=NY
```

**q3 - ViolationCodeDateRange (cross-shard):**
```
[1] plate=JFY6761 date=07/07/2022 county=QN make=INFIN fine=$0
[2] plate=KPZ2417 date=07/07/2022 county=MN make=HONDA fine=$0
[3] plate=GFN7546 date=07/07/2022 county=BK make=FORD fine=$0
[4] plate=KVD2614 date=07/07/2022 county=ST make=ME/BE fine=$0
[5] plate=HVC3378 date=07/07/2022 county=QN make=JEEP fine=$0
```

**q4 - ViolationCodeDateRange (D-only):**
```
[1] plate=L18NAP date=07/01/2022 county=BK make=JEEP fine=$0
[2] plate=KEE5315 date=07/01/2022 county=BK make=JEEP fine=$0
[3] plate=G27MWV date=07/01/2022 county=MN make=FORD fine=$0
[4] plate=HZ5115 date=07/01/2022 county=QN make=ME/BE fine=$0
[5] plate=HVD5826 date=07/01/2022 county=ST make=CHEVR fine=$0
```

## Comparison: Single-Host vs Multi-Host

| Query | Single-Host (Mac only) | Multi-Host | Difference |
|-------|------------------------|------------|------------|
| q1 | 382,996 ms | 414,562 ms | +8.2% |
| q2 | 408,061 ms | 511,307 ms | +25.3% |
| q3 | 422,174 ms | 429,491 ms | +1.7% |
| q4 | 443,486 ms | 438,041 ms | -1.2% |
| **Total** | **1,656,717 ms** | **1,793,401 ms** | **+8.3%** |

## Node Response Times (from A.log)

| Node | Machine | Language | q1 Response |
|------|---------|----------|-------------|
| G | Mac | C++ | 11 ms |
| C | Linux | C++ | 53,089 ms |
| D | Linux | C++ | 47,131 ms |
| H | Mac | Python | 414,520 ms |
| I | Mac | Python | 414,520 ms |
| B chain (via E->F) | Both | Mixed | 414,542 ms |

## Key Observations

1. C++ workers (C, D, G) are significantly faster than Python workers (F, H, I) when scanning the 63M row CSV.
2. Network overhead is minimal (~1ms ping latency between machines).
3. Python workers are the bottleneck — they take ~400s to scan the full CSV for date-range matching.
4. Multi-host adds ~8% overhead overall, mostly due to q2 which returns 10k+ chunks over the network.
5. gRPC deadline was increased to 1800s (from 600s) to accommodate Python processing time.

## Issues Encountered

1. Ethernet port flaky connection — Linux laptop's built-in NIC (enp0s31f6) had intermittent NO-CARRIER issues. Required multiple cable reconnections.
2. Static IP dropped on link flap — IP address needed reassignment after cable reconnection.
3. Original 600s deadline too short — Python workers exceeded the deadline when running on a slower machine. Fixed by increasing to 1800s.

## Data

- **Dataset:** combined_parking_violations.csv (63,103,919 rows)
- **Date:** 2026-05-07
