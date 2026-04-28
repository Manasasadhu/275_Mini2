# Complete Request Flow Through Classes

## Step-by-Step Execution for Query: plate_id="ABC123"

### STEP 1: CLIENT → GATEWAY (A)
```
CLIENT
  └─ Call: client.QueryByPlateId("req1", "ABC123", 100)
     └─ Creates QueryRequest protobuf:
        {
          request_id: "req1",
          plate_id: "ABC123",
          chunk_size: 100
        }
     └─ Makes gRPC call: stub_->SubmitQuery(&context, &request, &response)
        └─ Network: Connects to localhost:50051 (Node A)
           └─ gRPC serializes QueryRequest to binary
              └─ Sends over TCP/IP
```

**Classes Involved**:
- `ParkingClient` (client.cpp): Makes the RPC call
- Proto: `QueryRequest`, `QueryResponse`

---

### STEP 2: GATEWAY (A) RECEIVES & PROCESSES
```
NODE A (ParkingServiceImpl)
  └─ RPC Handler: SubmitQuery() is invoked by gRPC runtime
  
     A. ENTRY POINT
     ───────────────
     The gRPC server on A:50051 receives the binary data
       └─ gRPC deserializes to QueryRequest protobuf
          └─ Calls: ParkingServiceImpl::SubmitQuery(context, request, response)
     
     B. VALIDATE GATEWAY
     ──────────────────
     if (!config_.client_facing) {
         return UNIMPLEMENTED;  // Only A has client_facing=true
     }
     
     C. EXTRACT QUERY
     ────────────────
     req_id = "req1"
     plate_id = "ABC123"  (from request.plate_id())
     
     Log: "[A] Gateway received query: request_id=req1, plate_id=ABC123"
     
     D. CONVERT TO FORWARD REQUEST
     ─────────────────────────────
     Create ForwardRequest:
     {
       request_id: "req1",
       from_node: "A",  // Track who sent it (prevent backtracking)
       plate_id: "ABC123",
       chunk_size: 100
     }
     
     E. FORWARD TO NEIGHBORS [gRPC CALLS]
     ────────────────────────────────────
     Loop through config_.neighbors = ["B", "G", "H", "I"]
     
       FOR neighbor "B":
       ─────────────────
       grpc::ClientContext ctx;
       parkingviolation::ForwardResponse fwd_response;
       
       // THIS IS WHERE gRPC CLIENT CALL HAPPENS
       grpc::Status status = neighbor_stubs_["B"]->ForwardQuery(&ctx, forward_req, &fwd_response);
       
       Network: Connects to localhost:50052 (Node B)
         └─ gRPC serializes ForwardRequest to binary
            └─ Sends over TCP/IP
            └─ Waits for ForwardResponse
       
       When response arrives:
         ├─ gRPC deserializes to ForwardResponse
         ├─ Extract chunks from fwd_response.chunks()
         └─ Add each chunk to response.add_chunks()
       
       FOR neighbor "G": (same as B, but to localhost:50057)
       FOR neighbor "H": (same as B, but to localhost:50058)
       FOR neighbor "I": (same as B, but to localhost:50059)
     
     F. AGGREGATE RESULTS
     ────────────────────
     response now contains:
     {
       chunks: [
         <chunks from B>,
         <chunks from G>,
         <chunks from H>,
         <chunks from I>
       ]
     }
     
     Log: "[A] Gateway response contains X total chunks"
     
     G. RETURN TO CLIENT
     ──────────────────
     return grpc::Status::OK;
     
     gRPC serializes QueryResponse to binary
     Sends back over TCP to client
     
Classes Involved:
- ParkingServiceImpl: SubmitQuery() handler
- neighbor_stubs_: gRPC client proxies
- grpc::ClientContext: RPC context
- Proto: ForwardRequest, ForwardResponse
```

---

### STEP 3: NODE B (RELAY) RECEIVES & PROCESSES
```
NODE B (ParkingServiceImpl)
  └─ RPC Handler: ForwardQuery() is invoked by gRPC runtime
  
     A. ENTRY POINT
     ───────────────
     Node B:50052 receives binary ForwardRequest
       └─ gRPC deserializes to ForwardRequest
          └─ Calls: ParkingServiceImpl::ForwardQuery(context, request, response)
     
     B. EXTRACT REQUEST
     ──────────────────
     req_id = "req1"
     from_node = "A"  (track where it came from!)
     plate_id = "ABC123"
     
     C. DEDUPLICATION CHECK
     ─────────────────────
     if (processed_requests_.count("req1")) {
         return OK;  // Already processed this, skip
     }
     processed_requests_.insert("req1");
     
     D. CHECK IF WORKER
     ──────────────────
     if (config_.shard && !data_file.empty()) {
         // B has NO shard (role="relay"), so SKIP THIS
     }
     
     all_chunks is empty so far
     
     E. CHECK IF RELAY/GATEWAY
     ────────────────────────
     if (config_.role == "relay" || config_.role == "gateway") {
         // YES! B is a relay, so forward downstream
     }
     
     F. FORWARD TO DOWNSTREAM NEIGHBORS [gRPC CALLS]
     ────────────────────────────────────────────────
     Loop through config_.neighbors = ["A", "C", "D", "E"]
     
       Check: neighbor == from_node?
              "A" == "A" → YES! SKIP (don't send back to A)
       
       Check: neighbor == from_node?
              "C" != "A" → NO, forward!
       
       grpc::ClientContext ctx;
       parkingviolation::ForwardResponse fwd_response;
       
       // THIS IS WHERE gRPC CLIENT CALL HAPPENS (B calling C)
       grpc::Status status = neighbor_stubs_["C"]->ForwardQuery(&ctx, *request, &fwd_response);
       
       Network: Connects to localhost:50053 (Node C)
         └─ gRPC serializes ForwardRequest to binary
            └─ Sends over TCP/IP
            └─ Waits for ForwardResponse (C processes and returns)
       
       When response arrives:
         ├─ C returns chunks from its shard (2022-01-01 to 2022-06-30)
         └─ B adds C's chunks to all_chunks
       
       Check: neighbor == from_node?
              "D" != "A" → NO, forward!
       
       grpc::Status status = neighbor_stubs_["D"]->ForwardQuery(&ctx, *request, &fwd_response);
         └─ Node D processes and returns chunks
       
       Check: neighbor == from_node?
              "E" != "A" → NO, forward!
       
       grpc::Status status = neighbor_stubs_["E"]->ForwardQuery(&ctx, *request, &fwd_response);
         └─ Node E (relay) forwards to F, D, G
     
     G. COLLECTED ALL CHUNKS
     ───────────────────────
     all_chunks now contains:
     {
       <chunks from C>,
       <chunks from D>,
       <chunks from E (which aggregated F, D, G)>
     }
     
     H. RETURN TO A
     ──────────────
     for (const auto& chunk : all_chunks) {
         response->add_chunks()->CopyFrom(chunk);
     }
     return grpc::Status::OK;
     
     gRPC serializes ForwardResponse
     Sends back to Node A
     
Classes Involved:
- ParkingServiceImpl: ForwardQuery() handler
- neighbor_stubs_: gRPC client proxies to C, D, E
- Proto: ForwardRequest, ForwardResponse
```

---

### STEP 4: NODE C (WORKER) RECEIVES & PROCESSES
```
NODE C (ParkingServiceImpl)
  └─ RPC Handler: ForwardQuery() is invoked by gRPC runtime
  
     A. ENTRY POINT
     ───────────────
     Node C:50053 receives binary ForwardRequest
       └─ gRPC deserializes to ForwardRequest
          └─ Calls: ParkingServiceImpl::ForwardQuery(context, request, response)
     
     B. EXTRACT REQUEST
     ──────────────────
     req_id = "req1"
     from_node = "B"
     plate_id = "ABC123"
     
     C. DEDUPLICATION CHECK
     ─────────────────────
     if (processed_requests_.count("req1")) {
         return OK;  // Skip if already processed
     }
     processed_requests_.insert("req1");
     
     D. CHECK IF WORKER
     ──────────────────
     if (config_.shard && !data_file.empty()) {
         // YES! C is a worker
         
         config_.shard = {
           type: "issue_date_range",
           start: "2022-01-01",
           end: "2022-06-30"
         }
         
         data_file = "/Users/aravindreddy/Downloads/.../parking_violations_combined.csv"
         
         ════════════════════════════════════════════════════════════════════════════════
         ╔═══════════════════════════════════════════════════════════════════════════════╗
         ║  THIS IS WHERE SHARDING HAPPENS! (sharding.hpp is called here)              ║
         ╚═══════════════════════════════════════════════════════════════════════════════╝
         
         auto chunks = process_query_on_shard(
             "/path/to/parking_violations_combined.csv",
             "2022-01-01",           // C's shard start
             "2022-06-30",           // C's shard end
             "ABC123",               // plate_id filter
             100,                    // chunk_size
             "req1"                  // request_id
         );
         
         Inside process_query_on_shard():
         ──────────────────────────────
         1. Open CSV file
         2. Skip header
         3. Loop through all 11GB of rows:
            FOR each row in CSV:
              a. Extract "Issue Date" column
              b. Call: date_in_range(issue_date, "2022-01-01", "2022-06-30")
                 └─ Convert: "03/15/2022" → 20220315
                 └─ Compare: 20220101 ≤ 20220315 ≤ 20220630 ? YES
                 └─ Row is in C's shard! Process it
              c. Call: parse_csv_line(row) → ViolationRecord
              d. Extract: record.plate_id() = "ABC123"
              e. Check: "ABC123" == "ABC123" ? YES
              f. Add to current chunk: current_chunk.add_records(record)
              g. Check: chunk_size >= 100?
                 └─ If YES: yield chunk with is_last=false, start new chunk
              h. Continue next row
            
            FOR next row:
              i. Extract Issue Date: "05/22/2022"
              j. In range? 20220522 in [20220101, 20220630] ? YES
              k. parse_csv_line() → ViolationRecord
              l. Plate: "ABC123" == "ABC123" ? YES
              m. Add to chunk...
              ... (repeat for all matching rows)
         
         4. At EOF:
            Mark final chunk: is_last=true
            Return all chunks
         
         Result: all_chunks = [
           Chunk {request_id: "req1", records: [rec1, rec2, ...], is_last: false},
           Chunk {request_id: "req1", records: [rec101, rec102, ...], is_last: false},
           Chunk {request_id: "req1", records: [rec201, ...], is_last: true}
         ]
     }
     
     E. CHECK IF RELAY
     ────────────────
     if (config_.role == "relay" || config_.role == "gateway") {
         // NO! C is a worker, so SKIP forwarding
     }
     
     F. RETURN TO B
     ──────────────
     for (const auto& chunk : all_chunks) {
         response->add_chunks()->CopyFrom(chunk);
     }
     return grpc::Status::OK;
     
     gRPC serializes ForwardResponse containing all chunks
     Sends back to Node B
     
Classes Involved:
- ParkingServiceImpl: ForwardQuery() handler
- sharding.hpp: process_query_on_shard(), date_in_range(), parse_csv_line()
- Proto: ForwardRequest, ForwardResponse, Chunk, ViolationRecord
```

---

### STEP 5: AGGREGATION BACK UP THE TREE

```
Responses flow back up:

C returns chunks to B
  ├─ B receives C's chunks
  ├─ B aggregates C's chunks + D's chunks + E's results
  └─ B returns aggregated result to A

D returns chunks to B (same path)

E returns chunks to B (same path)
  ├─ E had forwarded to F, D, G
  ├─ F returns chunks to E
  ├─ D returns chunks to E (but D also got from B - dedup!)
  ├─ G returns chunks to E
  ├─ E aggregates all from F,D,G
  └─ E returns aggregated to B (which already got D)

G returns chunks to A (different path)

H returns chunks to A (different path)

I returns chunks to A (different path)

A (Gateway) aggregates all:
  ├─ B's aggregated chunks
  ├─ G's chunks
  ├─ H's chunks
  └─ I's chunks

A returns final response to CLIENT
```

---

### STEP 6: CLIENT RECEIVES RESPONSE

```
CLIENT (client.cpp)
  └─ gRPC response from A:50051 arrives
     └─ QueryResponse deserialized
        {
          chunks: [
            Chunk {records: [rec1, rec2, ...], is_last: false},
            Chunk {records: [rec101, rec102, ...], is_last: false},
            Chunk {records: [rec201, ...], is_last: true},
            ... (from all workers)
          ]
        }
     
     └─ Loop through response.chunks():
        FOR chunk in response.chunks():
          FOR record in chunk.records():
            Print: "Plate: ABC123, Date: 03/15/2022, Code: 36"
        
        total_records += chunk.records_size()
     
     Print: "Total records: X"
```

---

## Complete Message Flow Diagram

```
┌──────────┐
│  CLIENT  │
└────┬─────┘
     │
     │ gRPC QueryRequest
     │ (binary serialized by protobuf)
     ↓
┌─────────────────────────────────────────────────────────────┐
│ NODE A (Gateway) - SubmitQuery()                           │
│ • Deserialize QueryRequest                                 │
│ • Validate client_facing=true                              │
│ • Create ForwardRequest                                     │
│ • Make gRPC calls to neighbors:                             │
└─────────────────────────────────────────────────────────────┘
     │
     ├─ gRPC ForwardRequest → localhost:50052 (B)
     ├─ gRPC ForwardRequest → localhost:50057 (G)
     ├─ gRPC ForwardRequest → localhost:50058 (H)
     └─ gRPC ForwardRequest → localhost:50059 (I)
     │
     ├─ ┌────────────────────────────────────────────────────┐
     │  │ NODE B (Relay) - ForwardQuery()                   │
     │  │ • Deserialize ForwardRequest                       │
     │  │ • Skip self (from_node="A")                        │
     │  │ • Make gRPC calls to downstream:                   │
     │  └────────────────────────────────────────────────────┘
     │     │
     │     ├─ gRPC → localhost:50053 (C)
     │     │  │
     │     │  └─ ┌────────────────────────────────────────┐
     │     │     │ NODE C (Worker) - ForwardQuery()       │
     │     │     │ • Deserialize ForwardRequest           │
     │     │     │ • Has shard: process_query_on_shard()  │
     │     │     │   - Open CSV                            │
     │     │     │   - Filter by date: 2022-01 to 2022-06│
     │     │     │   - Filter by plate_id: ABC123         │
     │     │     │   - Return chunks                       │
     │     │     └────────────────────────────────────────┘
     │     │     gRPC ForwardResponse (chunks) ← returns to B
     │     │
     │     ├─ gRPC → localhost:50054 (D) [similar to C]
     │     │
     │     └─ gRPC → localhost:50055 (E)
     │        │
     │        └─ ┌──────────────────────────────────────┐
     │           │ NODE E (Relay) - ForwardQuery()      │
     │           │ • Make gRPC calls to:                │
     │           │   - F (gRPC)                         │
     │           │   - D (gRPC, but skip B as sender)   │
     │           │   - G (gRPC)                         │
     │           └──────────────────────────────────────┘
     │           gRPC ForwardResponse (aggregated) ← returns to B
     │
     │  gRPC ForwardResponse with all chunks ← B returns to A
     │
     ├─ gRPC → localhost:50057 (G)
     │  └─ Returns chunks directly to A
     │
     ├─ gRPC → localhost:50058 (H)
     │  └─ Returns chunks directly to A
     │
     └─ gRPC → localhost:50059 (I)
        └─ Returns chunks directly to A
     
     ↓
┌─────────────────────────────────────────┐
│ NODE A aggregates all chunks from      │
│ B, G, H, I                             │
│ Returns QueryResponse to CLIENT         │
└─────────────────────────────────────────┘
     │
     │ gRPC QueryResponse (binary serialized)
     ↓
┌──────────────────────────────────────┐
│ CLIENT receives response              │
│ • Deserialize QueryResponse           │
│ • Loop through chunks                 │
│ • Print results                       │
└──────────────────────────────────────┘
```

---

## Class Interactions Summary

| Step | Class | Method | gRPC Call | To |
|------|-------|--------|-----------|-----|
| 1 | ParkingClient | QueryByPlateId() | SubmitQuery | A:50051 |
| 2 | ParkingServiceImpl | SubmitQuery() | ForwardQuery | B,G,H,I |
| 3 | ParkingServiceImpl | ForwardQuery() | ForwardQuery | C,D,E |
| 4 | ParkingServiceImpl | ForwardQuery() | (none) | Process locally |
| 4 | sharding.hpp | process_query_on_shard() | (none) | Scan CSV |
| 5 | sharding.hpp | date_in_range() | (none) | Check shard |
| 5 | sharding.hpp | parse_csv_line() | (none) | Parse row |
| Response | — | — | ForwardResponse | Back up tree |

---

## Key Observations

1. **Same class, different behavior**: ParkingServiceImpl runs on all 9 nodes but behaves differently:
   - Gateway (A): Creates initial ForwardRequest, aggregates from neighbors
   - Relay (B, E): Forwards to downstream, aggregates results
   - Worker (C, D, F, G, H, I): Processes shard, returns chunks

2. **gRPC is the communication**: Every forward happens via gRPC calls to neighbor stubs

3. **Sharding only at workers**: Only worker nodes call process_query_on_shard()

4. **Deduplication prevents double-processing**: D and G don't process same query twice

5. **Aggregation happens at each level**: B aggregates from C,D,E; then A aggregates from B,G,H,I
