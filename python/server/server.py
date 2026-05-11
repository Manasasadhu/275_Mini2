import grpc
from concurrent import futures
import parking_violation_query_pb2
import parking_violation_query_pb2_grpc
import json
import csv
from datetime import datetime
import argparse
import sys
import time

class Config:
    def __init__(self, config_file, node_id):
        with open(config_file) as f:
            data = json.load(f)
        global_data = data['global']
        node_data = data['nodes'][node_id]
        self.node_id = node_data['node_id']
        self.host = node_data['host']
        self.port = node_data['port']
        self.neighbors = node_data['neighbors']
        self.language = node_data['language']
        self.role = node_data['role']
        self.client_facing = node_data['client_facing']
        self.chunk_size = node_data.get('chunk_size', global_data['default_chunk_size'])
        self.global_data_file = global_data['shared_data_file']
        self.date_field = global_data['date_field']
        if node_data['data_file'] is None:
            self.data_file = self.global_data_file
        else:
            self.data_file = node_data['data_file']
        self.shard = node_data['shard']  # dict or None
        if self.shard and self.shard.get('type') == 'county':
            self.shard_counties = set(self.shard.get('counties', []))
        else:
            self.shard_counties = set()

    def get_neighbor_port(self, neighbor_id):
        """Get port for a neighbor node"""
        port_map = {
            "A": 50051, "B": 50052, "C": 50053, "D": 50054, "E": 50055,
            "F": 50056, "G": 50057, "H": 50058, "I": 50059
        }
        return port_map.get(neighbor_id, 50051)

class ParkingService(parking_violation_query_pb2_grpc.ParkingViolationServiceServicer):
    def __init__(self, config):
        self.config = config
        self.processed_requests = set()  # for dedup
        self.neighbor_stubs = {}  # gRPC stubs to neighbors
        
        # Initialize gRPC stubs to all neighbors
        msg_options = [('grpc.max_receive_message_length', -1), ('grpc.max_send_message_length', -1)]
        for neighbor in config.neighbors:
            port = config.get_neighbor_port(neighbor)
            neighbor_addr = f"localhost:{port}"
            try:
                channel = grpc.insecure_channel(neighbor_addr, options=msg_options)
                self.neighbor_stubs[neighbor] = parking_violation_query_pb2_grpc.ParkingViolationServiceStub(channel)
                print(f"[{config.node_id}] Initialized neighbor {neighbor} at {neighbor_addr}")
            except Exception as e:
                print(f"[{config.node_id}] Warning: Could not initialize neighbor {neighbor}: {e}")

    def SubmitQuery(self, request, context):
        if not self.config.client_facing:
            context.abort(grpc.StatusCode.UNIMPLEMENTED, 'Not client-facing')
        
        query_type = request.WhichOneof('query') or 'unknown'
        print(f"[{self.config.node_id}] Gateway received query: request_id={request.request_id}, type={query_type}, chunk_size={request.chunk_size}")
        print(f"[{self.config.node_id}] Gateway neighbors={self.config.neighbors}")
        
        response = parking_violation_query_pb2.QueryResponse()
        
        # Forward to all neighbors
        for neighbor in self.config.neighbors:
            try:
                fwd_request = parking_violation_query_pb2.ForwardRequest(
                    request_id=request.request_id,
                    from_node=self.config.node_id,
                    chunk_size=request.chunk_size
                )

                # Copy the query type
                if request.HasField('plate_id'):
                    fwd_request.plate_id = request.plate_id
                elif request.HasField('violation_code'):
                    fwd_request.violation_code = request.violation_code
                elif request.HasField('issue_date'):
                    fwd_request.issue_date.CopyFrom(request.issue_date)
                elif request.HasField('plate_violation_history'):
                    fwd_request.plate_violation_history.CopyFrom(request.plate_violation_history)
                elif request.HasField('violation_code_date_range'):
                    fwd_request.violation_code_date_range.CopyFrom(request.violation_code_date_range)
                elif request.HasField('precinct_vehicle_analysis'):
                    fwd_request.precinct_vehicle_analysis.CopyFrom(request.precinct_vehicle_analysis)
                elif request.HasField('unregistered_vehicle_lookup'):
                    fwd_request.unregistered_vehicle_lookup.CopyFrom(request.unregistered_vehicle_lookup)

                rpc_start = time.time()
                fwd_response = self.neighbor_stubs[neighbor].ForwardQuery(fwd_request, timeout=120)
                rpc_end = time.time()
                rpc_ms = int((rpc_end - rpc_start) * 1000)
                for chunk in fwd_response.chunks:
                    response.chunks.append(chunk)
                print(f"[{self.config.node_id}] Latency to {neighbor}: {rpc_ms} ms, chunks={len(fwd_response.chunks)}")
            except Exception as e:
                rpc_end = time.time()
                rpc_ms = int((rpc_end - rpc_start) * 1000)
                print(f"[{self.config.node_id}] Error forwarding to {neighbor} (latency={rpc_ms} ms): {e}")
        
        return response

    def ForwardQuery(self, request, context):
        req_id = request.request_id
        from_node = request.from_node
        query_type = request.WhichOneof('query') or 'unknown'
        
        # Deduplication
        if req_id in self.processed_requests:
            print(f"[{self.config.node_id}] Dedup: already processed {req_id}")
            return parking_violation_query_pb2.ForwardResponse()
        self.processed_requests.add(req_id)

        print(f"[{self.config.node_id}] ForwardQuery received: request_id={req_id}, from_node={from_node}, type={query_type}")
        print(f"[{self.config.node_id}] Forwarding neighbors={self.config.neighbors}")

        response = parking_violation_query_pb2.ForwardResponse()
        
        # Forward to neighbors except the one that sent it (relay/gateway only)
        if self.config.role in ('relay', 'gateway'):
            for neighbor in self.config.neighbors:
                if neighbor == from_node:
                    print(f"[{self.config.node_id}] Skipping {neighbor} (query came from here)")
                    continue
                try:
                    rpc_start = time.time()
                    fwd_response = self.neighbor_stubs[neighbor].ForwardQuery(request, timeout=600)
                    rpc_end = time.time()
                    rpc_ms = int((rpc_end - rpc_start) * 1000)
                    response.chunks.extend(fwd_response.chunks)
                    print(f"[{self.config.node_id}] Latency to {neighbor}: {rpc_ms} ms, chunks={len(fwd_response.chunks)}")
                except Exception as e:
                    rpc_end = time.time()
                    rpc_ms = int((rpc_end - rpc_start) * 1000)
                    print(f"[{self.config.node_id}] Error forwarding to {neighbor} (latency={rpc_ms} ms): {e}")

        # Process own shard if this node is a worker
        if self.config.shard:
            print(f"[{self.config.node_id}] Processing own shard")
            chunks = self.process_query_on_shard(request)
            response.chunks.extend(chunks)
            print(f"[{self.config.node_id}] Own shard returned {len(chunks)} chunks")

        return response

    def process_query_on_shard(self, request):
        """Process query on local shard (supports both county and date-range sharding)"""
        chunks = []
        matched = 0
        current_chunk = parking_violation_query_pb2.Chunk(
            request_id=request.request_id,
            is_last=False
        )
        print(f"[{self.config.node_id}] Processing shard: file={self.config.data_file}, shard={self.config.shard}, request_id={request.request_id}, chunk_size={request.chunk_size}")

        known_counties = {
            "BX", "BRONX", "BK", "K", "BKLYN", "MN", "MAN", "NY",
            "QN", "Q", "QNS", "ST", "STATEN ISLAND", "SI"
        }

        # Determine shard type (default to date-based)
        shard_type = self.config.shard.get('type', 'issue_date_range') if self.config.shard else 'issue_date_range'

        # For county-based sharding
        is_catchall = False
        if shard_type == 'county_range':
            is_catchall = self.config.shard_counties == {"OTHER"}

        try:
            with open(self.config.data_file, 'r', encoding='latin-1') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Apply shard filtering based on shard type
                    if shard_type == 'county_range':
                        # County-based sharding
                        county = (row.get('Violation County') or '').strip()
                        if is_catchall:
                            if county in known_counties:
                                continue
                        elif county not in self.config.shard_counties:
                            continue
                    else:
                        # Date-based sharding (issue_date_range)
                        issue_date_str = row.get('Issue Date', '') or ''
                        if issue_date_str:
                            try:
                                shard_start = datetime.strptime(self.config.shard['start'], '%Y-%m-%d')
                                shard_end = datetime.strptime(self.config.shard['end'], '%Y-%m-%d')
                                issue_date = datetime.strptime(issue_date_str, '%m/%d/%Y')
                                if not (shard_start <= issue_date <= shard_end):
                                    continue
                            except:
                                continue

                    # Parse issue date once for any query type that needs it
                    issue_date = None
                    issue_date_str = row.get('Issue Date', '') or ''
                    try:
                        issue_date = datetime.strptime(issue_date_str, '%m/%d/%Y')
                    except Exception:
                        pass

                    # Query filter
                    if request.HasField('plate_id'):
                        if row.get('Plate ID') != request.plate_id:
                            continue
                    elif request.HasField('violation_code'):
                        try:
                            if int(row.get('Violation Code', 0)) != request.violation_code:
                                continue
                        except:
                            continue
                    elif request.HasField('issue_date'):
                        if issue_date is None:
                            continue
                        q_start = datetime.strptime(request.issue_date.start, '%Y-%m-%d')
                        q_end   = datetime.strptime(request.issue_date.end,   '%Y-%m-%d')
                        if not (q_start <= issue_date <= q_end):
                            continue
                    elif request.HasField('plate_violation_history'):
                        q = request.plate_violation_history
                        if row.get('Plate ID') != q.plate_id:
                            continue
                        if q.violation_code != 0 and int(row.get('Violation Code', 0)) != q.violation_code:
                            continue
                        if q.registration_state and row.get('Registration State') != q.registration_state:
                            continue
                        if q.date_range.start:
                            if issue_date is None:
                                continue
                            q_start = datetime.strptime(q.date_range.start, '%Y-%m-%d')
                            q_end   = datetime.strptime(q.date_range.end,   '%Y-%m-%d')
                            if not (q_start <= issue_date <= q_end):
                                continue
                    elif request.HasField('violation_code_date_range'):
                        q = request.violation_code_date_range
                        try:
                            if int(row.get('Violation Code', 0)) != q.violation_code:
                                continue
                        except:
                            continue
                        if q.date_range.start:
                            if issue_date is None:
                                continue
                            q_start = datetime.strptime(q.date_range.start, '%Y-%m-%d')
                            q_end   = datetime.strptime(q.date_range.end,   '%Y-%m-%d')
                            if not (q_start <= issue_date <= q_end):
                                continue
                    elif request.HasField('precinct_vehicle_analysis'):
                        q = request.precinct_vehicle_analysis
                        try:
                            vehicle_year = int(row.get('Vehicle Year', 0))
                            if row.get('Violation County') != q.county:
                                continue
                            if q.precinct != 0 and int(row.get('Violation Precinct', 0)) != q.precinct:
                                continue
                            if not (q.vehicle_year_min == 0 and q.vehicle_year_max == 0) and \
                               (vehicle_year < q.vehicle_year_min or vehicle_year > q.vehicle_year_max):
                                continue
                            if q.body_type and row.get('Vehicle Body Type') != q.body_type:
                                continue
                        except:
                            continue
                    elif request.HasField('unregistered_vehicle_lookup'):
                        q = request.unregistered_vehicle_lookup
                        try:
                            feet = int(row.get('Feet From Curb', 0))
                            unregistered = row.get('Unregistered Vehicle?', '').strip() == '1'
                            if (unregistered != q.unregistered or
                                row.get('Registration State') != q.state or
                                feet < q.feet_from_curb_min):
                                continue
                        except:
                            continue
                    
                    # Create record
                    record = parking_violation_query_pb2.ViolationRecord()
                    record.summons_number = int(row.get('Summons Number', '0') or 0)
                    record.plate_id = row.get('Plate ID', '')
                    record.registration_state = row.get('Registration State', '')
                    record.plate_type = row.get('Plate Type', '')
                    record.issue_date = row.get('Issue Date', '')
                    record.violation_code = int(row.get('Violation Code', '0') or 0)
                    record.vehicle_body_type = row.get('Vehicle Body Type', '')
                    record.vehicle_make = row.get('Vehicle Make', '')
                    record.issuing_agency = row.get('Issuing Agency', '')
                    record.violation_location = row.get('Violation Location', '')
                    record.violation_precinct = int(row.get('Violation Precinct', '0') or 0)
                    record.violation_description = row.get('Violation Description', '')
                    record.fine_amount = int(row.get('Fine Amount', '0') or 0)
                    record.precinct = row.get('Violation Precinct', '')
                    record.county = row.get('Violation County', '')
                    record.issuing_agency_name = row.get('Issuing Agency Name', '')
                    record.violation_status = row.get('Violation Status', '')
                    record.violation_county = row.get('Violation County', '')
                    record.unregistered_vehicle = row.get('Unregistered Vehicle?', '').strip() == '1'
                    record.vehicle_year = int(row.get('Vehicle Year', '0') or 0)
                    record.feet_from_curb = int(row.get('Feet From Curb', '0') or 0)
                    record.street_name = row.get('Street Name', '')
                    record.vehicle_color = row.get('Vehicle Color', '')
                    
                    current_chunk.records.append(record)
                    matched += 1
                    
                    if len(current_chunk.records) >= request.chunk_size:
                        chunks.append(current_chunk)
                        current_chunk = parking_violation_query_pb2.Chunk(
                            request_id=request.request_id, 
                            is_last=False
                        )
        except Exception as e:
            print(f"[{self.config.node_id}] Error processing shard: {e}", file=sys.stderr)
        
        if current_chunk.records or not chunks:
            current_chunk.is_last = True
            chunks.append(current_chunk)
        else:
            chunks[-1].is_last = True

        print(f"[{self.config.node_id}] Shard query complete: matched={matched}, chunks={len(chunks)}")
        return chunks

    def CancelQuery(self, request, context):
        return parking_violation_query_pb2.CancelResponse(success=True)

    def HealthCheck(self, request, context):
        return parking_violation_query_pb2.HealthResponse(healthy=True)

def serve(config_file, node_id):
    config = Config(config_file, node_id)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10),
                         options=[('grpc.max_receive_message_length', -1), ('grpc.max_send_message_length', -1)])
    parking_violation_query_pb2_grpc.add_ParkingViolationServiceServicer_to_server(
        ParkingService(config), server)
    server.add_insecure_port(f'{config.host}:{config.port}')
    server.start()
    print(f"[{node_id}] Server started on {config.host}:{config.port}")
    print(f"[{node_id}] Role: {config.role}, Language: {config.language}, neighbors={config.neighbors}")
    server.wait_for_termination()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--node', required=True)
    parser.add_argument('-c', '--config', required=True)
    args = parser.parse_args()
    serve(args.config, args.node)