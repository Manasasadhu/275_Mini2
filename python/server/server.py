import grpc
from concurrent import futures
import parking_violation_query_pb2
import parking_violation_query_pb2_grpc
import json
import csv
from datetime import datetime
import argparse
import sys

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
        for neighbor in config.neighbors:
            port = config.get_neighbor_port(neighbor)
            neighbor_addr = f"localhost:{port}"
            try:
                channel = grpc.insecure_channel(neighbor_addr)
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
                
                fwd_response = self.neighbor_stubs[neighbor].ForwardQuery(fwd_request)
                for chunk in fwd_response.chunks:
                    response.chunks.append(chunk)
                print(f"[{self.config.node_id}] Got {len(fwd_response.chunks)} chunks from {neighbor}")
            except Exception as e:
                print(f"[{self.config.node_id}] Error forwarding to {neighbor}: {e}")
        
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
        
        # Forward to neighbors except the one that sent it
        print(f"[{self.config.node_id}] Forwarding query {req_id} to neighbors (excluding {from_node})")
        for neighbor in self.config.neighbors:
            if neighbor == from_node:
                print(f"[{self.config.node_id}] Skipping {neighbor} (query came from here)")
                continue
            
            try:
                fwd_response = self.neighbor_stubs[neighbor].ForwardQuery(request)
                for chunk in fwd_response.chunks:
                    response.chunks.append(chunk)
                print(f"[{self.config.node_id}] Got {len(fwd_response.chunks)} chunks from {neighbor}")
            except Exception as e:
                print(f"[{self.config.node_id}] Error forwarding to {neighbor}: {e}")

        # Process own shard if this node is a worker
        if self.config.shard:
            print(f"[{self.config.node_id}] Processing own shard")
            chunks = self.process_query_on_shard(request)
            response.chunks.extend(chunks)
            print(f"[{self.config.node_id}] Own shard returned {len(chunks)} chunks")

        return response

    def process_query_on_shard(self, request):
        """Process query on local shard"""
        chunks = []
        matched = 0
        current_chunk = parking_violation_query_pb2.Chunk(
            request_id=request.request_id, 
            is_last=False
        )
        print(f"[{self.config.node_id}] Processing shard: file={self.config.data_file}, shard={self.config.shard}, request_id={request.request_id}, chunk_size={request.chunk_size}")
        
        try:
            with open(self.config.data_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Check shard date range
                    issue_date_str = row[self.config.date_field]  # 'Issue Date'
                    try:
                        issue_date = datetime.strptime(issue_date_str, '%m/%d/%Y')
                    except:
                        continue
                    
                    shard_start = datetime.strptime(self.config.shard['start'], '%Y-%m-%d')
                    shard_end = datetime.strptime(self.config.shard['end'], '%Y-%m-%d')
                    
                    if not (shard_start <= issue_date <= shard_end):
                        continue
                    
                    # Check query type
                    if request.HasField('plate_id'):
                        if row.get('Plate ID') != request.plate_id:
                            continue
                    elif request.HasField('violation_code'):
                        try:
                            if int(row.get('Violation Code', 0)) != request.violation_code:
                                continue
                        except:
                            continue
                    elif request.HasField('plate_violation_history'):
                        q = request.plate_violation_history
                        if row.get('Plate ID') != q.plate_id:
                            continue
                        if q.violation_code != 0 and int(row.get('Violation Code', 0)) != q.violation_code:
                            continue
                        if q.registration_state and row.get('Registration State') != q.registration_state:
                            continue
                        # Date range already checked by shard
                    elif request.HasField('violation_code_date_range'):
                        q = request.violation_code_date_range
                        if int(row.get('Violation Code', 0)) != q.violation_code:
                            continue
                        # Date range already checked by shard
                    elif request.HasField('precinct_vehicle_analysis'):
                        q = request.precinct_vehicle_analysis
                        try:
                            vehicle_year = int(row.get('Vehicle Year', 0))
                            if row.get('Violation County') != q.county:
                                continue
                            if q.precinct != 0 and int(row.get('Violation Precinct', 0)) != q.precinct:
                                continue
                            if not (q.vehicle_year_min == 0 and q.vehicle_year_max == 9999) and \
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
        
        # Add final chunk
        if current_chunk.records:
            current_chunk.is_last = True
            chunks.append(current_chunk)
        elif not chunks:
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
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
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