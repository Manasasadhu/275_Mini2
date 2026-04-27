import grpc
from concurrent import futures
import parking_violation_query_pb2
import parking_violation_query_pb2_grpc
import json
import csv
from datetime import datetime
import argparse

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

class ParkingService(parking_violation_query_pb2_grpc.ParkingViolationServiceServicer):
    def __init__(self, config):
        self.config = config
        self.processed_requests = set()  # for dedup

    def SubmitQuery(self, request, context):
        if not self.config.client_facing:
            context.abort(grpc.StatusCode.UNIMPLEMENTED, 'Not client-facing')
        # Aggregate from neighbors
        response = parking_violation_query_pb2.QueryResponse()
        # Pseudocode: chunks = self.forward_to_neighbors(request)
        # for chunk in chunks: response.chunks.append(chunk)
        return response

    def ForwardQuery(self, request, context):
        req_id = request.request_id
        if req_id in self.processed_requests:
            return parking_violation_query_pb2.ForwardResponse()  # dedup
        self.processed_requests.add(req_id)

        response = parking_violation_query_pb2.ForwardResponse()
        # Forward to neighbors except from_node
        for neighbor in self.config.neighbors:
            if neighbor == request.from_node:
                continue
            # Send to neighbor, collect chunks
            # response.chunks.extend(neighbor_response.chunks)

        # Process own shard if has data
        if self.config.shard:
            chunks = self.process_query_on_shard(request)
            response.chunks.extend(chunks)

        return response

    def process_query_on_shard(self, request):
        chunks = []
        current_chunk = parking_violation_query_pb2.Chunk(request_id=request.request_id, is_last=False)
        with open(self.config.data_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                issue_date = datetime.strptime(row['Issue Date'], '%m/%d/%Y')
                shard_start = datetime.strptime(self.config.shard['start'], '%Y-%m-%d')
                shard_end = datetime.strptime(self.config.shard['end'], '%Y-%m-%d')
                if not (shard_start <= issue_date <= shard_end):
                    continue
                # Check query match
                if request.HasField('plate_id') and row['Plate ID'] == request.plate_id:
                    # Add record
                    record = parking_violation_query_pb2.ViolationRecord()
                    # Fill record fields
                    current_chunk.records.append(record)
                    if len(current_chunk.records) >= request.chunk_size:
                        chunks.append(current_chunk)
                        current_chunk = parking_violation_query_pb2.Chunk(request_id=request.request_id, is_last=False)
        if current_chunk.records:
            current_chunk.is_last = True
            chunks.append(current_chunk)
        return chunks

    def CancelQuery(self, request, context):
        # Implement cancel
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
    print(f"Server started on {config.host}:{config.port}")
    server.wait_for_termination()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--node', required=True)
    parser.add_argument('-c', '--config', required=True)
    args = parser.parse_args()
    serve(args.config, args.node)