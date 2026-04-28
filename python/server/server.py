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
        self.shard = node_data['shard']

    def get_neighbor_port(self, neighbor_id):
        port_map = {
            "A": 50051, "B": 50052, "C": 50053, "D": 50054, "E": 50055,
            "F": 50056, "G": 50057, "H": 50058, "I": 50059
        }
        return port_map.get(neighbor_id, 50051)


def _copy_query_to_forward(request, fwd_request):
    if request.HasField('plate_id'):
        fwd_request.plate_id = request.plate_id
    elif request.HasField('violation_code'):
        fwd_request.violation_code = request.violation_code
    elif request.HasField('issue_date'):
        fwd_request.issue_date.CopyFrom(request.issue_date)
    elif request.HasField('plate_violation_history'):
        fwd_request.plate_violation_history.CopyFrom(request.plate_violation_history)
    elif request.HasField('precinct_vehicle_analysis'):
        fwd_request.precinct_vehicle_analysis.CopyFrom(request.precinct_vehicle_analysis)
    elif request.HasField('unregistered_vehicle_lookup'):
        fwd_request.unregistered_vehicle_lookup.CopyFrom(request.unregistered_vehicle_lookup)


def _date_in_range(date_str, shard_start, shard_end):
    try:
        dt = datetime.strptime(date_str.strip(), '%m/%d/%Y')
        s = datetime.strptime(shard_start, '%Y-%m-%d')
        e = datetime.strptime(shard_end, '%Y-%m-%d')
        return s <= dt <= e
    except (ValueError, AttributeError):
        return False


def _matches_query(row, request, date_field):
    if request.HasField('plate_id'):
        return row.get('Plate ID', '').strip() == request.plate_id

    if request.HasField('violation_code'):
        try:
            return int(row.get('Violation Code', 0)) == request.violation_code
        except (ValueError, TypeError):
            return False

    if request.HasField('issue_date'):
        return True  # shard filter already applied

    if request.HasField('plate_violation_history'):
        q = request.plate_violation_history
        if row.get('Plate ID', '').strip() != q.plate_id:
            return False
        try:
            if int(row.get('Violation Code', 0)) != q.violation_code:
                return False
        except (ValueError, TypeError):
            return False
        if q.date_range.start or q.date_range.end:
            try:
                rd = datetime.strptime(row.get(date_field, '').strip(), '%m/%d/%Y')
                if q.date_range.start:
                    if rd < datetime.strptime(q.date_range.start, '%Y-%m-%d'):
                        return False
                if q.date_range.end:
                    if rd > datetime.strptime(q.date_range.end, '%Y-%m-%d'):
                        return False
            except (ValueError, AttributeError):
                return False
        return True

    if request.HasField('precinct_vehicle_analysis'):
        q = request.precinct_vehicle_analysis
        if row.get('Violation County', '').strip() != q.county:
            return False
        try:
            if int(row.get('Violation Precinct', 0)) != q.precinct:
                return False
        except (ValueError, TypeError):
            return False
        try:
            vy = int(row.get('Vehicle Year', 0) or 0)
            if vy < q.vehicle_year_min or vy > q.vehicle_year_max:
                return False
        except (ValueError, TypeError):
            return False
        if row.get('Vehicle Body Type', '').strip() != q.body_type:
            return False
        return True

    if request.HasField('unregistered_vehicle_lookup'):
        q = request.unregistered_vehicle_lookup
        if q.unregistered:
            if row.get('Unregistered Vehicle?', '').strip() not in ('Y', 'y'):
                return False
        if row.get('Registration State', '').strip() != q.state:
            return False
        try:
            ffc = int(row.get('Feet From Curb', 0) or 0)
            if ffc <= q.feet_from_curb_min:
                return False
        except (ValueError, TypeError):
            return False
        return True

    return True


class ParkingService(parking_violation_query_pb2_grpc.ParkingViolationServiceServicer):
    def __init__(self, config):
        self.config = config
        self.processed_requests = set()
        self.neighbor_stubs = {}

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

        print(f"[{self.config.node_id}] Gateway received query: request_id={request.request_id}")

        response = parking_violation_query_pb2.QueryResponse()

        for neighbor in self.config.neighbors:
            try:
                fwd_request = parking_violation_query_pb2.ForwardRequest(
                    request_id=request.request_id,
                    from_node=self.config.node_id,
                    chunk_size=request.chunk_size
                )
                _copy_query_to_forward(request, fwd_request)

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

        if req_id in self.processed_requests:
            print(f"[{self.config.node_id}] Dedup: already processed {req_id}")
            return parking_violation_query_pb2.ForwardResponse()
        self.processed_requests.add(req_id)

        response = parking_violation_query_pb2.ForwardResponse()

        # Forward to neighbors except sender
        for neighbor in self.config.neighbors:
            if neighbor == from_node:
                continue
            try:
                fwd_response = self.neighbor_stubs[neighbor].ForwardQuery(request)
                for chunk in fwd_response.chunks:
                    response.chunks.append(chunk)
                print(f"[{self.config.node_id}] Got {len(fwd_response.chunks)} chunks from {neighbor}")
            except Exception as e:
                print(f"[{self.config.node_id}] Error forwarding to {neighbor}: {e}")

        # Process own shard if worker
        if self.config.shard:
            print(f"[{self.config.node_id}] Processing own shard")
            chunks = self._process_query_on_shard(request)
            response.chunks.extend(chunks)
            print(f"[{self.config.node_id}] Own shard returned {len(chunks)} chunks")

        return response

    def _process_query_on_shard(self, request):
        chunks = []
        current_chunk = parking_violation_query_pb2.Chunk(
            request_id=request.request_id,
            is_last=False
        )

        shard_start = self.config.shard['start']
        shard_end = self.config.shard['end']

        try:
            with open(self.config.data_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    issue_date_str = row.get(self.config.date_field, '')
                    if not _date_in_range(issue_date_str, shard_start, shard_end):
                        continue

                    if not _matches_query(row, request, self.config.date_field):
                        continue

                    record = parking_violation_query_pb2.ViolationRecord()
                    record.summons_number = int(row.get('Summons Number', 0) or 0)
                    record.plate_id = row.get('Plate ID', '')
                    record.registration_state = row.get('Registration State', '')
                    record.plate_type = row.get('Plate Type', '')
                    record.issue_date = row.get('Issue Date', '')
                    record.violation_code = int(row.get('Violation Code', 0) or 0)
                    record.vehicle_body_type = row.get('Vehicle Body Type', '')
                    record.vehicle_make = row.get('Vehicle Make', '')
                    record.issuing_agency = row.get('Issuing Agency', '')
                    record.street_code1 = int(row.get('Street Code1', 0) or 0)
                    record.street_code2 = int(row.get('Street Code2', 0) or 0)
                    record.street_code3 = int(row.get('Street Code3', 0) or 0)
                    record.violation_location = row.get('Violation Location', '')
                    record.violation_precinct = int(row.get('Violation Precinct', 0) or 0)
                    record.issuer_precinct = int(row.get('Issuer Precinct', 0) or 0)
                    record.issuer_code = int(row.get('Issuer Code', 0) or 0)
                    record.violation_description = row.get('Violation Description', '')
                    record.violation_county = row.get('Violation County', '')
                    record.street_name = row.get('Street Name', '')
                    record.unregistered_vehicle = row.get('Unregistered Vehicle?', '').strip() in ('Y', 'y')
                    record.vehicle_year = int(row.get('Vehicle Year', 0) or 0)
                    record.feet_from_curb = int(row.get('Feet From Curb', 0) or 0)
                    record.vehicle_color = row.get('Vehicle Color', '')

                    current_chunk.records.append(record)

                    if len(current_chunk.records) >= request.chunk_size:
                        chunks.append(current_chunk)
                        current_chunk = parking_violation_query_pb2.Chunk(
                            request_id=request.request_id,
                            is_last=False
                        )
        except Exception as e:
            print(f"[{self.config.node_id}] Error processing shard: {e}", file=sys.stderr)

        if current_chunk.records:
            current_chunk.is_last = True
            chunks.append(current_chunk)

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
    print(f"[{node_id}] Role: {config.role}, Language: {config.language}")
    server.wait_for_termination()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--node', required=True)
    parser.add_argument('-c', '--config', required=True)
    args = parser.parse_args()
    serve(args.config, args.node)
