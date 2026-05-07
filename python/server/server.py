import grpc
from concurrent import futures
import parking_violation_query_pb2
import parking_violation_query_pb2_grpc
import json
import csv
from collections import OrderedDict
from datetime import datetime
import argparse
import sys
import logging
import threading
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class Config:
    def __init__(self, config_file, node_id):
        import os
        with open(config_file) as f:
            data = json.load(f)
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(config_file)))
        global_data = data['global']
        node_data = data['nodes'][node_id]
        self.node_id = node_data['node_id']
        self.host = node_data['host']
        self.listen_host = node_data.get('listen_host', '0.0.0.0')
        self.port = node_data['port']
        self.neighbors = node_data['neighbors']
        self.language = node_data['language']
        self.role = node_data['role']
        self.client_facing = node_data['client_facing']
        self.chunk_size = node_data.get('chunk_size', global_data['default_chunk_size'])
        self.global_data_file = self._resolve_path(global_data['shared_data_file'], base_dir)
        self.date_field = global_data['date_field']
        if node_data['data_file'] is None:
            self.data_file = self.global_data_file
        else:
            self.data_file = self._resolve_path(node_data['data_file'], base_dir)
        self.shard = node_data['shard']  # dict or None
        self.neighbor_hosts = {nid: nd['host'] for nid, nd in data['nodes'].items()}
        self.neighbor_ports = {nid: nd['port'] for nid, nd in data['nodes'].items()}

    @staticmethod
    def _resolve_path(path, base_dir):
        import os
        if not path or os.path.isabs(path):
            return path
        return os.path.join(base_dir, path)

    def get_neighbor_addr(self, neighbor_id):
        host = self.neighbor_hosts.get(neighbor_id, 'localhost')
        port = self.neighbor_ports.get(neighbor_id, 50051)
        return f"{host}:{port}"

MAX_CACHE_ENTRIES = 64

def build_cache_key(request):
    query_type = request.WhichOneof('query')
    if query_type == 'plate_id':
        key = f"plate:{request.plate_id}"
    elif query_type == 'violation_code':
        key = f"vc:{request.violation_code}"
    elif query_type == 'issue_date':
        key = f"date:{request.issue_date.start}~{request.issue_date.end}"
    elif query_type == 'plate_violation_history':
        q = request.plate_violation_history
        key = f"pvh:{q.plate_id}:{q.violation_code}:{q.date_range.start}~{q.date_range.end}:{q.registration_state}"
    elif query_type == 'violation_code_date_range':
        q = request.violation_code_date_range
        key = f"vcdr:{q.violation_code}:{q.date_range.start}~{q.date_range.end}"
    elif query_type == 'precinct_vehicle_analysis':
        q = request.precinct_vehicle_analysis
        key = f"pva:{q.county}:{q.precinct}:{q.vehicle_year_min}~{q.vehicle_year_max}:{q.body_type}"
    elif query_type == 'unregistered_vehicle_lookup':
        q = request.unregistered_vehicle_lookup
        key = f"uvl:{int(q.unregistered)}:{q.state}:{q.feet_from_curb_min}"
    else:
        key = "unknown"
    key += f"|cs:{request.chunk_size}"
    return key


class LRUCache:
    def __init__(self, max_size=MAX_CACHE_ENTRIES):
        self.max_size = max_size
        self.cache = OrderedDict()
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return self.cache[key]
            return None

    def put(self, key, value):
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            else:
                if len(self.cache) >= self.max_size:
                    self.cache.popitem(last=False)
            self.cache[key] = value


class ParkingService(parking_violation_query_pb2_grpc.ParkingViolationServiceServicer):
    def __init__(self, config):
        self.config = config
        self.processed_requests = set()  # for dedup
        self.neighbor_stubs = {}  # gRPC stubs to neighbors
        self.cache = LRUCache()
        self.result_store = {}  # request_id -> list of chunks (for client pull)
        self.result_store_lock = threading.Lock()
        self.cancelled_requests = set()
        self.cancel_lock = threading.Lock()

        # Initialize gRPC stubs to all neighbors
        msg_options = [('grpc.max_receive_message_length', -1), ('grpc.max_send_message_length', -1)]
        for neighbor in config.neighbors:
            neighbor_addr = config.get_neighbor_addr(neighbor)
            try:
                channel = grpc.insecure_channel(neighbor_addr, options=msg_options)
                self.neighbor_stubs[neighbor] = parking_violation_query_pb2_grpc.ParkingViolationServiceStub(channel)
                logger.info(f"[{config.node_id}] Initialized neighbor {neighbor} at {neighbor_addr}")
            except Exception as e:
                logger.warning(f"[{config.node_id}] Could not initialize neighbor {neighbor}: {e}")

    def SubmitQuery(self, request, context):
        if not self.config.client_facing:
            context.abort(grpc.StatusCode.UNIMPLEMENTED, 'Not client-facing')

        query_type = request.WhichOneof('query') or 'unknown'
        logger.info(f"[{self.config.node_id}] Gateway received query: request_id={request.request_id}, type={query_type}, chunk_size={request.chunk_size}")
        logger.info(f"[{self.config.node_id}] Gateway neighbors={self.config.neighbors}")

        # Build forward request to compute cache key
        fwd_request = parking_violation_query_pb2.ForwardRequest(
            request_id=request.request_id,
            from_node=self.config.node_id,
            chunk_size=request.chunk_size
        )
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

        # Check gateway-level cache
        gateway_start = time.perf_counter()
        cache_key = build_cache_key(fwd_request)
        cached = self.cache.get(cache_key)
        if cached is not None:
            cache_us = int((time.perf_counter() - gateway_start) * 1_000_000)
            logger.info(f"[{self.config.node_id}] CACHE HIT at gateway for key={cache_key} (lookup_time={cache_us} us)")
            with self.result_store_lock:
                self.result_store[request.request_id] = cached
            response = parking_violation_query_pb2.QueryResponse()
            response.total_chunks = len(cached)
            response.request_id = request.request_id
            return response

        logger.info(f"[{self.config.node_id}] CACHE MISS at gateway, forwarding query")

        all_chunks = []

        # Forward to all neighbors in parallel
        def forward_to_neighbor(neighbor):
            try:
                fwd_start = time.perf_counter()
                fwd_response = self.neighbor_stubs[neighbor].ForwardQuery(fwd_request, timeout=120)
                fwd_ms = int((time.perf_counter() - fwd_start) * 1000)
                logger.info(f"[{self.config.node_id}] Got {len(fwd_response.chunks)} chunks from {neighbor} (rpc_time={fwd_ms} ms)")
                return list(fwd_response.chunks)
            except Exception as e:
                logger.error(f"[{self.config.node_id}] Error forwarding to {neighbor}: {e}")
                return []

        with futures.ThreadPoolExecutor(max_workers=len(self.config.neighbors)) as executor:
            futs = {executor.submit(forward_to_neighbor, n): n for n in self.config.neighbors}
            for fut in futures.as_completed(futs):
                all_chunks.extend(fut.result())

        # Store in gateway cache and result store
        self.cache.put(cache_key, all_chunks)
        with self.result_store_lock:
            self.result_store[request.request_id] = all_chunks

        gateway_ms = int((time.perf_counter() - gateway_start) * 1000)
        response = parking_violation_query_pb2.QueryResponse()
        response.total_chunks = len(all_chunks)
        response.request_id = request.request_id
        logger.info(f"[{self.config.node_id}] Gateway stored {len(all_chunks)} chunks for client pull (request_id={request.request_id}, total_time={gateway_ms} ms)")
        return response

    def ForwardQuery(self, request, context):
        req_id = request.request_id
        from_node = request.from_node
        query_type = request.WhichOneof('query') or 'unknown'

        # Check if cancelled
        with self.cancel_lock:
            if req_id in self.cancelled_requests:
                logger.info(f"[{self.config.node_id}] Query {req_id} is cancelled, aborting")
                context.abort(grpc.StatusCode.CANCELLED, "Request cancelled")

        # Deduplication
        if req_id in self.processed_requests:
            logger.info(f"[{self.config.node_id}] Dedup: already processed {req_id}")
            return parking_violation_query_pb2.ForwardResponse()
        self.processed_requests.add(req_id)

        node_start = time.perf_counter()
        logger.info(f"[{self.config.node_id}] ForwardQuery received: request_id={req_id}, from_node={from_node}, type={query_type}")

        # Check node-level cache
        cache_key = build_cache_key(request)
        cached = self.cache.get(cache_key)
        if cached is not None:
            cache_us = int((time.perf_counter() - node_start) * 1_000_000)
            logger.info(f"[{self.config.node_id}] CACHE HIT for key={cache_key} (lookup_time={cache_us} us)")
            response = parking_violation_query_pb2.ForwardResponse()
            response.chunks.extend(cached)
            return response

        response = parking_violation_query_pb2.ForwardResponse()

        # Forward to neighbors in parallel except the one that sent it (relay/gateway only)
        if self.config.role in ('relay', 'gateway'):
            downstream = [n for n in self.config.neighbors if n != from_node]
            for skipped in [n for n in self.config.neighbors if n == from_node]:
                logger.info(f"[{self.config.node_id}] Skipping {skipped} (query came from here)")

            def forward_downstream(neighbor):
                try:
                    fwd_start = time.perf_counter()
                    fwd_response = self.neighbor_stubs[neighbor].ForwardQuery(request, timeout=600)
                    fwd_ms = int((time.perf_counter() - fwd_start) * 1000)
                    logger.info(f"[{self.config.node_id}] Got {len(fwd_response.chunks)} chunks from {neighbor} (rpc_time={fwd_ms} ms)")
                    return list(fwd_response.chunks)
                except Exception as e:
                    logger.error(f"[{self.config.node_id}] Error forwarding to {neighbor}: {e}")
                    return []

            with futures.ThreadPoolExecutor(max_workers=len(downstream)) as executor:
                futs = {executor.submit(forward_downstream, n): n for n in downstream}
                for fut in futures.as_completed(futs):
                    response.chunks.extend(fut.result())

        # Process own shard if this node is a worker
        if self.config.shard:
            shard_start = time.perf_counter()
            logger.info(f"[{self.config.node_id}] Processing own shard")
            chunks = self.process_query_on_shard(request)
            shard_ms = int((time.perf_counter() - shard_start) * 1000)
            response.chunks.extend(chunks)
            logger.info(f"[{self.config.node_id}] Own shard returned {len(chunks)} chunks (scan_time={shard_ms} ms)")

        # Store in cache
        self.cache.put(cache_key, list(response.chunks))
        node_ms = int((time.perf_counter() - node_start) * 1000)
        logger.info(f"[{self.config.node_id}] Returning {len(response.chunks)} chunks for request {req_id} (node_time={node_ms} ms)")

        return response

    def process_query_on_shard(self, request):
        chunks = []
        matched = 0
        current_chunk = parking_violation_query_pb2.Chunk(
            request_id=request.request_id,
            is_last=False
        )
        logger.info(f"[{self.config.node_id}] Processing shard: file={self.config.data_file}, shard={self.config.shard}, request_id={request.request_id}, chunk_size={request.chunk_size}")

        shard_start = datetime.strptime(self.config.shard['start'], '%Y-%m-%d')
        shard_end = datetime.strptime(self.config.shard['end'], '%Y-%m-%d')

        q_date_start = q_date_end = None
        if request.HasField('issue_date'):
            q_date_start = datetime.strptime(request.issue_date.start, '%Y-%m-%d')
            q_date_end = datetime.strptime(request.issue_date.end, '%Y-%m-%d')
        elif request.HasField('plate_violation_history') and request.plate_violation_history.date_range.start:
            q_date_start = datetime.strptime(request.plate_violation_history.date_range.start, '%Y-%m-%d')
            q_date_end = datetime.strptime(request.plate_violation_history.date_range.end, '%Y-%m-%d')
        elif request.HasField('violation_code_date_range') and request.violation_code_date_range.date_range.start:
            q_date_start = datetime.strptime(request.violation_code_date_range.date_range.start, '%Y-%m-%d')
            q_date_end = datetime.strptime(request.violation_code_date_range.date_range.end, '%Y-%m-%d')

        try:
            with open(self.config.data_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    issue_date_str = row[self.config.date_field]
                    try:
                        issue_date = datetime.strptime(issue_date_str, '%m/%d/%Y')
                    except:
                        continue

                    if not (shard_start <= issue_date <= shard_end):
                        continue

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
                        if q_date_start and not (q_date_start <= issue_date <= q_date_end):
                            continue
                    elif request.HasField('plate_violation_history'):
                        q = request.plate_violation_history
                        if row.get('Plate ID') != q.plate_id:
                            continue
                        if q.violation_code != 0 and int(row.get('Violation Code', 0)) != q.violation_code:
                            continue
                        if q.registration_state and row.get('Registration State') != q.registration_state:
                            continue
                        if q_date_start and not (q_date_start <= issue_date <= q_date_end):
                            continue
                    elif request.HasField('violation_code_date_range'):
                        q = request.violation_code_date_range
                        try:
                            if int(row.get('Violation Code', 0)) != q.violation_code:
                                continue
                        except:
                            continue
                        if q_date_start and not (q_date_start <= issue_date <= q_date_end):
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
            logger.error(f"[{self.config.node_id}] Error processing shard: {e}")

        if current_chunk.records or not chunks:
            current_chunk.is_last = True
            chunks.append(current_chunk)
        else:
            chunks[-1].is_last = True

        logger.info(f"[{self.config.node_id}] Shard query complete: matched={matched}, chunks={len(chunks)}")
        return chunks

    def FetchChunks(self, request, context):
        req_id = request.request_id
        offset = request.offset
        limit = request.limit

        with self.result_store_lock:
            chunks = self.result_store.get(req_id)

        if chunks is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"No results for request_id={req_id}")

        total = len(chunks)
        end = min(offset + limit, total)
        response = parking_violation_query_pb2.FetchChunksResponse()
        response.total_chunks = total
        response.has_more = (end < total)
        for i in range(offset, end):
            response.chunks.append(chunks[i])

        logger.info(f"[{self.config.node_id}] FetchChunks: request_id={req_id} offset={offset} limit={limit} returned={end - offset} has_more={end < total}")
        return response

    def CancelQuery(self, request, context):
        req_id = request.request_id
        with self.cancel_lock:
            if req_id in self.cancelled_requests:
                return parking_violation_query_pb2.CancelResponse(success=True)
            self.cancelled_requests.add(req_id)

        logger.info(f"[{self.config.node_id}] CancelQuery: cancelling request {req_id}")

        # Remove from result store
        with self.result_store_lock:
            self.result_store.pop(req_id, None)

        # Propagate cancellation to all neighbors
        def propagate(neighbor):
            try:
                cancel_req = parking_violation_query_pb2.CancelRequest(request_id=req_id)
                self.neighbor_stubs[neighbor].CancelQuery(cancel_req, timeout=5)
            except Exception:
                pass

        for neighbor in self.config.neighbors:
            threading.Thread(target=propagate, args=(neighbor,), daemon=True).start()

        return parking_violation_query_pb2.CancelResponse(success=True)

    def HealthCheck(self, request, context):
        return parking_violation_query_pb2.HealthResponse(healthy=True)

def serve(config_file, node_id):
    config = Config(config_file, node_id)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10),
                         options=[('grpc.max_receive_message_length', -1), ('grpc.max_send_message_length', -1)])
    parking_violation_query_pb2_grpc.add_ParkingViolationServiceServicer_to_server(
        ParkingService(config), server)
    server.add_insecure_port(f'{config.listen_host}:{config.port}')
    server.start()
    logger.info(f"[{node_id}] Server started on {config.host}:{config.port}")
    logger.info(f"[{node_id}] Role: {config.role}, Language: {config.language}, neighbors={config.neighbors}")
    server.wait_for_termination()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--node', required=True)
    parser.add_argument('-c', '--config', required=True)
    args = parser.parse_args()
    serve(args.config, args.node)
