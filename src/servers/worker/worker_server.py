"""
Python gRPC Worker Server — Process I
Receives DelegateQuery from parent (Process A), returns empty (owns no counties).
"""

import sys
import os
import json
import time
import logging
import threading
import grpc
from concurrent import futures
from datetime import datetime

# ---------------------------------------------------------------------------
# Proto imports — generated stubs must exist before running this script.
# Run generate_proto.sh first.
# ---------------------------------------------------------------------------
PROTO_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "proto", "generated", "python")
sys.path.insert(0, PROTO_DIR)

import parking_violation_query_pb2       as pb2
import parking_violation_query_pb2_grpc  as pb2_grpc

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("WorkerI")

os.makedirs("logs", exist_ok=True)
_log_fh = None  # opened after config is loaded


def _log(event: str, req_id: str = "-", pending: int = 0,
         chunk: int = -1, records: int = -1, extra: str = "") -> None:
    ts = datetime.now().strftime("%H:%M:%S.") + f"{datetime.now().microsecond // 1000:03d}"
    line = (f"{ts} [I] {event:<22} req={req_id} pending={pending}"
            f" chunk={chunk} records={records}")
    if extra:
        line += f" {extra}"
    logger.info(line)
    if _log_fh:
        _log_fh.write(line + "\n")
        _log_fh.flush()


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------
def load_config(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Service implementation
# ---------------------------------------------------------------------------
class WorkerServiceImpl(pb2_grpc.ParkingViolationServiceServicer):

    def __init__(self, config: dict):
        self.config = config
        self.process_id = config["process_id"]
        self.owned_counties: list[str] = config["data_partitioning"]["owned_counties"]
        self._pending = 0
        self._pending_lock = threading.Lock()

        logger.info(f"[Worker {self.process_id}] starting (team={config['team']})")
        logger.info(f"[Worker {self.process_id}] listening on "
                    f"{config['listen_host']}:{config['listen_port']}")
        logger.info(f"[Worker {self.process_id}] owned counties: "
                    f"{self.owned_counties if self.owned_counties else '(none)'}")
        _log("INIT", extra="process started")

    # ------------------------------------------------------------------
    def DelegateQuery(self, request, context):
        with self._pending_lock:
            self._pending += 1
            pending = self._pending

        req_id = request.request_id
        delegator = request.delegating_process
        logger.info(f"\n[Worker {self.process_id}] delegation {req_id} from {delegator}")
        _log("RECV_DELEGATION", req_id, pending, extra=delegator)

        # ---- deserialize original QueryRequest ----
        query = pb2.QueryRequest()
        if not query.ParseFromString(request.original_query):
            with self._pending_lock:
                self._pending -= 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Failed to parse original_query")
            return

        # ---- determine counties to serve ----
        counties = self._select_counties(query)
        if not counties:
            logger.info(f"[Worker {self.process_id}] no matching counties — returning empty")
            _log("DONE", req_id, pending, chunk=0, records=0)
            with self._pending_lock:
                self._pending -= 1
            return  # yields nothing — empty stream

        # ---- Process I owns no counties in this topology, but the logic
        #      below would work for any worker that actually owns data. ----
        chunk_size = self.config["chunk_config"]["default_chunk_size"]
        records = self._load_data(query, counties)
        logger.info(f"[Worker {self.process_id}] loaded {len(records)} records")
        _log("LOADED", req_id, pending, records=len(records))

        chunk_num = 0
        for i in range(0, len(records), chunk_size):
            if context.is_active() is False:
                _log("CANCELLED", req_id, pending, chunk=chunk_num)
                with self._pending_lock:
                    self._pending -= 1
                return

            batch = records[i:i + chunk_size]
            resp = pb2.DelegationResponse()
            resp.request_id = req_id
            resp.chunk_number = chunk_num
            resp.is_final = False
            resp.responding_process = self.process_id
            for rec in batch:
                resp.records.append(rec)

            yield resp
            _log("CHUNK_SENT", req_id, pending, chunk=chunk_num, records=len(batch))
            logger.info(f"[Worker {self.process_id}] sent chunk {chunk_num} ({len(batch)} records)")
            chunk_num += 1

        _log("DONE", req_id, pending, chunk=chunk_num, records=len(records))
        with self._pending_lock:
            self._pending -= 1

    # ------------------------------------------------------------------
    def HealthCheck(self, request, context):
        with self._pending_lock:
            p = self._pending
        resp = pb2.HealthResponse()
        resp.responding_process = self.process_id
        resp.is_healthy = True
        resp.pending_requests = p
        resp.active_workers = 1
        return resp

    # ------------------------------------------------------------------
    def QueryViolations(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Workers do not accept direct queries")
        return iter([])

    # ------------------------------------------------------------------
    def CancelQuery(self, request, context):
        resp = pb2.CancelResponse()
        resp.request_id = request.request_id
        resp.cancelled = True
        resp.message = "cancel acknowledged"
        return resp

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _select_counties(self, query: pb2.QueryRequest) -> list[str]:
        if not self.owned_counties:
            return []
        qc = query.violation_county
        if not qc:
            return list(self.owned_counties)
        if qc in self.owned_counties:
            return [qc]
        return []

    def _load_data(self, query: pb2.QueryRequest,
                   counties: list[str]) -> list[pb2.ViolationRecord]:
        """
        Load matching records from the CSV.
        Returns a list of ViolationRecord proto messages.
        This is a simple sequential scan — suitable for a single-host dev setup.
        """
        data_path = self.config.get("data_path", "")
        if not data_path or not os.path.exists(data_path):
            logger.warning(f"[Worker {self.process_id}] data_path not found: {data_path}")
            return []

        county_set = set(counties)
        vc_filter  = str(query.violation_code) if query.violation_code else ""
        date_start = _norm_date(query.issue_date_start)
        date_end   = _norm_date(query.issue_date_end)
        max_rec    = query.max_records if query.max_records > 0 else 10_000_000

        results: list[pb2.ViolationRecord] = []

        with open(data_path, encoding="utf-8", errors="replace") as fh:
            header = fh.readline()  # skip header
            for raw_line in fh:
                if len(results) >= max_rec:
                    break
                cols = _split_csv(raw_line.rstrip("\n"))
                if len(cols) < 22:
                    continue

                county = cols[21].strip()
                if county not in county_set:
                    continue
                if vc_filter and cols[5].strip() != vc_filter:
                    continue

                raw_date = cols[4].strip()
                norm_date = _norm_date(raw_date)
                if date_start and norm_date < date_start:
                    continue
                if date_end and norm_date > date_end:
                    continue

                rec = pb2.ViolationRecord()
                rec.summons_number            = _safe_int64(cols[0])
                rec.plate_id                  = cols[1].strip()
                rec.registration_state        = cols[2].strip()
                rec.plate_type                = cols[3].strip()
                rec.issue_date                = raw_date
                rec.violation_code            = _safe_int32(cols[5])
                rec.vehicle_body_type         = cols[6].strip()
                rec.vehicle_make              = cols[7].strip()
                rec.issuing_agency            = cols[8].strip()
                rec.street_code1              = _safe_int32(cols[9])
                rec.street_code2              = _safe_int32(cols[10])
                rec.street_code3              = _safe_int32(cols[11])
                rec.vehicle_expiration_date   = _safe_int32(cols[12])
                rec.violation_location        = cols[13].strip()
                rec.violation_precinct        = _safe_int32(cols[14])
                rec.issuer_precinct           = _safe_int32(cols[15])
                rec.issuer_code               = _safe_int64(cols[16])
                rec.issuer_command            = cols[17].strip()
                rec.issuer_squad              = cols[18].strip()
                rec.violation_time            = cols[19].strip()
                rec.time_first_observed       = cols[20].strip()
                rec.violation_county          = county
                rec.violation_front_opposite  = cols[22].strip() if len(cols) > 22 else ""
                rec.house_number              = cols[23].strip() if len(cols) > 23 else ""
                rec.street_name               = cols[24].strip() if len(cols) > 24 else ""
                rec.intersecting_street       = cols[25].strip() if len(cols) > 25 else ""
                rec.date_first_observed       = _safe_int32(cols[26]) if len(cols) > 26 else 0
                rec.law_section               = _safe_int32(cols[27]) if len(cols) > 27 else 0
                rec.sub_division              = cols[28].strip() if len(cols) > 28 else ""
                rec.violation_legal_code      = cols[29].strip() if len(cols) > 29 else ""
                rec.days_parking_in_effect    = cols[30].strip() if len(cols) > 30 else ""
                rec.from_hours_in_effect      = cols[31].strip() if len(cols) > 31 else ""
                rec.to_hours_in_effect        = cols[32].strip() if len(cols) > 32 else ""
                rec.vehicle_color             = cols[33].strip() if len(cols) > 33 else ""
                rec.unregistered_vehicle      = _safe_bool(cols[34]) if len(cols) > 34 else False
                rec.vehicle_year              = _safe_int32(cols[35]) if len(cols) > 35 else 0
                rec.meter_number              = cols[36].strip() if len(cols) > 36 else ""
                rec.feet_from_curb            = _safe_int32(cols[37]) if len(cols) > 37 else 0
                rec.violation_post_code       = cols[38].strip() if len(cols) > 38 else ""
                rec.violation_description     = cols[39].strip() if len(cols) > 39 else ""
                rec.no_standing_violation     = cols[40].strip() if len(cols) > 40 else ""
                rec.hydrant_violation         = cols[41].strip() if len(cols) > 41 else ""
                rec.double_parking_violation  = cols[42].strip() if len(cols) > 42 else ""
                results.append(rec)

        return results


# ---------------------------------------------------------------------------
# CSV / type helpers
# ---------------------------------------------------------------------------
def _split_csv(line: str) -> list[str]:
    cols, cur, in_q = [], [], False
    for ch in line:
        if ch == '"':
            in_q = not in_q
        elif ch == ',' and not in_q:
            cols.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    cols.append("".join(cur))
    return cols


def _norm_date(d: str) -> str:
    """MM/DD/YYYY → YYYY/MM/DD, or '' if unparseable."""
    d = d.strip()
    if not d:
        return ""
    parts = d.split("/")
    if len(parts) == 3:
        return f"{parts[2]}/{parts[0]}/{parts[1]}"
    return d


def _safe_int64(s: str) -> int:
    try:
        return int(s.strip())
    except (ValueError, AttributeError):
        return 0


def _safe_int32(s: str) -> int:
    try:
        return int(s.strip())
    except (ValueError, AttributeError):
        return 0


def _safe_bool(s: str) -> bool:
    return s.strip().lower() in ("1", "true", "yes", "y")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def run(config_file: str) -> None:
    global _log_fh

    config = load_config(config_file)
    pid  = config["process_id"]
    host = config["listen_host"]
    port = config["listen_port"]

    os.makedirs("logs", exist_ok=True)
    _log_fh = open(f"logs/{pid}_worker.log", "a")

    servicer = WorkerServiceImpl(config)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ParkingViolationServiceServicer_to_server(servicer, server)
    address = f"{host}:{port}"
    server.add_insecure_port(address)
    server.start()

    logger.info(f"\n*** Worker {pid} listening on {address} ***\n")
    _log("STARTED", extra=f"listening on {address}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info(f"[Worker {pid}] shutting down")
        _log("SHUTDOWN", extra="process stopping")
        server.stop(grace=5)

    if _log_fh:
        _log_fh.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <config_file>", file=sys.stderr)
        sys.exit(1)
    run(sys.argv[1])