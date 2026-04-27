#!/usr/bin/env bash
# start_all.sh
# Starts all 9 processes for single-host development.
# Processes are started leaf-first so parents can connect immediately.
#
# Tree: A -> B -> C, D, E -> F, G
#            H, I
#
# Start order (leaves first):
#   Workers : C D F G H I
#   Team leaders: B E
#   Leader  : A

set -e

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="$REPO_ROOT/build"
CONFIG_DIR="$REPO_ROOT/configs"
LOG_DIR="$REPO_ROOT/logs"
PID_FILE="$REPO_ROOT/.pids"

mkdir -p "$LOG_DIR"

# -----------------------------------------------------------------------
# Verify build directory exists
# -----------------------------------------------------------------------
if [ ! -d "$BUILD_DIR" ]; then
    echo "ERROR: Build directory not found: $BUILD_DIR"
    echo "       Run: mkdir build && cd build && cmake .. && make -j$(nproc)"
    exit 1
fi

for bin in leader_server team_leader_server worker_server; do
    if [ ! -x "$BUILD_DIR/$bin" ]; then
        echo "ERROR: Binary not found or not executable: $BUILD_DIR/$bin"
        echo "       Run: cd build && make -j$(nproc)"
        exit 1
    fi
done

# -----------------------------------------------------------------------
# Clean up any previously running processes
# -----------------------------------------------------------------------
if [ -f "$PID_FILE" ]; then
    echo "==> Stopping previously running processes ..."
    while read pid; do
        kill "$pid" 2>/dev/null || true
    done < "$PID_FILE"
    rm -f "$PID_FILE"
    sleep 1
fi

> "$PID_FILE"  # create/truncate

start_process() {
    local name="$1"
    local binary="$2"
    local config="$3"

    echo "  Starting $name ..."
    "$BUILD_DIR/$binary" "$CONFIG_DIR/$config" \
        > "$LOG_DIR/${name}_stdout.log" 2>&1 &
    local pid=$!
    echo "$pid" >> "$PID_FILE"
    echo "    PID=$pid  config=$config"
    sleep 0.3   # brief pause so the port is open before the parent connects
}

start_python_worker() {
    local name="$1"
    local config="$2"
    local script="$REPO_ROOT/src/servers/worker/worker_server.py"

    echo "  Starting $name (Python) ..."
    python3 "$script" "$CONFIG_DIR/$config" \
        > "$LOG_DIR/${name}_stdout.log" 2>&1 &
    local pid=$!
    echo "$pid" >> "$PID_FILE"
    echo "    PID=$pid  config=$config"
    sleep 0.3
}

echo ""
echo "=== Starting Parking Violation System (single-host) ==="
echo ""

# ---- Layer 3: leaf workers (C, D, F, G, H) ----
echo "--> Layer 3: leaf workers"
start_process "process_c" "worker_server"      "process_c.json"
start_process "process_d" "worker_server"      "process_d.json"
start_process "process_f" "worker_server"      "process_f.json"
start_process "process_g" "worker_server"      "process_g.json"
start_process "process_h" "worker_server"      "process_h.json"

# ---- Layer 3: Python worker (I) ----
echo "--> Layer 3: Python worker"
start_python_worker "process_i" "process_i.json"

echo ""
sleep 1   # wait for leaf workers to bind ports

# ---- Layer 2: team leaders (B, E) ----
echo "--> Layer 2: team leaders"
start_process "process_b" "team_leader_server" "process_b.json"
start_process "process_e" "team_leader_server" "process_e.json"

echo ""
sleep 1   # wait for team leaders to bind ports

# ---- Layer 1: leader (A) ----
echo "--> Layer 1: leader"
start_process "process_a" "leader_server"      "process_a.json"

echo ""
sleep 1

echo "=== All processes started. PIDs saved to $PID_FILE ==="
echo ""
echo "Leader (A) is listening on localhost:50051"
echo ""
echo "Run a query:"
echo "  $BUILD_DIR/client --health"
echo "  $BUILD_DIR/client --county NY --max 100"
echo "  $BUILD_DIR/client --county BK --max 200 --verbose"
echo ""
echo "Stop all processes:"
echo "  ./stop_all.sh"