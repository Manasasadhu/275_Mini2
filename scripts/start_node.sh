#!/bin/bash
#
# start_node.sh — Multi-host startup script
#
# Detects which nodes belong to THIS machine (by matching IP),
# builds if needed, generates Python proto stubs, and starts only local nodes.
#
# Usage:
#   bash scripts/start_node.sh                    # auto-detect local IP
#   bash scripts/start_node.sh 192.168.1.10       # specify IP manually
#   bash scripts/start_node.sh localhost           # single-host mode
#
# Works on any number of machines — just update nodes.json host fields.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG="$ROOT_DIR/configs/nodes.json"
CPP_SERVER="$ROOT_DIR/build/server"
PYTHON_SERVER="$ROOT_DIR/python/server/server.py"
LOG_DIR="$ROOT_DIR/logs"
PYTHON_BIN="python3"

if command -v python3.10 >/dev/null 2>&1; then
    PYTHON_BIN="python3.10"
fi

mkdir -p "$LOG_DIR"

# -----------------------------------------------------------------------
# Step 1: Determine this machine's IP
# -----------------------------------------------------------------------
if [ -n "$1" ]; then
    MY_IP="$1"
else
    MY_IP=$(ifconfig 2>/dev/null | grep "inet " | grep -v 127.0.0.1 | head -1 | awk '{print $2}')
    if [ -z "$MY_IP" ]; then
        MY_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
    fi
    if [ -z "$MY_IP" ]; then
        MY_IP="localhost"
    fi
fi

echo "============================================="
echo "  Parking Violation System — Node Starter"
echo "============================================="
echo "Machine IP : $MY_IP"
echo "Config     : $CONFIG"
echo ""

# -----------------------------------------------------------------------
# Step 2: Find which nodes belong to this machine
# -----------------------------------------------------------------------
if ! command -v python3 >/dev/null 2>&1; then
    echo "ERROR: python3 is required to parse config"
    exit 1
fi

LOCAL_NODES=$(python3 -c "
import json, sys
with open('$CONFIG') as f:
    data = json.load(f)
my_ip = '$MY_IP'
nodes = []
for nid, nd in data['nodes'].items():
    if nd['host'] == my_ip or (my_ip == 'localhost' and nd['host'] == 'localhost'):
        nodes.append(nid)
# Sort: workers first, then relays, then gateway (so dependencies start first)
role_order = {'worker': 0, 'relay': 1, 'gateway': 2}
nodes.sort(key=lambda n: role_order.get(data['nodes'][n]['role'], 0))
print(' '.join(nodes))
")

if [ -z "$LOCAL_NODES" ]; then
    echo "ERROR: No nodes found for IP=$MY_IP in $CONFIG"
    echo ""
    echo "Hosts in config:"
    python3 -c "
import json
with open('$CONFIG') as f:
    data = json.load(f)
for nid, nd in data['nodes'].items():
    print(f'  {nid}: {nd[\"host\"]}:{nd[\"port\"]}')
"
    echo ""
    echo "Either update nodes.json or pass your IP manually:"
    echo "  bash scripts/start_node.sh YOUR_IP"
    exit 1
fi

echo "Nodes on this machine: $LOCAL_NODES"
echo ""

# -----------------------------------------------------------------------
# Step 3: Check data file exists
# -----------------------------------------------------------------------
DATA_FILE=$(python3 -c "
import json
with open('$CONFIG') as f:
    data = json.load(f)
print(data['global']['shared_data_file'])
")

if [ ! -f "$DATA_FILE" ]; then
    ABS_PATH="$ROOT_DIR/$DATA_FILE"
    if [ ! -f "$ABS_PATH" ]; then
        echo "WARNING: Data file not found: $DATA_FILE"
        echo "         Also checked: $ABS_PATH"
        echo "         Worker nodes will fail to process queries."
        echo ""
    fi
fi

# -----------------------------------------------------------------------
# Step 4: Build C++ if needed
# -----------------------------------------------------------------------
HAS_CPP=false
for node in $LOCAL_NODES; do
    LANG=$(python3 -c "
import json
with open('$CONFIG') as f:
    data = json.load(f)
print(data['nodes']['$node']['language'])
")
    if [ "$LANG" = "cpp" ]; then
        HAS_CPP=true
        break
    fi
done

if [ "$HAS_CPP" = true ] && [ ! -x "$CPP_SERVER" ]; then
    echo "C++ server binary not found. Building..."
    cd "$ROOT_DIR"
    ./build.sh
    echo ""
fi

# -----------------------------------------------------------------------
# Step 5: Generate Python proto stubs if needed
# -----------------------------------------------------------------------
HAS_PYTHON=false
for node in $LOCAL_NODES; do
    LANG=$(python3 -c "
import json
with open('$CONFIG') as f:
    data = json.load(f)
print(data['nodes']['$node']['language'])
")
    if [ "$LANG" = "python" ]; then
        HAS_PYTHON=true
        break
    fi
done

if [ "$HAS_PYTHON" = true ]; then
    PB2_FILE="$ROOT_DIR/python/server/parking_violation_query_pb2.py"
    if [ ! -f "$PB2_FILE" ]; then
        echo "Python proto stubs not found. Generating..."
        if [ -f "$ROOT_DIR/scripts/gen_proto.sh" ]; then
            bash "$ROOT_DIR/scripts/gen_proto.sh"
        else
            echo "ERROR: scripts/gen_proto.sh not found. Generate proto stubs manually."
            exit 1
        fi
        echo ""
    fi

    if ! $PYTHON_BIN -c "import grpc" 2>/dev/null; then
        echo "Installing grpcio..."
        $PYTHON_BIN -m pip install grpcio grpcio-tools --quiet
        echo ""
    fi
fi

# -----------------------------------------------------------------------
# Step 6: Stop any previously running nodes on this machine
# -----------------------------------------------------------------------
for node in $LOCAL_NODES; do
    PID_FILE="$LOG_DIR/$node.pid"
    if [ -f "$PID_FILE" ]; then
        OLD_PID=$(cat "$PID_FILE")
        if kill -0 "$OLD_PID" 2>/dev/null; then
            echo "Stopping previous node $node (PID: $OLD_PID)..."
            kill "$OLD_PID" 2>/dev/null || true
            sleep 0.3
        fi
        rm -f "$PID_FILE"
    fi
done

# -----------------------------------------------------------------------
# Step 7: Start nodes (workers first, then relays, then gateway)
# -----------------------------------------------------------------------
echo "Starting nodes..."
echo ""

for node in $LOCAL_NODES; do
    NODE_INFO=$(python3 -c "
import json
with open('$CONFIG') as f:
    data = json.load(f)
nd = data['nodes']['$node']
print(nd['language'], nd['role'], nd['port'])
")
    LANG=$(echo "$NODE_INFO" | awk '{print $1}')
    ROLE=$(echo "$NODE_INFO" | awk '{print $2}')
    PORT=$(echo "$NODE_INFO" | awk '{print $3}')

    if [ "$LANG" = "cpp" ]; then
        echo "  [$node] Starting C++ $ROLE on port $PORT..."
        "$CPP_SERVER" -n "$node" -c "$CONFIG" > "$LOG_DIR/$node.log" 2>&1 &
    elif [ "$LANG" = "python" ]; then
        echo "  [$node] Starting Python $ROLE on port $PORT..."
        "$PYTHON_BIN" "$PYTHON_SERVER" -n "$node" -c "$CONFIG" > "$LOG_DIR/$node.log" 2>&1 &
    else
        echo "  [$node] Unknown language: $LANG, skipping"
        continue
    fi

    echo $! > "$LOG_DIR/$node.pid"
    sleep 0.5
done

echo ""
echo "============================================="
echo "  All local nodes started!"
echo "============================================="
echo ""
echo "Nodes running on this machine ($MY_IP):"
for node in $LOCAL_NODES; do
    PID=$(cat "$LOG_DIR/$node.pid" 2>/dev/null || echo "?")
    NODE_INFO=$(python3 -c "
import json
with open('$CONFIG') as f:
    data = json.load(f)
nd = data['nodes']['$node']
print(nd['role'], nd['language'], nd['port'])
")
    ROLE=$(echo "$NODE_INFO" | awk '{print $1}')
    LANG=$(echo "$NODE_INFO" | awk '{print $2}')
    PORT=$(echo "$NODE_INFO" | awk '{print $3}')
    echo "  $node  $ROLE  $LANG  port=$PORT  PID=$PID"
done
echo ""
echo "View logs    : tail -f $LOG_DIR/<node>.log"
echo "Stop nodes   : bash scripts/stop_node.sh"
echo "Run client   : ./build/client -c configs/nodes.json"
echo ""
