#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG="${1:-$ROOT_DIR/configs/nodes_multi.json}"
LOG_DIR="$ROOT_DIR/logs"
CPP_SERVER="$ROOT_DIR/build/server"
PYTHON_SERVER="$ROOT_DIR/python/server/server.py"
PYTHON_BIN="$ROOT_DIR/venv/bin/python"

if [ ! -x "$PYTHON_BIN" ]; then
    PYTHON_BIN="python3"
fi

mkdir -p "$LOG_DIR"

MY_IP="${2:-$(hostname -I 2>/dev/null | awk '{print $1}')}"

echo "============================================="
echo "  Starting local nodes"
echo "============================================="
echo "Config : $CONFIG"
echo "My IP  : $MY_IP"
echo ""

LOCAL_NODES=$($PYTHON_BIN -c "
import json
with open('$CONFIG') as f:
    data = json.load(f)
for nid, nd in data['nodes'].items():
    if nd['host'] == '$MY_IP':
        print(nid, nd['language'], nd['role'], nd['port'])
")

if [ -z "$LOCAL_NODES" ]; then
    echo "ERROR: No nodes found for IP=$MY_IP in $CONFIG"
    exit 1
fi

# Stop previously running local nodes
while IFS=' ' read -r node lang role port; do
    PID_FILE="$LOG_DIR/$node.pid"
    if [ -f "$PID_FILE" ]; then
        OLD_PID=$(cat "$PID_FILE")
        if kill -0 "$OLD_PID" 2>/dev/null; then
            kill "$OLD_PID" 2>/dev/null || true
        fi
        rm -f "$PID_FILE"
    fi
done <<< "$LOCAL_NODES"

sleep 0.5

# Archive existing logs and results before clearing
bash "$SCRIPT_DIR/archive_logs.sh"

# Clear old logs
rm -f "$LOG_DIR"/*.log

# Start nodes
while IFS=' ' read -r node lang role port; do
    if [ "$lang" = "cpp" ]; then
        echo "  [$node] C++ $role on port $port"
        "$CPP_SERVER" -n "$node" -c "$CONFIG" > "$LOG_DIR/$node.log" 2>&1 &
    elif [ "$lang" = "python" ]; then
        echo "  [$node] Python $role on port $port"
        PYTHONUNBUFFERED=1 "$PYTHON_BIN" "$PYTHON_SERVER" -n "$node" -c "$CONFIG" > "$LOG_DIR/$node.log" 2>&1 &
    fi
    echo $! > "$LOG_DIR/$node.pid"
    sleep 0.5
done <<< "$LOCAL_NODES"

echo ""
echo "All local nodes started. Logs in $LOG_DIR/"
echo "Check status : bash scripts/status.sh"
echo "Stop nodes   : bash scripts/stop_local.sh"
