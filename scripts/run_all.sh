#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

LOG_DIR="logs"
CONFIG="configs/nodes.json"
CPP_SERVER="build/server"
PYTHON_SERVER="python/server/server.py"
PYTHON_BIN="python3"

if command -v python3.10 >/dev/null 2>&1; then
    PYTHON_BIN="python3.10"
fi

mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/*.log "$LOG_DIR"/*.pid

# C++ nodes (A, B, C, D, E, G)
for node in A B C D E G; do
    echo "Starting C++ node $node..."
    "$CPP_SERVER" -n "$node" -c "$CONFIG" > "$LOG_DIR/$node.log" 2>&1 &
    echo $! > "$LOG_DIR/$node.pid"
    sleep 0.5  # Small delay to let server start
done

# Python nodes (F, H, I)
for node in F H I; do
    echo "Starting Python node $node with $PYTHON_BIN..."
    PYTHONUNBUFFERED=1 "$PYTHON_BIN" "$PYTHON_SERVER" -n "$node" -c "$CONFIG" > "$LOG_DIR/$node.log" 2>&1 &
    echo $! > "$LOG_DIR/$node.pid"
    sleep 0.5
done

echo ""
echo "All nodes started. Logs available in $LOG_DIR/"
echo "Monitor a node: tail -f $LOG_DIR/A.log"
echo "Stop all nodes: bash scripts/stop_all.sh"
