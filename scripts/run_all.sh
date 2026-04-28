#!/bin/bash

set -e

LOG_DIR="logs"
CONFIG="configs/nodes.json"
CPP_SERVER="build/server"
PYTHON_SERVER="python/server/server.py"

mkdir -p "$LOG_DIR"

# C++ nodes (A, B, C, D, E, G)
for node in A B C D E G; do
    echo "Starting C++ node $node..."
    "$CPP_SERVER" -n "$node" -c "$CONFIG" > "$LOG_DIR/$node.log" 2>&1 &
    echo $! > "$LOG_DIR/$node.pid"
    sleep 0.5  # Small delay to let server start
done

# Python nodes (F, H, I)
for node in F H I; do
    echo "Starting Python node $node..."
    python3 -u "$PYTHON_SERVER" -n "$node" -c "$CONFIG" > "$LOG_DIR/$node.log" 2>&1 &
    echo $! > "$LOG_DIR/$node.pid"
    sleep 0.5
done

echo ""
echo "All nodes started. Logs available in $LOG_DIR/"
echo "Monitor a node: tail -f $LOG_DIR/A.log"
echo "Stop all nodes: bash scripts/stop_all.sh"
