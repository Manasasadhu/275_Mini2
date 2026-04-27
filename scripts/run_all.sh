#!/bin/bash

set -e

LOG_DIR="logs"
CONFIG="configs/nodes.json"
CPP_SERVER="build/src/server/server"
PYTHON_SERVER="python/server/server.py"

mkdir -p "$LOG_DIR"

# C++ nodes
for node in A B C D E G; do
    echo "Starting node $node..."
    "$CPP_SERVER" -n "$node" -c "$CONFIG" > "$LOG_DIR/$node.log" 2>&1 &
    echo $! > "$LOG_DIR/$node.pid"
done

# Python nodes
for node in F H I; do
    echo "Starting node $node..."
    python3 "$PYTHON_SERVER" -n "$node" -c "$CONFIG" > "$LOG_DIR/$node.log" 2>&1 &
    echo $! > "$LOG_DIR/$node.pid"
done

echo "All nodes started. Check logs/ for details."
