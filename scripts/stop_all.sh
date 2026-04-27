#!/bin/bash

LOG_DIR="logs"

for node in A B C D E F G H I; do
    pid_file="$LOG_DIR/$node.pid"
    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        echo "Stopping node $node (PID: $pid)..."
        kill "$pid" 2>/dev/null || true
        rm "$pid_file"
    fi
done

echo "All nodes stopped."
