#!/bin/bash

LOG_DIR="logs"

# Kill all nodes by reading their PID files
for node in A B C D E F G H I; do
    if [ -f "$LOG_DIR/$node.pid" ]; then
        PID=$(cat "$LOG_DIR/$node.pid")
        if kill $PID 2>/dev/null; then
            echo "Stopped node $node (PID: $PID)"
        fi
        rm -f "$LOG_DIR/$node.pid"
    fi
done

echo "All nodes stopped."
