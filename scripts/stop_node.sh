#!/bin/bash
#
# stop_node.sh — Stops all nodes running on THIS machine
#
# Usage:
#   bash scripts/stop_node.sh           # stop all local nodes
#   bash scripts/stop_node.sh A B C     # stop specific nodes

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"

if [ $# -gt 0 ]; then
    NODES="$@"
else
    NODES="A B C D E F G H I"
fi

STOPPED=0
for node in $NODES; do
    PID_FILE="$LOG_DIR/$node.pid"
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "Stopping node $node (PID: $PID)..."
            kill "$PID" 2>/dev/null || true
            STOPPED=$((STOPPED + 1))
        else
            echo "Node $node (PID: $PID) not running"
        fi
        rm -f "$PID_FILE"
    fi
done

if [ $STOPPED -eq 0 ]; then
    echo "No running nodes found."
else
    echo ""
    echo "$STOPPED node(s) stopped."
fi
