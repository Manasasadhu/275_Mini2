#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"

for pid_file in "$LOG_DIR"/*.pid; do
    [ -f "$pid_file" ] || continue
    PID=$(cat "$pid_file")
    NODE=$(basename "$pid_file" .pid)
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID" 2>/dev/null
        echo "Stopped $NODE (PID $PID)"
    fi
    rm -f "$pid_file"
done

echo "All local nodes stopped."
