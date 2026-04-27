#!/usr/bin/env bash
# stop_all.sh
# Stops all processes that were started by start_all.sh

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$REPO_ROOT/.pids"

if [ ! -f "$PID_FILE" ]; then
    echo "No PID file found at $PID_FILE — nothing to stop."
    exit 0
fi

echo "=== Stopping Parking Violation System ==="
stopped=0
while read pid; do
    if kill -0 "$pid" 2>/dev/null; then
        echo "  Killing PID $pid ..."
        kill "$pid" 2>/dev/null && stopped=$((stopped + 1))
    else
        echo "  PID $pid already stopped."
    fi
done < "$PID_FILE"

rm -f "$PID_FILE"
echo ""
echo "Stopped $stopped process(es)."

# Also kill any stray processes still binding the known ports
for port in 50051 50052 50053 50054 50055 50056 50057 50058 50059; do
    pid=$(lsof -ti tcp:$port 2>/dev/null) || true
    if [ -n "$pid" ]; then
        echo "  Killing stray process on port $port (PID $pid)"
        kill $pid 2>/dev/null || true
    fi
done

echo "Done."