#!/bin/bash
#
# archive_logs.sh — Archive logs and results with timestamp
#
# Creates a timestamped snapshot in archives/ containing:
#   - All node logs (logs/*.log)
#   - All result files (results_*.txt)
#
# Usage:
#   bash scripts/archive_logs.sh              # auto-timestamp
#   bash scripts/archive_logs.sh "my_label"   # custom label appended to timestamp

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"
ARCHIVE_DIR="$ROOT_DIR/archives"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LABEL="${1:-}"
if [ -n "$LABEL" ]; then
    ARCHIVE_NAME="${TIMESTAMP}_${LABEL}"
else
    ARCHIVE_NAME="$TIMESTAMP"
fi

# Check if there's anything to archive
LOG_COUNT=$(find "$LOG_DIR" -name "*.log" 2>/dev/null | wc -l)
RESULT_COUNT=$(find "$ROOT_DIR" -maxdepth 1 -name "results_*.txt" 2>/dev/null | wc -l)

if [ "$LOG_COUNT" -eq 0 ] && [ "$RESULT_COUNT" -eq 0 ]; then
    echo "Nothing to archive (no logs or results found)."
    exit 0
fi

mkdir -p "$ARCHIVE_DIR/$ARCHIVE_NAME"

# Archive logs
if [ "$LOG_COUNT" -gt 0 ]; then
    cp "$LOG_DIR"/*.log "$ARCHIVE_DIR/$ARCHIVE_NAME/" 2>/dev/null
fi

# Archive results
if [ "$RESULT_COUNT" -gt 0 ]; then
    cp "$ROOT_DIR"/results_*.txt "$ARCHIVE_DIR/$ARCHIVE_NAME/" 2>/dev/null
fi

echo "Archived to: archives/$ARCHIVE_NAME/"
echo "  Logs: $LOG_COUNT file(s)"
echo "  Results: $RESULT_COUNT file(s)"
