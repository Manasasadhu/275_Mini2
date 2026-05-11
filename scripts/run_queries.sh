#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

CONFIG="${1:-configs/single-host/year-based/nodes_chunk500.json}"
CLIENT="build/client"

if [ ! -f "$CLIENT" ]; then
    echo "Error: $CLIENT not found. Please build first with: cmake -B build && cmake --build build"
    exit 1
fi

if [ ! -f "$CONFIG" ]; then
    echo "Error: Config file $CONFIG not found"
    exit 1
fi

echo "Running queries with config: $CONFIG"
echo "========================================"
echo ""

# Query 1: Plate violation history
echo "[Query 1] Plate Violation History - JER1863, NY, 2022-01-01 to 2025-12-31"
$CLIENT -c "$CONFIG"
echo ""

echo "========================================"
echo "All 4 queries completed!"
