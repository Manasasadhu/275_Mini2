#!/bin/bash
#
# configure_hosts.sh — Auto-generate multi-host config from IP addresses
#
# Usage:
#   2 machines:
#     bash scripts/configure_hosts.sh 172.16.0.200 172.16.0.154
#
#   3 machines:
#     bash scripts/configure_hosts.sh 172.16.0.100 172.16.0.200 172.16.0.154
#
#   Auto-detect (uses this machine's ethernet IP as Machine 1):
#     bash scripts/configure_hosts.sh auto 172.16.0.154
#
# Output: configs/nodes_multi.json (overwritten)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT="$ROOT_DIR/configs/nodes_multi.json"

if [ $# -lt 2 ]; then
    echo "Usage:"
    echo "  2 hosts: bash scripts/configure_hosts.sh <IP1> <IP2>"
    echo "  3 hosts: bash scripts/configure_hosts.sh <IP1> <IP2> <IP3>"
    echo ""
    echo "  IP1 = Machine running A, B, C (+ D, E for 2-host)"
    echo "  IP2 = Machine running D, E, F (3-host) or F, G, H, I (2-host)"
    echo "  IP3 = Machine running G, H, I (3-host only)"
    echo ""
    echo "  Use 'auto' for IP1 to detect this machine's ethernet IP"
    exit 1
fi

# Resolve 'auto' to actual IP
IP1="$1"
if [ "$IP1" = "auto" ]; then
    IP1=$(ifconfig 2>/dev/null | grep "inet " | grep -v 127.0.0.1 | grep -v "10.0.0" | head -1 | awk '{print $2}')
    if [ -z "$IP1" ]; then
        IP1=$(ifconfig 2>/dev/null | grep "inet " | grep -v 127.0.0.1 | head -1 | awk '{print $2}')
    fi
    echo "Auto-detected IP1: $IP1"
fi

IP2="$2"
IP3="${3:-}"

if [ -n "$IP3" ]; then
    # 3-host mode: A,B,C on IP1 | D,E,F on IP2 | G,H,I on IP3
    echo "Generating 3-host config:"
    echo "  Machine 1 ($IP1): Nodes A, B, C"
    echo "  Machine 2 ($IP2): Nodes D, E, F"
    echo "  Machine 3 ($IP3): Nodes G, H, I"

    HOST_A="$IP1"; HOST_B="$IP1"; HOST_C="$IP1"
    HOST_D="$IP2"; HOST_E="$IP2"; HOST_F="$IP2"
    HOST_G="$IP3"; HOST_H="$IP3"; HOST_I="$IP3"
else
    # 2-host mode: A,B,C,D,E on IP1 | F,G,H,I on IP2
    echo "Generating 2-host config:"
    echo "  Machine 1 ($IP1): Nodes A, B, C, D, E"
    echo "  Machine 2 ($IP2): Nodes F, G, H, I"

    HOST_A="$IP1"; HOST_B="$IP1"; HOST_C="$IP1"; HOST_D="$IP1"; HOST_E="$IP1"
    HOST_F="$IP2"; HOST_G="$IP2"; HOST_H="$IP2"; HOST_I="$IP2"
fi

cat > "$OUTPUT" << EOF
{
  "global": {
    "default_chunk_size": 500,
    "shared_data_file": "combined_parking_violations.csv",
    "date_field": "Issue Date"
  },
  "nodes": {
    "A": {
      "node_id": "A",
      "host": "$HOST_A",
      "port": 50051,
      "neighbors": ["B", "G", "H", "I"],
      "language": "cpp",
      "role": "gateway",
      "team": "blue",
      "team_leader": false,
      "client_facing": true,
      "chunk_size": 100,
      "data_file": null,
      "shard": null
    },
    "B": {
      "node_id": "B",
      "host": "$HOST_B",
      "port": 50052,
      "neighbors": ["A", "C", "D", "E"],
      "language": "cpp",
      "role": "relay",
      "team": "blue",
      "team_leader": true,
      "client_facing": false,
      "chunk_size": 100,
      "data_file": null,
      "shard": null
    },
    "C": {
      "node_id": "C",
      "host": "$HOST_C",
      "port": 50053,
      "neighbors": ["B"],
      "language": "cpp",
      "role": "worker",
      "team": "yellow",
      "team_leader": false,
      "client_facing": false,
      "chunk_size": 100,
      "data_file": null,
      "shard": {"type": "issue_date_range", "start": "2018-07-01", "end": "2019-06-30"}
    },
    "D": {
      "node_id": "D",
      "host": "$HOST_D",
      "port": 50054,
      "neighbors": ["B", "E"],
      "language": "cpp",
      "role": "worker",
      "team": "blue",
      "team_leader": false,
      "client_facing": false,
      "chunk_size": 100,
      "data_file": null,
      "shard": {"type": "issue_date_range", "start": "2022-07-01", "end": "2022-12-31"}
    },
    "E": {
      "node_id": "E",
      "host": "$HOST_E",
      "port": 50055,
      "neighbors": ["B", "D", "F", "G"],
      "language": "cpp",
      "role": "relay",
      "team": "yellow",
      "team_leader": true,
      "client_facing": false,
      "chunk_size": 100,
      "data_file": null,
      "shard": null
    },
    "F": {
      "node_id": "F",
      "host": "$HOST_F",
      "port": 50056,
      "neighbors": ["E"],
      "language": "python",
      "role": "worker",
      "team": "yellow",
      "team_leader": false,
      "client_facing": false,
      "chunk_size": 100,
      "data_file": null,
      "shard": {"type": "issue_date_range", "start": "2023-07-01", "end": "2024-06-30"}
    },
    "G": {
      "node_id": "G",
      "host": "$HOST_G",
      "port": 50057,
      "neighbors": ["A", "E"],
      "language": "cpp",
      "role": "worker",
      "team": "yellow",
      "team_leader": false,
      "client_facing": false,
      "chunk_size": 100,
      "data_file": null,
      "shard": {"type": "issue_date_range", "start": "2024-07-01", "end": "2025-06-30"}
    },
    "H": {
      "node_id": "H",
      "host": "$HOST_H",
      "port": 50058,
      "neighbors": ["A"],
      "language": "python",
      "role": "worker",
      "team": "blue",
      "team_leader": false,
      "client_facing": false,
      "chunk_size": 100,
      "data_file": null,
      "shard": {"type": "issue_date_range", "start": "2025-07-01", "end": "2026-06-30"}
    },
    "I": {
      "node_id": "I",
      "host": "$HOST_I",
      "port": 50059,
      "neighbors": ["A"],
      "language": "python",
      "role": "worker",
      "team": "yellow",
      "team_leader": false,
      "client_facing": false,
      "chunk_size": 100,
      "data_file": null,
      "shard": {"type": "issue_date_range", "start": "2023-01-01", "end": "2023-06-30"}
    }
  }
}
EOF

echo ""
echo "Config written to: $OUTPUT"
echo ""
echo "Next steps:"
echo "  1. Copy $OUTPUT to all machines"
echo "  2. Start workers first, then gateway:"
if [ -n "$IP3" ]; then
    echo "     Machine 3: bash scripts/start_local.sh configs/nodes_multi.json $IP3"
    echo "     Machine 2: bash scripts/start_local.sh configs/nodes_multi.json $IP2"
    echo "     Machine 1: bash scripts/start_local.sh configs/nodes_multi.json $IP1"
else
    echo "     Machine 2: bash scripts/start_local.sh configs/nodes_multi.json $IP2"
    echo "     Machine 1: bash scripts/start_local.sh configs/nodes_multi.json $IP1"
fi
echo "  3. Run client: ./build/client -c configs/nodes_multi.json"
