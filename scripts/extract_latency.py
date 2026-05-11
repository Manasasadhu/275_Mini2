#!/usr/bin/env python3
"""
Extract network latency data from server logs and save to CSV for graphing.
Usage: python scripts/extract_latency.py <log_dir> <output_file>
"""

import os
import re
import csv
import sys
from collections import defaultdict

def extract_latencies(log_dir):
    """Extract all latency measurements from logs."""
    latencies = []

    # Pattern to match latency logs: "[node] Latency to neighbor: X ms, chunks=Y"
    latency_pattern = r'\[(\w+)\] Latency to (\w+): (\d+) ms, chunks=(\d+)'

    for filename in os.listdir(log_dir):
        if filename.endswith('.log'):
            filepath = os.path.join(log_dir, filename)
            try:
                with open(filepath, 'r') as f:
                    for line in f:
                        match = re.search(latency_pattern, line)
                        if match:
                            from_node, to_node, latency_ms, chunks = match.groups()
                            latencies.append({
                                'from_node': from_node,
                                'to_node': to_node,
                                'latency_ms': int(latency_ms),
                                'chunks': int(chunks)
                            })
            except Exception as e:
                print(f"Warning: Could not read {filepath}: {e}", file=sys.stderr)

    return latencies

def aggregate_latencies(latencies):
    """Aggregate latencies by node pair."""
    aggregated = defaultdict(lambda: {'min': float('inf'), 'max': 0, 'sum': 0, 'count': 0})

    for entry in latencies:
        key = f"{entry['from_node']}->{entry['to_node']}"
        aggregated[key]['min'] = min(aggregated[key]['min'], entry['latency_ms'])
        aggregated[key]['max'] = max(aggregated[key]['max'], entry['latency_ms'])
        aggregated[key]['sum'] += entry['latency_ms']
        aggregated[key]['count'] += 1

    return aggregated

def save_to_csv(latencies, aggregated, output_file):
    """Save latency data to CSV file."""
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)

        # Header
        writer.writerow(['From Node', 'To Node', 'Latency (ms)', 'Chunks'])

        # Individual measurements
        for entry in latencies:
            writer.writerow([
                entry['from_node'],
                entry['to_node'],
                entry['latency_ms'],
                entry['chunks']
            ])

        # Separator and aggregated stats
        writer.writerow([])
        writer.writerow(['Node Pair', 'Min (ms)', 'Max (ms)', 'Avg (ms)', 'Count'])

        for node_pair in sorted(aggregated.keys()):
            stats = aggregated[node_pair]
            avg = stats['sum'] / stats['count']
            writer.writerow([
                node_pair,
                stats['min'],
                stats['max'],
                f"{avg:.2f}",
                stats['count']
            ])

if __name__ == '__main__':
    log_dir = sys.argv[1] if len(sys.argv) > 1 else 'logs'
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'latency_data.csv'

    if not os.path.isdir(log_dir):
        print(f"Error: Log directory '{log_dir}' not found", file=sys.stderr)
        sys.exit(1)

    latencies = extract_latencies(log_dir)
    if not latencies:
        print(f"No latency data found in {log_dir}", file=sys.stderr)
        sys.exit(1)

    aggregated = aggregate_latencies(latencies)
    save_to_csv(latencies, aggregated, output_file)

    print(f"Latency data saved to {output_file}")
    print(f"Total measurements: {len(latencies)}")
    print(f"Unique node pairs: {len(aggregated)}")
    print("\nSummary:")
    for node_pair in sorted(aggregated.keys()):
        stats = aggregated[node_pair]
        avg = stats['sum'] / stats['count']
        print(f"  {node_pair}: min={stats['min']}ms, max={stats['max']}ms, avg={avg:.2f}ms")
