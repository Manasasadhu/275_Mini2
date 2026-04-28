#!/bin/bash
# Simple test script to send a plate_id query to the gateway (node A)

CONFIG="configs/nodes.json"
REQUEST_ID="test_$(date +%s)"
PLATE_ID="ABC123"  # Test plate

echo "Sending query: request_id=$REQUEST_ID, plate_id=$PLATE_ID"
echo ""
echo "To test manually, compile and run:"
echo "  build/src/server/server -n C -c $CONFIG  # Worker node C"
echo "  build/src/server/server -n A -c $CONFIG  # Gateway node A"
echo "  build/src/client/client -c $CONFIG       # Client (sends query to A)"
echo ""
echo "Test query to send:"
echo "  request_id: $REQUEST_ID"
echo "  plate_id: $PLATE_ID"
echo "  chunk_size: 100"
