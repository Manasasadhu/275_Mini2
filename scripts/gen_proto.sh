#!/bin/bash

set -e

PROTO_DIR="proto"
PY_OUT="python/server"
PLUGIN="$(command -v grpc_python_plugin || true)"

if [[ -z "$PLUGIN" ]]; then
  PLUGIN="/opt/homebrew/bin/grpc_python_plugin"
fi

if [[ ! -x "$PLUGIN" ]]; then
  echo "error: grpc_python_plugin not found. Install it or update scripts/gen_proto.sh."
  exit 1
fi

echo "Generating Python protobuf stubs..."
protoc -I"$PROTO_DIR" \
  --python_out="$PY_OUT" \
  --pyi_out="$PY_OUT" \
  --grpc_out="$PY_OUT" \
  --plugin=protoc-gen-grpc="$PLUGIN" \
  "$PROTO_DIR/parking_violation_query.proto"

echo "Generated files in $PY_OUT"
