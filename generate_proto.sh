#!/usr/bin/env bash
# generate_proto.sh
# Generates C++ and Python gRPC/protobuf stubs from proto/parking_violation_query.proto
# Run this once before building the project.

set -e

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
PROTO_FILE="$REPO_ROOT/proto/parking_violation_query.proto"
PROTO_DIR="$REPO_ROOT/proto"

# ---- C++ output (used by CMake build, but also useful to pre-generate) ----
CPP_OUT="$REPO_ROOT/proto/generated/cpp"
# ---- Python output (used by worker_server.py) ----
PY_OUT="$REPO_ROOT/proto/generated/python"

mkdir -p "$CPP_OUT" "$PY_OUT"

# -----------------------------------------------------------------------
# Detect grpc_cpp_plugin
# -----------------------------------------------------------------------
GRPC_CPP_PLUGIN=""
for dir in /opt/homebrew/bin /opt/anaconda3/bin /usr/local/bin /usr/bin; do
    if [ -x "$dir/grpc_cpp_plugin" ]; then
        GRPC_CPP_PLUGIN="$dir/grpc_cpp_plugin"
        break
    fi
done
if [ -z "$GRPC_CPP_PLUGIN" ]; then
    echo "ERROR: grpc_cpp_plugin not found. Install grpc (brew install grpc)."
    exit 1
fi

# -----------------------------------------------------------------------
# Detect grpc_python_plugin (grpc_tools)
# -----------------------------------------------------------------------
if python3 -c "import grpc_tools.protoc" 2>/dev/null; then
    PYTHON_PLUGIN="python3 -m grpc_tools.protoc"
    USE_GRPC_TOOLS=1
else
    USE_GRPC_TOOLS=0
    # fallback: look for grpc_python_plugin binary
    GRPC_PY_PLUGIN=""
    for dir in /opt/homebrew/bin /opt/anaconda3/bin /usr/local/bin /usr/bin; do
        if [ -x "$dir/grpc_python_plugin" ]; then
            GRPC_PY_PLUGIN="$dir/grpc_python_plugin"
            break
        fi
    done
fi

echo "=== Generating C++ stubs ==="
protoc \
    --cpp_out="$CPP_OUT" \
    --grpc_out="$CPP_OUT" \
    --plugin=protoc-gen-grpc="$GRPC_CPP_PLUGIN" \
    -I "$PROTO_DIR" \
    "$PROTO_FILE"
echo "  -> $CPP_OUT"

echo "=== Generating Python stubs ==="
if [ "$USE_GRPC_TOOLS" -eq 1 ]; then
    python3 -m grpc_tools.protoc \
        --python_out="$PY_OUT" \
        --grpc_python_out="$PY_OUT" \
        -I "$PROTO_DIR" \
        "$PROTO_FILE"
elif [ -n "$GRPC_PY_PLUGIN" ]; then
    protoc \
        --python_out="$PY_OUT" \
        --grpc_out="$PY_OUT" \
        --plugin=protoc-gen-grpc="$GRPC_PY_PLUGIN" \
        -I "$PROTO_DIR" \
        "$PROTO_FILE"
else
    echo "WARNING: grpc_tools not found. Install with: pip install grpcio-tools"
    echo "         Python worker (Process I) will not work without Python stubs."
fi

# Create __init__.py so the generated directory is importable as a package
touch "$PY_OUT/__init__.py"

echo ""
echo "=== Done ==="
echo "C++ stubs : $CPP_OUT"
echo "Python stubs: $PY_OUT"