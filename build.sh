#!/bin/bash
set -e

BUILD_DIR="build"

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_GRPC_COMPONENTS=ON

# Build all targets: load_test, server, client
make -j$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)

cd ..

# Set up Python venv if not present
if [ ! -d "venv" ]; then
    echo ""
    echo "Creating Python virtual environment..."
    python3 -m venv venv
    ./venv/bin/pip install --quiet grpcio grpcio-tools
    bash scripts/gen_proto.sh
    echo "Python venv ready."
fi

echo ""
echo "Build complete."
echo "Executables available:"
echo "  - build/load_test      (CSV loader test)"
echo "  - build/src/server/server (C++ gRPC server)"
echo "  - build/src/client/client (C++ gRPC client)"