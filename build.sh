#!/bin/bash
set -e

BUILD_DIR="build"

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_GRPC_COMPONENTS=ON

# Build all targets: load_test, server, client
make -j$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)

echo ""
echo "Build complete."
echo "Executables available:"
echo "  - build/load_test      (CSV loader test)"
echo "  - build/src/server/server (C++ gRPC server)"
echo "  - build/src/client/client (C++ gRPC client)"