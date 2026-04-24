#!/bin/bash
set -e

BUILD_DIR="build"

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_GRPC_COMPONENTS=OFF

# Build only load_test for now (no gRPC servers yet)
make -j$(nproc 2>/dev/null || sysctl -n hw.logicalcpu) load_test

echo ""
echo "Build complete."
echo "Run the loader test:"