#!/bin/bash
set -e

BUILD_DIR="build"

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

cmake .. -DCMAKE_BUILD_TYPE=Release

# Build only load_test for now (no gRPC servers yet)
make -j$(nproc 2>/dev/null || sysctl -n hw.logicalcpu) load_test

echo ""
echo "Build complete."
echo "Run the loader test:"
echo "  $BUILD_DIR/load_test \"/Users/Asha/Downloads/dob_permits copy.csv\""
echo "  $BUILD_DIR/load_test \"/Users/Asha/Downloads/dob_permits copy.csv\" MANHATTAN"
echo "  $BUILD_DIR/load_test \"/Users/Asha/Downloads/dob_permits copy.csv\" BROOKLYN EQ 1000"
