#!/usr/bin/env bash

# Build and run the SWIM visual demo

echo "Building SWIM visual demo..."
cd "$(dirname "$0")/../../.."
go build -o bin/swim-visual-demo cmd/swim/visual-demo/*.go

if [ $? -eq 0 ]; then
    echo "✓ Build successful"
    echo ""
    ./bin/swim-visual-demo "$@"
else
    echo "✗ Build failed"
    exit 1
fi

