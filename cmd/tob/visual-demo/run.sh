#!/usr/bin/env bash

# Build and run the TOB visual demo

echo "Building TOB visual demo..."
cd "$(dirname "$0")/../../.."
go build -o bin/tob-visual-demo cmd/tob/visual-demo/*.go

if [ $? -eq 0 ]; then
    echo "✓ Build successful"
    echo ""
    ./bin/tob-visual-demo "$@"
else
    echo "✗ Build failed"
    exit 1
fi

