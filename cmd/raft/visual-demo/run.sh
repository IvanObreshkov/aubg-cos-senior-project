#!/usr/bin/env bash

# Raft Visual Demo Runner

PROJECT_DIR="/Users/iobreshkov/PersonalCode/aubg-cos-senior-project"
cd "$PROJECT_DIR" || exit 1

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║     Raft Log Replication Visual Demo                          ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Check if binary exists, build if not
if [ ! -f "bin/raft-visual-demo" ]; then
    echo "Building visual demo..."
    go build -o bin/raft-visual-demo cmd/raft/visual-demo/*.go
    if [ $? -ne 0 ]; then
        echo "❌ Build failed"
        exit 1
    fi
    echo "✓ Build successful"
    echo ""
fi

./bin/raft-visual-demo "$@"

