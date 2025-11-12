#!/usr/bin/env bash

# Cleanup script for Raft Visual Demo
# Kills all running instances and clears ports

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  Cleaning up Raft Visual Demo processes...                    ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Kill raft-visual-demo processes
echo "⏹  Killing raft-visual-demo processes..."
pkill -9 -f raft-visual-demo 2>/dev/null && echo "✓ Processes killed" || echo "  No processes found"

# Clear ports
echo "⏹  Clearing ports 50051, 50052, 50053, 8080..."
lsof -ti:50051,50052,50053,8080 2>/dev/null | xargs kill -9 2>/dev/null && echo "✓ Ports cleared" || echo "  No ports in use"

# Verify
echo ""
echo "Verification:"
if lsof -i:50051,50052,50053,8080 2>/dev/null | grep LISTEN > /dev/null; then
    echo "⚠️  Warning: Some ports are still in use:"
    lsof -i:50051,50052,50053,8080 2>/dev/null | grep LISTEN
else
    echo "✓ All ports are free"
fi

echo ""
echo "✓ Cleanup complete! You can now start the demo."

