# Kill any processes using Raft ports (including 4th server)
echo "🧹 Killing old processes..."
lsof -ti:50051,50052,50053,50054 2>/dev/null | xargs kill -9 2>/dev/null || true
sleep 1

# Remove old DB files
echo "🗑️  Removing old database files..."
rm -f ../../data/*.db || true

# Remove temporary server binary
rm -f /tmp/raft-server || true

echo "✅ Cleanup complete!"
