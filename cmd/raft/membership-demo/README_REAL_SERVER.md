# Raft Membership Demo with REAL 4th Server

This demo now **actually starts a 4th server** instead of just adding it virtually!

## What's New

✅ **Real 4th server** - Actually spins up on port 50054  
✅ **Automatic join** - Server adds itself to the cluster  
✅ **Log catch-up** - 4th server receives all historical entries  
✅ **Full participation** - 4th server votes and replicates  
✅ **Clean removal** - RemoveServer works properly  

## How to Run

### Terminal 1: Start the 3-Server Cluster
```bash
./cleanup.sh
go run ./cmd/app/main.go
```

Wait for: `Started election timers - cluster is ready`

### Terminal 2: Run the Membership Demo
```bash
go run ./cmd/membership-demo/main.go
```

## What Happens

### Phase 1-2: Initial 3-Server Cluster
- Shows 3 servers
- Submits command to verify replication works

### Phase 3: **Start 4th Server** (NEW!)
```
Building server binary...
✓ Server binary built

Starting 4th server process...
✓ Server started (PID: 12345)

The 4th server is now:
- Running on port 50054
- Contacting the leader to join
- Catching up on the log
- Participating in consensus
```

**Behind the scenes:**
1. Demo builds `./cmd/single-server/main.go`
2. Starts it as a background process
3. 4th server calls `AddServer` RPC to join cluster
4. Leader creates C_old,new configuration
5. C_old,new gets replicated and committed
6. Leader creates C_new configuration
7. C_new gets replicated and committed
8. 4th server is now a full cluster member!

### Phase 4: 4-Server Cluster
- **Queries all 4 servers** (including the new one!)
- Shows that 4th server has caught up on the log

### Phase 5: Command with 4 Servers
- Submits command that replicates to **all 4 servers**
- Demonstrates 4-server consensus

### Phase 6: Remove a Server
- Removes one of the original followers
- Uses RemoveServer RPC
- Shows two-phase configuration change

### Phase 7: Back to 3 Servers
- Queries remaining servers
- Removed server stops participating

## Technical Details

### The `single-server` Program

Located at: `cmd/single-server/main.go`

**What it does:**
```go
1. Starts a Raft server on specified port
2. Contacts the leader (via --leader flag)
3. Calls AddServer RPC to join cluster
4. Starts orchestrator and election timer
5. Begins participating in Raft consensus
```

**Usage:**
```bash
go run ./cmd/single-server/main.go \
  -port 50054 \
  -leader localhost:50051
```

### Differences from Virtual Server

**Before (Virtual Server):**
- ❌ AddServer RPC just updated configuration
- ❌ No actual server process running
- ❌ Leader couldn't send RPCs to it
- ❌ Error spam in logs
- ❌ Couldn't query it for state

**Now (Real Server):**
- ✅ Actual server process starts
- ✅ Server joins cluster automatically
- ✅ Leader sends it AppendEntries RPCs
- ✅ No error spam
- ✅ Can query it like any other server

## Architecture

```
Initial Cluster:
┌──────────┐  ┌──────────┐  ┌──────────┐
│ Server 1 │  │ Server 2 │  │ Server 3 │
│ :50051   │  │ :50052   │  │ :50053   │
│ (Leader) │  │(Follower)│  │(Follower)│
└──────────┘  └──────────┘  └──────────┘

After AddServer:
┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│ Server 1 │  │ Server 2 │  │ Server 3 │  │ Server 4 │
│ :50051   │  │ :50052   │  │ :50053   │  │ :50054   │
│ (Leader) │  │(Follower)│  │(Follower)│  │(Follower)│
└──────────┘  └──────────┘  └──────────┘  └──────────┘
                                             ↑
                                        NEW! Actually
                                        running!
```

## Troubleshooting

**"Failed to build server"**
- Make sure you're in the project root directory
- Check that `cmd/single-server/main.go` exists

**"Failed to start server"**
- Port 50054 might already be in use
- Try: `lsof -i :50054` to check

**"Failed to join cluster"**
- Leader might not be ready yet
- Server will still run, just not in the cluster

**"Connection refused to server 4"**
- 4th server might still be starting
- Wait a few seconds for it to fully initialize

## Cleanup

The demo automatically kills the 4th server when it exits. If it doesn't:

```bash
# Find the process
ps aux | grep raft-server

# Kill it
kill <PID>

# Or kill all Go processes
pkill -f "raft-server"
```

## Benefits for Your Presentation

✅ **More realistic** - Shows how servers actually join  
✅ **Complete demo** - Full lifecycle (start → join → participate → leave)  
✅ **Visual proof** - Can query 4th server to see it's real  
✅ **Better logs** - No error spam about missing servers  
✅ **Impressive** - Demonstrates dynamic cluster membership  

This is **production-quality** membership management! 🚀

