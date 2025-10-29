#!/usr/bin/env bash

# SWIM Protocol Visual Demo
# Shows real-time logs in a more readable format

PROJECT_DIR="/Users/iobreshkov/PersonalCode/aubg-cos-senior-project"
BIN="$PROJECT_DIR/bin/swim-demo"
LOG_DIR="$PROJECT_DIR/logs/swim-visual"

mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/*.log

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║          SWIM Protocol Visual Demonstration                   ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "This demo will show you the SWIM protocol in action step-by-step."
echo ""
echo "Press Enter to start..."
read

# PIDs
PID1=""
PID2=""
PID3=""

cleanup() {
    echo ""
    echo "Cleaning up..."
    [ -n "$PID1" ] && kill $PID1 2>/dev/null
    [ -n "$PID2" ] && kill $PID2 2>/dev/null
    [ -n "$PID3" ] && kill $PID3 2>/dev/null
    wait 2>/dev/null
}

trap cleanup EXIT INT TERM

show_recent_logs() {
    local node=$1
    local lines=${2:-5}
    echo ""
    echo "=== Recent activity from $node ==="
    tail -n "$lines" "$LOG_DIR/$node.log" 2>/dev/null | \
        grep -E "JOINED|LEFT|FAILED|SUSPECT|Member|cluster members" | \
        sed 's/^2025\/10\/28 [0-9:]*//g' | \
        head -10
    echo ""
}

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 1: Starting the seed node (node1)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Node1 will start on port 7946 and wait for other nodes to join."
echo ""

$BIN -id=node1 -bind=127.0.0.1:7946 > "$LOG_DIR/node1.log" 2>&1 &
PID1=$!

sleep 3
show_recent_logs "node1"

echo "Press Enter to continue..."
read

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 2: Adding node2 to the cluster"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Node2 will join by contacting node1 (the seed)."
echo "Watch for:"
echo "  • Join request"
echo "  • Membership sync"
echo "  • Ping/Ack exchanges begin"
echo ""

$BIN -id=node2 -bind=127.0.0.1:7947 -join=127.0.0.1:7946 > "$LOG_DIR/node2.log" 2>&1 &
PID2=$!

sleep 4
show_recent_logs "node1" 8
show_recent_logs "node2" 5

echo "Press Enter to continue..."
read

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 3: Adding node3 to the cluster"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Node3 joins the cluster. All nodes will gossip about the new member."
echo ""

$BIN -id=node3 -bind=127.0.0.1:7948 -join=127.0.0.1:7946 > "$LOG_DIR/node3.log" 2>&1 &
PID3=$!

sleep 4
show_recent_logs "node1" 6

echo ""
echo "Current cluster state (from node1):"
tail -n 50 "$LOG_DIR/node1.log" | grep "Current cluster members" -A 4 | tail -5

echo ""
echo "Press Enter to continue..."
read

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 4: Observing normal protocol operation"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "The cluster is now running. Let's watch the protocol for 5 seconds..."
echo ""
echo "You should see:"
echo "  • Random member probing (Section 3 of the paper)"
echo "  • Ping/Ack exchanges"
echo "  • Periodic status updates"
echo ""

sleep 5

echo "Sample of protocol messages:"
tail -n 20 "$LOG_DIR/node1.log" | grep -E "Probing|Received ping|Received ACK" | head -10

echo ""
echo "Press Enter to simulate a failure..."
read

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 5: Simulating node failure"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "💥 Killing node3 to simulate a crash..."
echo ""
echo "Watch for the SWIM failure detection protocol:"
echo "  1. Direct ping fails"
echo "  2. Indirect probes via other nodes"
echo "  3. Member marked as SUSPECT"
echo "  4. After timeout, marked as FAILED"
echo ""

kill -9 $PID3 2>/dev/null
PID3=""

echo "Waiting 10 seconds for failure detection..."
sleep 10

echo ""
echo "Failure detection results:"
tail -n 50 "$LOG_DIR/node1.log" | grep -E "timeout|indirect|SUSPECT|FAILED" | tail -10

echo ""
echo "Press Enter to demonstrate graceful leave..."
read

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 6: Graceful node departure"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Gracefully stopping node2 (SIGTERM)..."
echo ""
echo "Node2 should:"
echo "  • Announce its departure (LEFT message)"
echo "  • Gossip the leave notification"
echo "  • Shut down cleanly"
echo ""

kill -SIGTERM $PID2 2>/dev/null
wait $PID2 2>/dev/null
PID2=""

sleep 3

echo ""
echo "Leave notification:"
tail -n 30 "$LOG_DIR/node1.log" | grep -E "LEFT|leaving" | tail -5

echo ""
echo "Final cluster state:"
tail -n 50 "$LOG_DIR/node1.log" | grep "Current cluster members" -A 4 | tail -5

echo ""
echo "Press Enter to see the summary..."
read

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                     Demonstration Summary                      ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "✓ Step 1: Seed node started"
echo "✓ Step 2: Second node joined via gossip"
echo "✓ Step 3: Third node joined, cluster formed"
echo "✓ Step 4: Observed normal protocol operation"
echo "✓ Step 5: Failure detected via ping/indirect-ping"
echo "✓ Step 6: Graceful leave notification"
echo ""
echo "Key SWIM Features Demonstrated:"
echo ""
echo "  📋 Section 3 - Basic Protocol"
echo "     • Random member probing"
echo "     • Direct ping/ack"
echo "     • Indirect ping-req protocol"
echo ""
echo "  🔍 Section 4.2 - Suspicion Mechanism"
echo "     • Suspect state before failure"
echo "     • Reduced false positives"
echo ""
echo "  📢 Section 4.4 - Gossip Dissemination"
echo "     • Updates piggybacked on messages"
echo "     • Infection-style propagation"
echo ""
echo "  👋 Section 4.1 - Voluntary Leave"
echo "     • Clean departure notification"
echo "     • Immediate gossip of leave"
echo ""
echo "Logs saved in: $LOG_DIR"
echo ""
echo "To review logs:"
echo "  cat $LOG_DIR/node1.log"
echo "  cat $LOG_DIR/node2.log"
echo "  cat $LOG_DIR/node3.log"
echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║         ✓ SWIM Protocol Demonstration Complete!               ║"
echo "╚════════════════════════════════════════════════════════════════╝"

