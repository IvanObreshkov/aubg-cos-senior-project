#!/usr/bin/env bash

# SWIM Protocol Interactive Demo
# This script provides an interactive menu to demonstrate various SWIM scenarios

set -e

PROJECT_DIR="/Users/iobreshkov/PersonalCode/aubg-cos-senior-project"
BIN="$PROJECT_DIR/bin/swim-demo"
LOG_DIR="$PROJECT_DIR/logs/swim"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Create log directory
mkdir -p "$LOG_DIR"

# Clean up old logs
rm -f "$LOG_DIR"/*.log

# PID tracking (simple variables instead of associative array)
PID_NODE1=""
PID_NODE2=""
PID_NODE3=""
PID_NODE4=""
PID_NODE5=""

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}  SWIM Protocol Interactive Demo${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""

# Build the demo if needed
if [ ! -f "$BIN" ]; then
    echo -e "${YELLOW}Building SWIM demo...${NC}"
    cd "$PROJECT_DIR"
    go build -o bin/swim-demo cmd/swim/main.go
    echo -e "${GREEN}✓ Build complete${NC}"
fi

# Function to start a node
start_node() {
    local id=$1
    local port=$2
    local join_addr=$3
    local pid=""

    # Check if already running
    case $id in
        node1) pid=$PID_NODE1 ;;
        node2) pid=$PID_NODE2 ;;
        node3) pid=$PID_NODE3 ;;
        node4) pid=$PID_NODE4 ;;
        node5) pid=$PID_NODE5 ;;
    esac

    if [ -n "$pid" ]; then
        echo -e "${YELLOW}Node $id is already running (PID: $pid)${NC}"
        return
    fi

    local cmd="$BIN -id=$id -bind=127.0.0.1:$port -advertise=127.0.0.1:$port"
    if [ -n "$join_addr" ]; then
        cmd="$cmd -join=$join_addr"
    fi

    echo -e "${GREEN}Starting $id on port $port...${NC}"
    $cmd > "$LOG_DIR/$id.log" 2>&1 &
    pid=$!

    case $id in
        node1) PID_NODE1=$pid ;;
        node2) PID_NODE2=$pid ;;
        node3) PID_NODE3=$pid ;;
        node4) PID_NODE4=$pid ;;
        node5) PID_NODE5=$pid ;;
    esac

    echo -e "${GREEN}✓ $id started (PID: $pid)${NC}"
    sleep 1
}

# Function to stop a node
stop_node() {
    local id=$1
    local pid=""

    case $id in
        node1) pid=$PID_NODE1; PID_NODE1="" ;;
        node2) pid=$PID_NODE2; PID_NODE2="" ;;
        node3) pid=$PID_NODE3; PID_NODE3="" ;;
        node4) pid=$PID_NODE4; PID_NODE4="" ;;
        node5) pid=$PID_NODE5; PID_NODE5="" ;;
    esac

    if [ -z "$pid" ]; then
        echo -e "${YELLOW}Node $id is not running${NC}"
        return
    fi

    echo -e "${RED}Stopping $id (PID: $pid)...${NC}"
    kill -SIGTERM "$pid" 2>/dev/null || true
    echo -e "${RED}✓ $id stopped${NC}"
    sleep 1
}

# Function to kill a node (simulate crash)
kill_node() {
    local id=$1
    local pid=""

    case $id in
        node1) pid=$PID_NODE1; PID_NODE1="" ;;
        node2) pid=$PID_NODE2; PID_NODE2="" ;;
        node3) pid=$PID_NODE3; PID_NODE3="" ;;
        node4) pid=$PID_NODE4; PID_NODE4="" ;;
        node5) pid=$PID_NODE5; PID_NODE5="" ;;
    esac

    if [ -z "$pid" ]; then
        echo -e "${YELLOW}Node $id is not running${NC}"
        return
    fi

    echo -e "${RED}💥 Killing $id (simulating crash)...${NC}"
    kill -9 "$pid" 2>/dev/null || true
    echo -e "${RED}✓ $id crashed${NC}"
    sleep 1
}

# Function to show node status
show_status() {
    echo ""
    echo -e "${BLUE}=== Cluster Status ===${NC}"

    for id in node1 node2 node3 node4 node5; do
        local pid=""
        case $id in
            node1) pid=$PID_NODE1 ;;
            node2) pid=$PID_NODE2 ;;
            node3) pid=$PID_NODE3 ;;
            node4) pid=$PID_NODE4 ;;
            node5) pid=$PID_NODE5 ;;
        esac

        if [ -n "$pid" ]; then
            if kill -0 "$pid" 2>/dev/null; then
                echo -e "  ${GREEN}●${NC} $id - Running (PID: $pid)"
            else
                echo -e "  ${RED}●${NC} $id - Dead (PID: $pid)"
                case $id in
                    node1) PID_NODE1="" ;;
                    node2) PID_NODE2="" ;;
                    node3) PID_NODE3="" ;;
                    node4) PID_NODE4="" ;;
                    node5) PID_NODE5="" ;;
                esac
            fi
        else
            echo -e "  ${RED}○${NC} $id - Not started"
        fi
    done
    echo ""
}

# Function to tail logs
show_logs() {
    local id=$1
    local lines=${2:-20}

    if [ ! -f "$LOG_DIR/$id.log" ]; then
        echo -e "${YELLOW}No logs found for $id${NC}"
        return
    fi

    echo ""
    echo -e "${BLUE}=== Last $lines lines from $id ===${NC}"
    tail -n "$lines" "$LOG_DIR/$id.log"
    echo ""
}

# Function to follow logs
follow_logs() {
    local id=$1

    if [ ! -f "$LOG_DIR/$id.log" ]; then
        echo -e "${YELLOW}No logs found for $id${NC}"
        return
    fi

    echo -e "${BLUE}Following logs for $id (Ctrl+C to stop)${NC}"
    tail -f "$LOG_DIR/$id.log"
}

# Function to cleanup
cleanup() {
    echo ""
    echo -e "${YELLOW}Cleaning up...${NC}"
    [ -n "$PID_NODE1" ] && kill "$PID_NODE1" 2>/dev/null || true
    [ -n "$PID_NODE2" ] && kill "$PID_NODE2" 2>/dev/null || true
    [ -n "$PID_NODE3" ] && kill "$PID_NODE3" 2>/dev/null || true
    [ -n "$PID_NODE4" ] && kill "$PID_NODE4" 2>/dev/null || true
    [ -n "$PID_NODE5" ] && kill "$PID_NODE5" 2>/dev/null || true
    echo -e "${GREEN}✓ Cleanup complete${NC}"
    exit 0
}

trap cleanup EXIT INT TERM

# Scenario functions
scenario_basic_cluster() {
    echo ""
    echo -e "${BLUE}=== Scenario 1: Basic Cluster Formation ===${NC}"
    echo "Starting 3 nodes and watching them discover each other..."
    echo ""

    start_node "node1" 7946 ""
    sleep 2

    start_node "node2" 7947 "127.0.0.1:7946"
    sleep 2

    start_node "node3" 7948 "127.0.0.1:7946"
    sleep 3

    show_status
    echo -e "${GREEN}✓ Cluster formed with 3 nodes${NC}"
    echo -e "${BLUE}Check logs to see membership convergence${NC}"
}

scenario_failure_detection() {
    echo ""
    echo -e "${BLUE}=== Scenario 2: Failure Detection ===${NC}"
    echo "Starting 3 nodes, then simulating a crash..."
    echo ""

    # Start cluster if not already running
    if [ -z "${NODE_PIDS[node1]}" ]; then
        start_node "node1" 7946 ""
        sleep 2
        start_node "node2" 7947 "127.0.0.1:7946"
        sleep 2
        start_node "node3" 7948 "127.0.0.1:7946"
        sleep 3
    fi

    show_status
    echo ""
    echo -e "${YELLOW}Killing node2 to simulate crash in 3 seconds...${NC}"
    sleep 3

    kill_node "node2"

    echo ""
    echo -e "${BLUE}Watch the logs - node1 and node3 should detect node2's failure${NC}"
    echo -e "${BLUE}Detection should happen within 2-3 protocol periods (~2-3 seconds)${NC}"
    sleep 5

    show_status
    echo ""
    echo -e "${YELLOW}Recent logs from node1:${NC}"
    show_logs "node1" 10
}

scenario_suspicion_mechanism() {
    echo ""
    echo -e "${BLUE}=== Scenario 3: Suspicion Mechanism ===${NC}"
    echo "Demonstrating suspect -> failed transition..."
    echo ""

    # Start fresh cluster
    cleanup_nodes

    start_node "node1" 7946 ""
    sleep 2
    start_node "node2" 7947 "127.0.0.1:7946"
    sleep 2
    start_node "node3" 7948 "127.0.0.1:7946"
    sleep 3

    show_status
    echo ""
    echo -e "${YELLOW}Killing node3 in 3 seconds...${NC}"
    sleep 3

    kill_node "node3"

    echo ""
    echo -e "${BLUE}Watch for:${NC}"
    echo "  1. Node3 marked as SUSPECT (after probe timeout)"
    echo "  2. Indirect probes attempted"
    echo "  3. Node3 marked as FAILED (after suspicion timeout)"
    sleep 8

    echo ""
    echo -e "${YELLOW}Recent logs from node1:${NC}"
    show_logs "node1" 15
}

scenario_graceful_leave() {
    echo ""
    echo -e "${BLUE}=== Scenario 4: Graceful Leave ===${NC}"
    echo "Demonstrating voluntary departure..."
    echo ""

    # Ensure cluster is running
    if [ -z "${NODE_PIDS[node1]}" ]; then
        start_node "node1" 7946 ""
        sleep 2
        start_node "node2" 7947 "127.0.0.1:7946"
        sleep 2
        start_node "node3" 7948 "127.0.0.1:7946"
        sleep 3
    fi

    show_status
    echo ""
    echo -e "${YELLOW}Gracefully stopping node2 in 3 seconds...${NC}"
    sleep 3

    stop_node "node2"

    echo ""
    echo -e "${BLUE}Node2 should send leave notification${NC}"
    sleep 3

    echo ""
    echo -e "${YELLOW}Recent logs from node1:${NC}"
    show_logs "node1" 10
}

scenario_rejoin() {
    echo ""
    echo -e "${BLUE}=== Scenario 5: Node Rejoin ===${NC}"
    echo "Demonstrating a node rejoining after failure..."
    echo ""

    # Ensure we have a failed node
    if [ -n "${NODE_PIDS[node2]}" ]; then
        kill_node "node2"
        sleep 5
    fi

    show_status
    echo ""
    echo -e "${YELLOW}Restarting node2 in 3 seconds...${NC}"
    sleep 3

    start_node "node2" 7947 "127.0.0.1:7946"

    echo ""
    echo -e "${BLUE}Node2 should rejoin and sync with cluster${NC}"
    sleep 3

    show_status
    echo ""
    echo -e "${YELLOW}Recent logs from node2:${NC}"
    show_logs "node2" 15
}

scenario_scale_up() {
    echo ""
    echo -e "${BLUE}=== Scenario 6: Scaling Up ===${NC}"
    echo "Adding more nodes to the cluster..."
    echo ""

    # Start base cluster
    if [ -z "${NODE_PIDS[node1]}" ]; then
        start_node "node1" 7946 ""
        sleep 2
        start_node "node2" 7947 "127.0.0.1:7946"
        sleep 2
        start_node "node3" 7948 "127.0.0.1:7946"
        sleep 3
    fi

    show_status
    echo ""
    echo -e "${YELLOW}Adding node4 and node5...${NC}"

    start_node "node4" 7949 "127.0.0.1:7946"
    sleep 2
    start_node "node5" 7950 "127.0.0.1:7946"
    sleep 3

    show_status
    echo ""
    echo -e "${GREEN}✓ Cluster now has 5 nodes${NC}"
}

cleanup_nodes() {
    echo -e "${YELLOW}Stopping all nodes...${NC}"
    [ -n "$PID_NODE1" ] && kill "$PID_NODE1" 2>/dev/null || true
    [ -n "$PID_NODE2" ] && kill "$PID_NODE2" 2>/dev/null || true
    [ -n "$PID_NODE3" ] && kill "$PID_NODE3" 2>/dev/null || true
    [ -n "$PID_NODE4" ] && kill "$PID_NODE4" 2>/dev/null || true
    [ -n "$PID_NODE5" ] && kill "$PID_NODE5" 2>/dev/null || true
    PID_NODE1=""
    PID_NODE2=""
    PID_NODE3=""
    PID_NODE4=""
    PID_NODE5=""
    sleep 2
}

# Main menu
show_menu() {
    echo ""
    echo -e "${BLUE}=== SWIM Demo Menu ===${NC}"
    echo "  1) Basic Cluster Formation (3 nodes)"
    echo "  2) Failure Detection (crash simulation)"
    echo "  3) Suspicion Mechanism (suspect->failed)"
    echo "  4) Graceful Leave (voluntary departure)"
    echo "  5) Node Rejoin (recovery after failure)"
    echo "  6) Scale Up (add more nodes)"
    echo ""
    echo "  s) Show cluster status"
    echo "  l) Show logs for a node"
    echo "  f) Follow logs for a node"
    echo "  k) Kill a node (simulate crash)"
    echo "  r) Restart a node"
    echo "  c) Cleanup all nodes"
    echo "  q) Quit"
    echo ""
}

# Interactive mode
interactive_mode() {
    while true; do
        show_menu
        read -p "Select option: " choice

        case $choice in
            1) scenario_basic_cluster ;;
            2) scenario_failure_detection ;;
            3) scenario_suspicion_mechanism ;;
            4) scenario_graceful_leave ;;
            5) scenario_rejoin ;;
            6) scenario_scale_up ;;
            s) show_status ;;
            l)
                read -p "Enter node ID (node1-node5): " node_id
                read -p "Number of lines [20]: " lines
                lines=${lines:-20}
                show_logs "$node_id" "$lines"
                ;;
            f)
                read -p "Enter node ID (node1-node5): " node_id
                follow_logs "$node_id"
                ;;
            k)
                read -p "Enter node ID to kill: " node_id
                kill_node "$node_id"
                ;;
            r)
                read -p "Enter node ID to restart: " node_id
                case $node_id in
                    node1) start_node "node1" 7946 "" ;;
                    node2) start_node "node2" 7947 "127.0.0.1:7946" ;;
                    node3) start_node "node3" 7948 "127.0.0.1:7946" ;;
                    node4) start_node "node4" 7949 "127.0.0.1:7946" ;;
                    node5) start_node "node5" 7950 "127.0.0.1:7946" ;;
                    *) echo -e "${RED}Invalid node ID${NC}" ;;
                esac
                ;;
            c) cleanup_nodes ;;
            q) cleanup; exit 0 ;;
            *) echo -e "${RED}Invalid option${NC}" ;;
        esac

        read -p "Press Enter to continue..."
    done
}

# Start interactive mode
interactive_mode

