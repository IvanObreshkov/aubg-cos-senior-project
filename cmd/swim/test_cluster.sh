#!/bin/bash

# SWIM Protocol Test Script
# This script demonstrates the SWIM protocol with multiple nodes

echo "=========================================="
echo "SWIM Protocol Demonstration"
echo "=========================================="
echo ""
echo "This script will start 3 SWIM nodes:"
echo "  - Node 1 (seed):  127.0.0.1:7946"
echo "  - Node 2:         127.0.0.1:7947"
echo "  - Node 3:         127.0.0.1:7948"
echo ""
echo "Instructions:"
echo "  1. Watch the nodes discover each other"
echo "  2. Press Ctrl+C in any node terminal to simulate failure"
echo "  3. Observe failure detection in other nodes"
echo "  4. Restart the failed node to see it rejoin"
echo ""
echo "Opening terminals in 3 seconds..."
sleep 3

# Check if we're on macOS
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS - use osascript to open new Terminal windows

    # Start Node 1 (seed node)
    osascript -e 'tell application "Terminal"
        do script "cd '"$(pwd)"' && echo \"Starting Node 1 (seed node)...\" && ./bin/swim-demo -id=node1 -bind=127.0.0.1:7946"
    end tell'

    sleep 2

    # Start Node 2
    osascript -e 'tell application "Terminal"
        do script "cd '"$(pwd)"' && echo \"Starting Node 2...\" && ./bin/swim-demo -id=node2 -bind=127.0.0.1:7947 -join=127.0.0.1:7946"
    end tell'

    sleep 2

    # Start Node 3
    osascript -e 'tell application "Terminal"
        do script "cd '"$(pwd)"' && echo \"Starting Node 3...\" && ./bin/swim-demo -id=node3 -bind=127.0.0.1:7948 -join=127.0.0.1:7946"
    end tell'

    echo "✓ Opened 3 terminal windows with SWIM nodes"
    echo ""
    echo "To stop all nodes, press Ctrl+C in each terminal"

elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux - try to use gnome-terminal or xterm
    if command -v gnome-terminal &> /dev/null; then
        gnome-terminal -- bash -c "cd $(pwd) && echo 'Starting Node 1 (seed node)...' && ./bin/swim-demo -id=node1 -bind=127.0.0.1:7946; exec bash"
        sleep 2
        gnome-terminal -- bash -c "cd $(pwd) && echo 'Starting Node 2...' && ./bin/swim-demo -id=node2 -bind=127.0.0.1:7947 -join=127.0.0.1:7946; exec bash"
        sleep 2
        gnome-terminal -- bash -c "cd $(pwd) && echo 'Starting Node 3...' && ./bin/swim-demo -id=node3 -bind=127.0.0.1:7948 -join=127.0.0.1:7946; exec bash"
    elif command -v xterm &> /dev/null; then
        xterm -e "cd $(pwd) && ./bin/swim-demo -id=node1 -bind=127.0.0.1:7946" &
        sleep 2
        xterm -e "cd $(pwd) && ./bin/swim-demo -id=node2 -bind=127.0.0.1:7947 -join=127.0.0.1:7946" &
        sleep 2
        xterm -e "cd $(pwd) && ./bin/swim-demo -id=node3 -bind=127.0.0.1:7948 -join=127.0.0.1:7946" &
    else
        echo "No suitable terminal emulator found. Please run nodes manually:"
        echo ""
        echo "Terminal 1: ./bin/swim-demo -id=node1 -bind=127.0.0.1:7946"
        echo "Terminal 2: ./bin/swim-demo -id=node2 -bind=127.0.0.1:7947 -join=127.0.0.1:7946"
        echo "Terminal 3: ./bin/swim-demo -id=node3 -bind=127.0.0.1:7948 -join=127.0.0.1:7946"
    fi
else
    echo "Unsupported OS. Please run nodes manually in separate terminals:"
    echo ""
    echo "Terminal 1: ./bin/swim-demo -id=node1 -bind=127.0.0.1:7946"
    echo "Terminal 2: ./bin/swim-demo -id=node2 -bind=127.0.0.1:7947 -join=127.0.0.1:7946"
    echo "Terminal 3: ./bin/swim-demo -id=node3 -bind=127.0.0.1:7948 -join=127.0.0.1:7946"
fi

