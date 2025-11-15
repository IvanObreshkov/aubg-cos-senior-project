package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// main is the entry point for the SWIM visual demonstration.
// It creates a 5-node SWIM cluster and starts a web server for visualization.
func main() {
	autoFail := flag.Bool("fail", false, "Automatically simulate node failures for demo")
	flag.Parse()

	printBanner()
	initializeGlobalState()

	// Create and start the SWIM cluster
	nodes, nodeAddrs, nodeConfigs = createCluster()
	startCluster()

	// Wait for cluster to stabilize
	time.Sleep(5 * time.Second)

	// Start web server for visualization
	startWebServer()

	// Auto-simulate failures if requested
	if *autoFail {
		go autoSimulateFailures()
	}

	// Wait for shutdown signal
	waitForShutdown()
}

// printBanner displays the startup banner.
func printBanner() {
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║     SWIM Membership & Failure Detection Visualization         ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("Starting %d-node SWIM cluster...\n", numNodes)
	fmt.Println()
}

// initializeGlobalState initializes all global state variables.
func initializeGlobalState() {
	lastNodeState = make(map[string]NodeInfo)
	nodeEvents = make(map[string][]EventMessage)
	nodeIDToName = make(map[string]string)
	shutdownCtx, shutdownCancel = context.WithCancel(context.Background())
}

// startCluster starts all SWIM nodes.
func startCluster() {
	// Start all nodes
	for i, node := range nodes {
		log.Printf("Starting node %d on %s", i+1, nodeAddrs[i])
		if err := node.Start(); err != nil {
			log.Fatalf("Node %d failed to start: %v", i+1, err)
		}
	}

	log.Printf("All %d nodes are now running", numNodes)
	time.Sleep(2 * time.Second)

	// Nodes 2-5 will automatically join node 1 (configured in cluster.go)
	log.Printf("Nodes joining cluster...")
}

// startWebServer starts the HTTP server for the web UI.
func startWebServer() {
	// Serve static files (CSS, JS)
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("cmd/swim/visual-demo/static"))))

	// API endpoints
	http.HandleFunc("/", serveHTML)
	http.HandleFunc("/api/state", handleState)
	http.HandleFunc("/api/events", handleEvents)
	http.HandleFunc("/api/crash-node", handleCrashNode)

	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", httpPort), nil); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	fmt.Printf("\n✓ Web visualization available at: http://localhost:%d\n", httpPort)
	fmt.Println("\nCommands:")
	fmt.Println("  - Open browser to see visualization")
	fmt.Println("  - Observe membership and failure detection")
	fmt.Println("  - Simulate node crashes via web UI")
	fmt.Println("  - Press Ctrl+C to shutdown\n")
}

// autoSimulateFailures automatically simulates node failures for demo purposes.
func autoSimulateFailures() {
	time.Sleep(15 * time.Second)

	// Crash node 3
	log.Println("Auto-demo: Simulating failure of Node-3...")
	simulateNodeFailure(2)

	time.Sleep(20 * time.Second)

	// Crash node 5
	log.Println("Auto-demo: Simulating failure of Node-5...")
	simulateNodeFailure(4)
}

// waitForShutdown waits for interrupt signal and performs graceful shutdown.
func waitForShutdown() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	printShutdownBanner()
	performShutdown()
	printGoodbye()
}

// printShutdownBanner displays the shutdown banner.
func printShutdownBanner() {
	fmt.Println("\n╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║ Shutting down cluster...                                      ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()
}

// performShutdown gracefully shuts down all components.
func performShutdown() {
	// Cancel shutdown context
	fmt.Println("⏹  Stopping background monitors...")
	shutdownCancel()
	time.Sleep(300 * time.Millisecond)
	fmt.Println("✓ Background monitors stopped")

	// Shutdown all nodes
	fmt.Printf("⏹  Shutting down %d SWIM nodes...\n", len(nodes))
	for i, node := range nodes {
		if node != nil {
			node.Stop()
			fmt.Printf("✓ Node-%d shutdown complete\n", i+1)
		}
	}

	time.Sleep(500 * time.Millisecond)
}

// printGoodbye displays the final goodbye message.
func printGoodbye() {
	fmt.Println()
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║ Cluster shutdown complete. Goodbye! 👋                        ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
}
