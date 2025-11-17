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

// main is the entry point for the TOB visual demonstration.
// It creates a 4-node TOB cluster (1 sequencer + 3 regular nodes) and starts a web server for visualization.
func main() {
	autoBroadcast := flag.Bool("auto", false, "Automatically broadcast messages for demo")
	flag.Parse()

	printBanner()
	initializeGlobalState()

	// Create and start the TOB cluster
	nodes, nodeAddrs, nodeConfigs = createCluster()
	startCluster()

	// Wait for cluster to stabilize
	time.Sleep(2 * time.Second)

	// Start web server for visualization
	startWebServer()

	// Auto-broadcast messages if requested
	if *autoBroadcast {
		go autoBroadcastMessages()
	}

	// Wait for shutdown signal
	waitForShutdown()
}

// printBanner displays the startup banner.
func printBanner() {
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║   Total Order Broadcast (Fixed Sequencer) Visualization       ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("Starting %d-node TOB cluster (1 Sequencer + %d Nodes)...\n", numNodes, numNodes-1)
	fmt.Println()
}

// initializeGlobalState initializes all global state variables.
func initializeGlobalState() {
	messagesSent = make(map[string]int)
	messagesDelivered = make(map[string]int)
	deliveredMessages = make(map[string][]MessageInfo)
	nodeEvents = make(map[string][]EventMessage)
	nodeIDToName = make(map[string]string)
	shutdownCtx, shutdownCancel = context.WithCancel(context.Background())
}

// startCluster starts all TOB nodes.
func startCluster() {
	// Start all nodes
	for i, node := range nodes {
		nodeName := getNodeNameByIndex(i)
		log.Printf("Starting %s on %s", nodeName, nodeAddrs[i])
		if err := node.Start(); err != nil {
			log.Fatalf("%s failed to start: %v", nodeName, err)
		}
	}

	log.Printf("All %d nodes are now running", numNodes)
	log.Printf("Sequencer is on port %d", basePort)
}

// startWebServer starts the HTTP server for the web UI.
func startWebServer() {
	// Serve static files (CSS, JS)
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("cmd/tob/visual-demo/static"))))

	// API endpoints
	http.HandleFunc("/", serveHTML)
	http.HandleFunc("/api/state", handleState)
	http.HandleFunc("/api/events", handleEvents)
	http.HandleFunc("/api/broadcast", handleBroadcast)

	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", httpPort), nil); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	fmt.Printf("\n✓ Web visualization available at: http://localhost:%d\n", httpPort)
	fmt.Println("\nCommands:")
	fmt.Println("  - Open browser to see visualization")
	fmt.Println("  - Broadcast messages via web UI")
	fmt.Println("  - Watch total order delivery across all nodes")
	fmt.Println("  - Press Ctrl+C to shutdown\n")
}

// autoBroadcastMessages automatically broadcasts messages for demo purposes.
func autoBroadcastMessages() {
	messages := []string{
		"First message",
		"Second message",
		"Third message",
		"Fourth message",
		"Fifth message",
	}

	time.Sleep(5 * time.Second)

	for i, msg := range messages {
		nodeIndex := (i % (numNodes - 1)) + 1 // Rotate through non-sequencer nodes
		nodeName := getNodeNameByIndex(nodeIndex)
		log.Printf("Auto-demo: %s broadcasting: %s", nodeName, msg)
		if err := broadcastMessage(nodeIndex, msg); err != nil {
			log.Printf("Auto-demo failed: %v", err)
		}
		time.Sleep(3 * time.Second)
	}
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
	fmt.Printf("⏹  Shutting down %d TOB nodes...\n", len(nodes))
	for i, node := range nodes {
		if node != nil {
			nodeName := getNodeNameByIndex(i)
			_ = node.Stop()
			fmt.Printf("✓ %s shutdown complete\n", nodeName)
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
