package main

import (
	"aubg-cos-senior-project/internal/raft/server"
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// main is the entry point for the Raft visual demonstration.
// It creates a 3-node Raft cluster and starts a web server for visualization.
func main() {
	autoSubmit := flag.Bool("auto", false, "Automatically submit commands for demo")
	flag.Parse()

	printBanner()
	initializeGlobalState()

	// Create and start the Raft cluster
	servers, serverAddrs = createCluster()
	startCluster()

	// Start web server for visualization
	startWebServer()

	// Wait for cluster to stabilize
	time.Sleep(3 * time.Second)

	// Start background monitoring
	go monitorLogReplication()

	// Auto-submit commands if requested
	if *autoSubmit {
		go autoSubmitCommands()
	}

	// Wait for shutdown signal
	waitForShutdown()
}

// printBanner displays the startup banner.
func printBanner() {
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║     Raft Log Replication Visual Demo                          ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("Starting %d-node Raft cluster...\n", numServers)
	fmt.Println()
}

// initializeGlobalState initializes all global state variables.
func initializeGlobalState() {
	lastServerState = make(map[string]ServerInfo)
	serverEvents = make(map[string][]EventMessage)
	serverIDToName = make(map[server.ServerID]string)
	shutdownCtx, shutdownCancel = context.WithCancel(context.Background())
}

// startCluster starts all Raft servers and their orchestrators.
func startCluster() {
	// Create orchestrators for election management
	orchestrators := make([]*server.Orchestrator, numServers)
	for i, srv := range servers {
		orchestrators[i] = server.NewOrchestrator(srv.GetPubSub(), srv)
	}

	// Start all servers in parallel
	var wg sync.WaitGroup
	for i, srv := range servers {
		wg.Add(1)
		port := basePort + i
		go func(s *server.Server, p int, idx int) {
			defer wg.Done()
			log.Printf("Starting server %d on port %d", idx+1, p)
			if err := s.StartServer(p); err != nil {
				log.Printf("Server %d failed to start: %v", idx+1, err)
			}
		}(srv, port, i)
	}

	// Wait for all servers to be listening
	wg.Wait()
	log.Printf("All %d servers are now listening", numServers)
	time.Sleep(100 * time.Millisecond)

	// Start orchestrators
	for _, orch := range orchestrators {
		go orch.Run()
	}
	log.Printf("Started %d orchestrators", numServers)
	time.Sleep(100 * time.Millisecond)

	// Start election timers
	for _, srv := range servers {
		srv.StartElectionTimer()
	}
	log.Printf("Started election timers - cluster is ready")
}

// startWebServer starts the HTTP server for the web UI.
func startWebServer() {
	// Serve static files (CSS, JS)
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("cmd/raft/visual-demo/static"))))

	// API endpoints
	http.HandleFunc("/", serveHTML)
	http.HandleFunc("/api/state", handleState)
	http.HandleFunc("/api/events", handleEvents)
	http.HandleFunc("/api/submit", handleSubmit)
	http.HandleFunc("/api/shutdown-server", handleShutdownServer)

	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", httpPort), nil); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	fmt.Printf("\n✓ Web visualization available at: http://localhost:%d\n", httpPort)
	fmt.Println("\nCommands:")
	fmt.Println("  - Open browser to see visualization")
	fmt.Println("  - Submit commands via web UI")
	fmt.Println("  - Press Ctrl+C to shutdown\n")
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
	// Stop background goroutines
	fmt.Println("⏹  Stopping background monitors...")
	shutdownCancel()
	time.Sleep(300 * time.Millisecond)
	fmt.Println("✓ Background monitors stopped")

	// Shutdown all servers
	fmt.Printf("⏹  Shutting down %d Raft servers...\n", len(servers))
	for i, srv := range servers {
		srv.GracefulShutdown()
		fmt.Printf("✓ Server-%d shutdown complete\n", i+1)
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
