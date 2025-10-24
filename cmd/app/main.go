package main

import (
	"aubg-cos-senior-project/internal/pubsub"
	"aubg-cos-senior-project/internal/raft/server"
	"context"
	"fmt"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	clusterSize := 3
	basePort := 50051

	// Reserve addresses for the cluster
	addrs := reserveAddresses(clusterSize, basePort)

	// Create all servers and their orchestrators in the cluster
	serverOrchestratorMap := createCluster(addrs)

	// Create a done channel to signal when the shutdown is complete
	done := make(chan bool, 1)

	// Start graceful shutdown monitoring before bootCluster blocks
	go listenForShutdown(serverOrchestratorMap, done)

	// This will block indefinitely until shutdown is triggered
	bootCluster(serverOrchestratorMap, basePort)

	// Wait for the graceful shutdown to complete
	<-done
}

func reserveAddresses(clusterSize int, basePort int) []server.ServerAddress {
	var allPeers []server.ServerAddress

	for i := 0; i < clusterSize; i++ {
		addr := fmt.Sprintf("localhost:%d", basePort+i)
		allPeers = append(allPeers, server.ServerAddress(addr))
	}

	return allPeers
}

func createCluster(addrs []server.ServerAddress) map[*server.Server]*server.Orchestrator {
	serverToOrchestratorMap := make(map[*server.Server]*server.Orchestrator)

	// First pass: create all servers so we can collect their IDs
	// IMPORTANT: Each server gets its OWN PubSub instance to prevent cross-server event pollution
	var servers []*server.Server
	for _, addr := range addrs {
		pubSub := pubsub.NewPubSub() // Create separate PubSub for each server
		srv := server.NewServer(0, addr, nil, pubSub)
		servers = append(servers, srv)
	}

	// Second pass: update each server with peer IDs (all servers except itself)
	for i, srv := range servers {
		var peerIDs []server.ServerID
		for j, otherSrv := range servers {
			// Exclude current server
			if j != i {
				peerIDs = append(peerIDs, otherSrv.ID)
			}
		}
		// Update the peers list for this server
		srv.SetPeers(peerIDs)

		// Create orchestrator using the server's own PubSub instance
		orch := server.NewOrchestrator(srv.GetPubSub(), srv)
		serverToOrchestratorMap[srv] = orch
	}

	return serverToOrchestratorMap
}

func bootCluster(serverToOrchestratorMap map[*server.Server]*server.Orchestrator, basePort int) {
	var wg sync.WaitGroup

	// Create deterministic port assignments by collecting servers into a slice first
	servers := make([]*server.Server, 0, len(serverToOrchestratorMap))
	for srv := range serverToOrchestratorMap {
		servers = append(servers, srv)
	}

	// Start ALL servers first (without orchestrators) so they can all communicate
	for i, srv := range servers {
		wg.Add(1)
		port := basePort + i
		go func(s *server.Server, p int, idx int) {
			defer wg.Done()
			log.Printf("Starting server %d on port %d", idx, p)
			if err := s.StartServer(p); err != nil {
				log.Printf("Server %v failed to boot due to err: %v", s.ID, err)
			}
		}(srv, port, i)
	}

	// Wait for all servers to be listening
	wg.Wait()
	log.Printf("All %d servers are now listening", len(serverToOrchestratorMap))

	// Small delay to ensure gRPC servers are fully accepting connections
	time.Sleep(100 * time.Millisecond)

	// Start orchestrators which will listen for election timeout events
	for _, orch := range serverToOrchestratorMap {
		go orch.Run()
	}
	log.Printf("Started %d orchestrators", len(serverToOrchestratorMap))

	// Small delay to ensure orchestrators are ready to receive events
	time.Sleep(100 * time.Millisecond)

	// NOW start election timers - orchestrators are ready to handle events
	for _, srv := range servers {
		srv.StartElectionTimer()
	}
	log.Printf("Started election timers - cluster is ready")
}

func listenForShutdown(serverToOrchestratorMap map[*server.Server]*server.Orchestrator, done chan bool) {
	// Create context that listens for the interrupt signal from the OS.
	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Block the thread until an interrupt signal is received.
	<-signalCtx.Done()

	log.Println("Shutting down gracefully, press Ctrl+C again to force")
	stop() // Disable signal handler so second Ctrl+C will force immediate exit of the process via the OS

	// Create a separate context with timeout for the graceful shutdown process. All servers have 5 seconds to finish
	// the request they are currently handling
	forceShutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var servers []*server.Server
	for srv := range serverToOrchestratorMap {
		servers = append(servers, srv)
	}

	gracefulShutdownDone := gracefullyShutdownCluster(serverToOrchestratorMap)

	// Race the shutdown completion against the timeout
	select {
	case <-gracefulShutdownDone:
		log.Println("All servers shutdown gracefully")
	case <-forceShutdownCtx.Done():
		log.Println("Graceful shutdown timeout reached, forcing shutdown...")

		// Force shutdown all servers that haven't stopped yet
		for _, raftServer := range servers {
			go raftServer.ForceShutdown()
		}

		// Give force shutdown a brief moment to complete
		time.Sleep(500 * time.Millisecond)
		log.Println("Force shutdown complete")
	}

	log.Println("Cluster exiting")
	done <- true
}

func gracefullyShutdownCluster(serverToOrchestratorMap map[*server.Server]*server.Orchestrator) chan struct{} {
	// Create a WaitGroup to wait for all servers to shut down
	var gracefulShutdownWget sync.WaitGroup

	// Gracefully Shutdown all servers in a concurrent manner
	for raftServer := range serverToOrchestratorMap {
		gracefulShutdownWget.Add(1)
		go func(s *server.Server) {
			defer gracefulShutdownWget.Done()
			s.GracefulShutdown()
		}(raftServer)
	}

	// Convert the blocking WaitGroup.Wait() to a channel signal
	gracefulShutdownDone := make(chan struct{})
	go func() {
		gracefulShutdownWget.Wait()
		close(gracefulShutdownDone)
	}()

	return gracefulShutdownDone
}
