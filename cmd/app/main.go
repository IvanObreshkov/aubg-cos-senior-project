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
	clusterSize := 2
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
	pubSub := pubsub.NewPubSub()

	// First pass: create all servers so we can collect their IDs
	var servers []*server.Server
	for _, addr := range addrs {
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

		orch := server.NewOrchestrator(pubSub, srv)
		serverToOrchestratorMap[srv] = orch
	}

	return serverToOrchestratorMap
}

func bootCluster(serverToOrchestratorMap map[*server.Server]*server.Orchestrator, basePort int) {
	i := 0
	for srv, orch := range serverToOrchestratorMap {
		// Start each server in a separate goroutine
		go func(i int, s *server.Server) {
			port := basePort + i
			if err := s.StartServer(port); err != nil {
				log.Printf("Server %v failed to boot due to err: %v", s.ID, err)
			}
		}(i, srv)

		// Start each server orchestrator in a separate goroutine
		go orch.Run()

		i++
		// Small delay between server starts to prevent race conditions
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("Started %d servers and their orchestrators in the Raft cluster", len(serverToOrchestratorMap))
	log.Println("Press Ctrl+C to stop all servers and their orchestrators")
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
