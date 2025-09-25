package main

import (
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

	// TODO: Maybe impl service discovery, to make port binding dynamic
	reservedAddresses := reserveAddresses(clusterSize, basePort)
	servers := createCluster(clusterSize, reservedAddresses)

	// Create a done channel to signal when the shutdown is complete
	done := make(chan bool, 1)

	// Start graceful shutdown monitoring before bootCluster blocks
	go listenForShutdown(servers, done)

	// This will block indefinitely until shutdown is triggered
	bootCluster(servers, basePort)

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

func createCluster(clusterSize int, reservedAddresses []server.ServerAddress) []*server.Server {
	servers := make([]*server.Server, clusterSize)

	for i := 0; i < clusterSize; i++ {
		var peers []server.ServerAddress
		for j, addr := range reservedAddresses {
			// Create peers list (all servers except current one)
			if j != i {
				peers = append(peers, addr)
			}
		}

		servers[i] = server.NewServer(0, peers)
	}

	return servers
}

func bootCluster(servers []*server.Server, basePort int) {
	for i, raftServer := range servers {
		// Start each server in a separate goroutine, as each one makes a blocking call to wait for new incoming TCP
		// connections, which will block the main goroutine itself
		go func(i int, s *server.Server) {
			port := basePort + i
			if err := s.StartServer(port); err != nil {
				log.Printf("Server %v failed to boot due to err: %v", s.ID, err)
			}
		}(i, raftServer)

		// Small delay between server starts to prevent race conditions
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("Started %d servers in the Raft cluster", len(servers))
	log.Println("Press Ctrl+C to stop all servers")
}

func listenForShutdown(servers []*server.Server, done chan bool) {
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

	gracefulShutdownDone := gracefullyShutdownCluster(servers)

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

func gracefullyShutdownCluster(servers []*server.Server) chan struct{} {
	// Create a WaitGroup to wait for all servers to shut down
	var gracefulShutdownWget sync.WaitGroup

	// Gracefully Shutdown all servers in a concurrent manner
	for _, raftServer := range servers {
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
