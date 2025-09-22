package main

import (
	"aubg-cos-senior-project/internal/raft"
	"aubg-cos-senior-project/internal/raft/server"
	"fmt"
	"log"
	"sync"
	"time"
)

func main() {
	clusterSize := 2
	basePort := 50051

	// TODO: Maybe impl service discovery, to make port binding dynamic
	var allPeers []raft.ServerAddress
	for i := 0; i < clusterSize; i++ {
		addr := fmt.Sprintf("localhost:%d", basePort+i)
		allPeers = append(allPeers, raft.ServerAddress(addr))
	}

	var wg sync.WaitGroup
	servers := make([]*server.Server, clusterSize)

	for i := 0; i < clusterSize; i++ {
		var peers []raft.ServerAddress
		for j, addr := range allPeers {
			// Create peers list (all servers except current one)
			if j != i {
				peers = append(peers, addr)
			}
		}

		servers[i] = server.NewServer(0, peers)

		// Start each server in a separate goroutine, in order to not block the main goroutine
		wg.Add(1)
		go func(serverIndex int, raftNode *server.Server) {
			defer wg.Done()

			port := basePort + serverIndex
			if err := raftNode.StartServer(port); err != nil {
				log.Printf("Server %v failed: %v", raftNode.ID, err)
			}
		}(i, servers[i])

		// Small delay between server starts
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("Started %d servers in the Raft cluster", clusterSize)
	log.Println("Press Ctrl+C to stop all servers")

	// Wait for all servers to finish initialization
	wg.Wait()
}
