package main

import (
	"aubg-cos-senior-project/internal/pubsub"
	"aubg-cos-senior-project/internal/raft/server"
	"aubg-cos-senior-project/internal/raft/state_machine"
	"fmt"
	"log"
	"time"
)

// createCluster initializes and returns a 3-node Raft cluster.
// It creates all servers, establishes peer connections, and sets up state machines.
func createCluster() ([]*server.Server, []string) {
	var srvs []*server.Server
	var addrs []string

	// First pass: create addresses
	for i := 0; i < numServers; i++ {
		addrs = append(addrs, fmt.Sprintf("localhost:%d", basePort+i))
	}

	// Second pass: create all servers so we can collect their IDs
	serverAddresses := make(map[server.ServerID]server.ServerAddress)
	for i, addr := range addrs {
		pubSubClient := pubsub.NewPubSub()
		srv := server.NewServer(0, server.ServerAddress(addr), nil, pubSubClient)
		srvs = append(srvs, srv)
		serverAddresses[srv.ID] = server.ServerAddress(addr)

		// Map server UUID to friendly name (Server-1, Server-2, etc.)
		serverIDToNameMu.Lock()
		serverIDToName[srv.ID] = fmt.Sprintf("Server-%d", i+1)
		serverIDToNameMu.Unlock()
	}

	// Third pass: update each server with peer information
	for i, srv := range srvs {
		// Build peer map (everyone except self)
		peers := make(map[server.ServerID]server.ServerAddress)
		for id, addr := range serverAddresses {
			if id != srv.ID {
				peers[id] = addr
			}
		}

		// Set peers with addresses for Raft communication
		srv.SetPeersWithAddresses(peers)

		// Set state machine (replicated KV store)
		srv.StateMachine = state_machine.NewKVStateMachine(string(srv.ID))

		// Subscribe to Raft events for visualization
		go subscribeToEvents(srv, i)
	}

	return srvs, addrs
}

// subscribeToEvents listens for Raft consensus events and records them for visualization.
// This goroutine runs for the lifetime of the server.
func subscribeToEvents(srv *server.Server, idx int) {
	electionTimeoutChan := make(chan *pubsub.Event[time.Time], 10)
	voteGrantedChan := make(chan *pubsub.Event[server.VoteGrantedPayload], 10)
	electionWonChan := make(chan *pubsub.Event[uint64], 10)

	ps := srv.GetPubSub()
	pubsub.Subscribe(ps, server.ElectionTimeoutExpired, electionTimeoutChan, pubsub.SubscriptionOptions{IsBlocking: false})
	pubsub.Subscribe(ps, server.VoteGranted, voteGrantedChan, pubsub.SubscriptionOptions{IsBlocking: false})
	pubsub.Subscribe(ps, server.ElectionWon, electionWonChan, pubsub.SubscriptionOptions{IsBlocking: false})

	serverName := fmt.Sprintf("Server-%d", idx+1)
	for {
		select {
		case <-electionTimeoutChan:
			// Election timeout expired - server is starting an election
			addEvent(EventMessage{
				Type:      "election_timeout",
				ServerID:  serverName,
				Message:   fmt.Sprintf("%s election timeout - starting election", serverName),
				Timestamp: time.Now(),
			})
		case event := <-voteGrantedChan:
			// Server received a vote from another server
			voterName := getServerName(event.Payload.From)
			addEvent(EventMessage{
				Type:      "vote_granted",
				ServerID:  serverName,
				Message:   fmt.Sprintf("%s received vote from %s for term %d", serverName, voterName, event.Payload.Term),
				Timestamp: time.Now(),
				Details:   event.Payload,
			})
		case event := <-electionWonChan:
			// Server won the election and became leader
			addEvent(EventMessage{
				Type:      "election_won",
				ServerID:  serverName,
				Message:   fmt.Sprintf("%s won election for term %d", serverName, event.Payload),
				Timestamp: time.Now(),
				Details:   map[string]uint64{"term": event.Payload},
			})
		}
	}
}

// autoSubmitCommands automatically submits test commands for demo purposes.
// This is used when the -auto flag is provided.
func autoSubmitCommands() {
	time.Sleep(5 * time.Second) // Wait for leader election

	commands := []string{
		"SET user=alice",
		"SET role=admin",
		"SET location=sofia",
		"SET language=go",
		"SET protocol=raft",
	}

	for i, cmd := range commands {
		time.Sleep(3 * time.Second)
		log.Printf("Auto-submitting command %d: %s", i+1, cmd)

		leaderAddr := findLeader()
		if leaderAddr != "" {
			success, index := submitCommand(leaderAddr, cmd)
			if success {
				addEvent(EventMessage{
					Type:      "command_submitted",
					ServerID:  "Auto-Client",
					Message:   fmt.Sprintf("Auto-submitted: %s (index: %d)", cmd, index),
					Timestamp: time.Now(),
				})
			}
		}
	}
}
