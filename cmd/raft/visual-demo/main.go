package main

import (
	"aubg-cos-senior-project/internal/pubsub"
	"aubg-cos-senior-project/internal/raft/proto"
	"aubg-cos-senior-project/internal/raft/server"
	"aubg-cos-senior-project/internal/raft/state_machine"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	numServers = 3
	basePort   = 50051
	httpPort   = 8080
)

// ServerInfo holds the runtime information about a Raft server
type ServerInfo struct {
	ID                string            `json:"id"`
	Address           string            `json:"address"`
	State             string            `json:"state"`
	Term              uint64            `json:"term"`
	CommitIndex       uint64            `json:"commitIndex"`
	LastApplied       uint64            `json:"lastApplied"`
	LastLogIndex      uint64            `json:"lastLogIndex"`
	LogEntries        []string          `json:"logEntries"`
	IsLeader          bool              `json:"isLeader"`
	RecentEvents      []EventMessage    `json:"recentEvents"`      // Last few events for this server
	StateMachineState map[string]string `json:"stateMachineState"` // Current state machine contents
}

// ClusterState represents the entire cluster state for visualization
type ClusterState struct {
	Servers   []ServerInfo `json:"servers"`
	Timestamp time.Time    `json:"timestamp"`
}

// EventMessage represents a Raft event for the frontend
type EventMessage struct {
	Type      string    `json:"type"`
	ServerID  string    `json:"serverId"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
	Details   any       `json:"details,omitempty"`
}

var (
	servers          []*server.Server
	serverAddrs      []string
	eventsMu         sync.RWMutex
	events           []EventMessage
	maxEvents        = 100
	lastStateMu      sync.RWMutex
	lastServerState  map[string]ServerInfo // Track last known state per server
	serverEventsMu   sync.RWMutex
	serverEvents     map[string][]EventMessage // Per-server event logs
	maxServerEvents  = 10                      // Keep last 10 events per server
	shutdownCtx      context.Context
	shutdownCancel   context.CancelFunc
	serverIDToName   map[server.ServerID]string // Map server UUID to Server-1, Server-2, etc.
	serverIDToNameMu sync.RWMutex
)

func main() {
	autoSubmit := flag.Bool("auto", false, "Automatically submit commands")
	flag.Parse()

	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║     Raft Log Replication Visual Demo                          ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("Starting %d-node Raft cluster...\n", numServers)
	fmt.Println()

	// Initialize state tracking
	lastServerState = make(map[string]ServerInfo)
	serverEvents = make(map[string][]EventMessage)
	serverIDToName = make(map[server.ServerID]string)

	// Create shutdown context for clean goroutine termination
	shutdownCtx, shutdownCancel = context.WithCancel(context.Background())

	// Create cluster
	servers, serverAddrs = createCluster()

	// Create orchestrators for each server
	orchestrators := make([]*server.Orchestrator, numServers)
	for i, srv := range servers {
		orchestrators[i] = server.NewOrchestrator(srv.GetPubSub(), srv)
	}

	// Start ALL servers first so they can communicate
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

	// Small delay to ensure gRPC servers are fully accepting connections
	time.Sleep(100 * time.Millisecond)

	// Start orchestrators which will listen for election timeout events
	for _, orch := range orchestrators {
		go orch.Run()
	}
	log.Printf("Started %d orchestrators", numServers)

	// Small delay to ensure orchestrators are ready to receive events
	time.Sleep(100 * time.Millisecond)

	// NOW start election timers - orchestrators are ready to handle events
	for _, srv := range servers {
		srv.StartElectionTimer()
	}
	log.Printf("Started election timers - cluster is ready")

	// Start HTTP server for visualization immediately
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

	// Print user-facing message immediately
	fmt.Printf("\n✓ Web visualization available at: http://localhost:%d\n", httpPort)
	fmt.Println("\nCommands:")
	fmt.Println("  - Open browser to see visualization")
	fmt.Println("  - Submit commands via web UI")
	fmt.Println("  - Press Ctrl+C to shutdown\n")

	// Wait for cluster to stabilize and elect a leader
	time.Sleep(3 * time.Second)

	// Start background state monitor to detect log replication events
	go monitorLogReplication()

	// Auto-submit commands if requested
	if *autoSubmit {
		go autoSubmitCommands()
	}

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\n╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║ Shutting down cluster...                                      ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Cancel shutdown context to stop all background goroutines
	fmt.Println("⏹  Stopping background monitors...")
	shutdownCancel()

	// Give background goroutines a moment to exit cleanly
	time.Sleep(300 * time.Millisecond)
	fmt.Println("✓ Background monitors stopped")

	// Now shutdown servers
	fmt.Printf("⏹  Shutting down %d Raft servers...\n", len(servers))
	for i, srv := range servers {
		srv.GracefulShutdown()
		fmt.Printf("✓ Server-%d shutdown complete\n", i+1)
	}

	time.Sleep(500 * time.Millisecond)

	fmt.Println()
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║ Cluster shutdown complete. Goodbye! 👋                        ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
}

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

		// Map server UUID to friendly name
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

		// Set peers with addresses
		srv.SetPeersWithAddresses(peers)

		// Set state machine
		srv.StateMachine = state_machine.NewKVStateMachine(string(srv.ID))

		// Subscribe to events for logging
		go subscribeToEvents(srv, i)
	}

	return srvs, addrs
}

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
			addEvent(EventMessage{
				Type:      "election_timeout",
				ServerID:  serverName,
				Message:   fmt.Sprintf("%s election timeout - starting election", serverName),
				Timestamp: time.Now(),
			})
		case event := <-voteGrantedChan:
			voterName := getServerName(event.Payload.From)
			addEvent(EventMessage{
				Type:      "vote_granted",
				ServerID:  serverName,
				Message:   fmt.Sprintf("%s received vote from %s for term %d", serverName, voterName, event.Payload.Term),
				Timestamp: time.Now(),
				Details:   event.Payload,
			})
		case event := <-electionWonChan:
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

func addEvent(event EventMessage) {
	eventsMu.Lock()
	defer eventsMu.Unlock()
	events = append(events, event)
	if len(events) > maxEvents {
		events = events[len(events)-maxEvents:]
	}

	// Also add to per-server event log if it's a server event
	if event.ServerID != "" && event.ServerID != "Client" && event.ServerID != "Auto-Client" {
		serverEventsMu.Lock()
		defer serverEventsMu.Unlock()

		if serverEvents[event.ServerID] == nil {
			serverEvents[event.ServerID] = []EventMessage{}
		}
		serverEvents[event.ServerID] = append(serverEvents[event.ServerID], event)
		if len(serverEvents[event.ServerID]) > maxServerEvents {
			serverEvents[event.ServerID] = serverEvents[event.ServerID][len(serverEvents[event.ServerID])-maxServerEvents:]
		}
	}
}

// getServerName returns the friendly name (Server-1, Server-2) for a server UUID
func getServerName(serverID server.ServerID) string {
	serverIDToNameMu.RLock()
	defer serverIDToNameMu.RUnlock()
	if name, ok := serverIDToName[serverID]; ok {
		return name
	}
	// Fallback to showing truncated UUID
	idStr := string(serverID)
	if len(idStr) > 12 {
		return idStr[:12] + "..."
	}
	return idStr
}

// monitorLogReplication monitors server state changes to detect log replication events
func monitorLogReplication() {
	ticker := time.NewTicker(200 * time.Millisecond) // Check 5 times per second
	defer ticker.Stop()

	for {
		select {
		case <-shutdownCtx.Done():
			// Shutdown signal received, stop monitoring
			return
		case <-ticker.C:
			for i, addr := range serverAddrs {
				serverName := fmt.Sprintf("Server-%d", i+1)
				state := queryServerState(addr)
				if state == nil {
					continue
				}

				currentInfo := ServerInfo{
					ID:           serverName,
					State:        state.State,
					Term:         state.CurrentTerm,
					CommitIndex:  state.CommitIndex,
					LastApplied:  state.LastApplied,
					LastLogIndex: state.LastLogIndex,
				}

				lastStateMu.Lock()
				lastInfo, exists := lastServerState[serverName]
				isLeader := currentInfo.State == "Leader"

				// Detect log append (new entry received)
				if exists && currentInfo.LastLogIndex > lastInfo.LastLogIndex {
					entriesAdded := currentInfo.LastLogIndex - lastInfo.LastLogIndex
					var message string
					var eventType string
					if isLeader {
						// Leader appended command from client
						message = fmt.Sprintf("%s [LEADER] Appended command to log at index %d", serverName, currentInfo.LastLogIndex)
						eventType = "log_appended"
					} else {
						// Follower received AppendEntries RPC from leader
						message = fmt.Sprintf("%s [RPC] Received AppendEntries from leader → appended %d entries (index: %d → %d)",
							serverName, entriesAdded, lastInfo.LastLogIndex, currentInfo.LastLogIndex)
						eventType = "rpc_append_entries"
					}
					addEvent(EventMessage{
						Type:      eventType,
						ServerID:  serverName,
						Message:   message,
						Timestamp: time.Now(),
						Details: map[string]any{
							"oldIndex":   lastInfo.LastLogIndex,
							"newIndex":   currentInfo.LastLogIndex,
							"isLeader":   isLeader,
							"numEntries": entriesAdded,
						},
					})
				}

				// Detect commit (entry committed)
				if exists && currentInfo.CommitIndex > lastInfo.CommitIndex {
					var message string
					if isLeader {
						// Leader committed after receiving majority acknowledgments
						message = fmt.Sprintf("%s [LEADER] ✓ Entry %d is now COMMITTED (majority replicated)",
							serverName, currentInfo.CommitIndex)
					} else {
						// Follower learned about commit from leader's heartbeat
						message = fmt.Sprintf("%s Updated commitIndex from %d to %d (learned from leader)",
							serverName, lastInfo.CommitIndex, currentInfo.CommitIndex)
					}
					addEvent(EventMessage{
						Type:      "entry_committed",
						ServerID:  serverName,
						Message:   message,
						Timestamp: time.Now(),
						Details: map[string]any{
							"oldCommit": lastInfo.CommitIndex,
							"newCommit": currentInfo.CommitIndex,
							"isLeader":  isLeader,
						},
					})
				}

				// Detect state machine application
				if exists && currentInfo.LastApplied > lastInfo.LastApplied {
					entriesApplied := currentInfo.LastApplied - lastInfo.LastApplied
					message := fmt.Sprintf("%s Applied %d log entries to state machine (lastApplied: %d → %d)",
						serverName, entriesApplied, lastInfo.LastApplied, currentInfo.LastApplied)
					addEvent(EventMessage{
						Type:      "entry_applied",
						ServerID:  serverName,
						Message:   message,
						Timestamp: time.Now(),
						Details: map[string]any{
							"oldApplied": lastInfo.LastApplied,
							"newApplied": currentInfo.LastApplied,
							"numApplied": entriesApplied,
						},
					})
				}

				// Detect term change
				if exists && currentInfo.Term > lastInfo.Term {
					message := fmt.Sprintf("%s [TERM-%d→%d] Term increased",
						serverName, lastInfo.Term, currentInfo.Term)
					addEvent(EventMessage{
						Type:      "term_changed",
						ServerID:  serverName,
						Message:   message,
						Timestamp: time.Now(),
						Details: map[string]any{
							"oldTerm": lastInfo.Term,
							"newTerm": currentInfo.Term,
						},
					})
				}

				// Detect state change (Leader/Follower/Candidate)
				if exists && currentInfo.State != lastInfo.State {
					message := fmt.Sprintf("%s Transitioning %s → %s",
						serverName, lastInfo.State, currentInfo.State)
					addEvent(EventMessage{
						Type:      "state_changed",
						ServerID:  serverName,
						Message:   message,
						Timestamp: time.Now(),
						Details: map[string]any{
							"oldState": lastInfo.State,
							"newState": currentInfo.State,
						},
					})
				}

				lastServerState[serverName] = currentInfo
				lastStateMu.Unlock()
			}
		}
	}
}

func handleState(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	var clusterState ClusterState
	clusterState.Timestamp = time.Now()

	for i, addr := range serverAddrs {
		info := ServerInfo{
			ID:      fmt.Sprintf("Server-%d", i+1),
			Address: addr,
		}

		// Query server state
		if state := queryServerState(addr); state != nil {
			info.State = state.State
			info.Term = state.CurrentTerm
			info.CommitIndex = state.CommitIndex
			info.LastApplied = state.LastApplied
			info.LastLogIndex = state.LastLogIndex
			info.IsLeader = state.State == "Leader"

			// Get recent log entries
			if entries := getLogEntries(addr, 5); entries != nil {
				info.LogEntries = entries
			}

			// Get state machine contents
			if smState := getStateMachineState(i); smState != nil {
				info.StateMachineState = smState
			}

			// Get recent events for this server
			serverEventsMu.RLock()
			if events, ok := serverEvents[info.ID]; ok {
				// Return events in reverse order (newest first)
				reversed := make([]EventMessage, len(events))
				for i, e := range events {
					reversed[len(events)-1-i] = e
				}
				info.RecentEvents = reversed
			}
			serverEventsMu.RUnlock()
		} else {
			info.State = "Unreachable"
		}

		clusterState.Servers = append(clusterState.Servers, info)
	}

	_ = json.NewEncoder(w).Encode(clusterState)
}

func handleEvents(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	eventsMu.RLock()
	defer eventsMu.RUnlock()

	_ = json.NewEncoder(w).Encode(events)
}

func handleSubmit(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Command string `json:"command"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Find leader
	leaderAddr := findLeader()
	if leaderAddr == "" {
		json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"error":   "No leader found",
		})
		return
	}

	// Submit command
	success, index := submitCommand(leaderAddr, req.Command)
	if success {
		addEvent(EventMessage{
			Type:      "command_submitted",
			ServerID:  "Client",
			Message:   fmt.Sprintf("Command submitted: %s (index: %d)", req.Command, index),
			Timestamp: time.Now(),
			Details:   map[string]any{"command": req.Command, "index": index},
		})
	}

	json.NewEncoder(w).Encode(map[string]any{
		"success": success,
		"index":   index,
		"leader":  leaderAddr,
	})
}

func handleShutdownServer(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ServerIndex int `json:"serverIndex"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.ServerIndex < 0 || req.ServerIndex >= len(servers) {
		json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"error":   "Invalid server index",
		})
		return
	}

	servers[req.ServerIndex].GracefulShutdown()
	addEvent(EventMessage{
		Type:      "server_shutdown",
		ServerID:  fmt.Sprintf("Server-%d", req.ServerIndex+1),
		Message:   fmt.Sprintf("Server-%d shut down", req.ServerIndex+1),
		Timestamp: time.Now(),
	})

	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
	})
}

func queryServerState(addr string) *proto.GetServerStateResponse {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	defer conn.Close()

	client := proto.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resp, err := client.GetServerState(ctx, &proto.GetServerStateRequest{})
	if err != nil {
		return nil
	}

	return resp
}

func getLogEntries(addr string, count int) []string {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	defer conn.Close()

	client := proto.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Get server state first to know the log size
	state, err := client.GetServerState(ctx, &proto.GetServerStateRequest{})
	if err != nil || state.LastLogIndex == 0 {
		return []string{}
	}

	// Get recent entries
	var entries []string
	startIndex := uint64(1)
	if state.LastLogIndex > uint64(count) {
		startIndex = state.LastLogIndex - uint64(count) + 1
	}

	for i := startIndex; i <= state.LastLogIndex; i++ {
		entries = append(entries, fmt.Sprintf("Entry %d", i))
	}

	return entries
}

func getStateMachineState(serverIndex int) map[string]string {
	if serverIndex < 0 || serverIndex >= len(servers) {
		return nil
	}

	srv := servers[serverIndex]
	if srv == nil || srv.StateMachine == nil {
		return nil
	}

	// Type assert to KVStateMachine to access the Get method
	kvSM, ok := srv.StateMachine.(*state_machine.KVStateMachine)
	if !ok {
		return nil
	}

	// Get all key-value pairs from the state machine
	return kvSM.GetAll()
}

func findLeader() string {
	for _, addr := range serverAddrs {
		if state := queryServerState(addr); state != nil && state.State == "Leader" {
			return addr
		}
	}
	return ""
}

func submitCommand(addr string, cmd string) (bool, uint64) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false, 0
	}
	defer conn.Close()

	client := proto.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.ClientCommand(ctx, &proto.ClientCommandRequest{
		Command: []byte(cmd),
	})
	if err != nil {
		return false, 0
	}

	return resp.Success, resp.Index
}

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

func serveHTML(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "cmd/raft/visual-demo/index.html")
}
