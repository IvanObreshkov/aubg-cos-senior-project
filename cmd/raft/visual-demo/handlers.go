package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// handleState returns the current state of all servers in the cluster.
// This is the main API endpoint used by the web UI to refresh the display.
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

		// Query server state via gRPC
		if state := queryServerState(addr); state != nil {
			info.State = state.State
			info.Term = state.CurrentTerm
			info.CommitIndex = state.CommitIndex
			info.LastApplied = state.LastApplied
			info.LastLogIndex = state.LastLogIndex
			info.IsLeader = state.State == "Leader"

			// Get recent log entries for display
			if entries := getLogEntries(addr, 5); entries != nil {
				info.LogEntries = entries
			}

			// Get state machine contents (KV store)
			if smState := getStateMachineState(i); smState != nil {
				info.StateMachineState = smState
			}

			// Get recent events for this specific server
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

// handleEvents returns the global event log (all events from all servers).
// This endpoint is currently not used by the UI but available for debugging.
func handleEvents(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	eventsMu.RLock()
	defer eventsMu.RUnlock()

	_ = json.NewEncoder(w).Encode(events)
}

// handleSubmit handles command submission from the web UI.
// It finds the current leader and forwards the command to it.
func handleSubmit(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse command from request body
	var req struct {
		Command string `json:"command"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Find the current leader
	leaderAddr := findLeader()
	if leaderAddr == "" {
		json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"error":   "No leader found",
		})
		return
	}

	// Submit command to leader
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

// handleShutdownServer gracefully shuts down a specific server (for testing/demo).
// This is useful for demonstrating fault tolerance and leader re-election.
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

// serveHTML serves the main HTML page for the web UI.
func serveHTML(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "cmd/raft/visual-demo/static/index.html")
}
