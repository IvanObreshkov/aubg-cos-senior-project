package main

import (
	"encoding/json"
	"net/http"
	"time"
)

// handleState returns the current state of all nodes in the cluster.
// This is the main API endpoint used by the web UI to refresh the display.
func handleState(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	var clusterState ClusterState
	clusterState.Timestamp = time.Now()

	for i, node := range nodes {
		nodeName := getNodeNameByIndex(i)
		info := getNodeInfo(node, nodeName, nodeConfigs[i], i)
		clusterState.Nodes = append(clusterState.Nodes, info)
	}

	_ = json.NewEncoder(w).Encode(clusterState)
}

// handleEvents returns the global event log (all events from all nodes).
func handleEvents(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	eventsMu.RLock()
	defer eventsMu.RUnlock()

	_ = json.NewEncoder(w).Encode(events)
}

// handleBroadcast handles message broadcast requests from the UI.
func handleBroadcast(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		NodeIndex int    `json:"nodeIndex"` // Which node to broadcast from
		Message   string `json:"message"`   // Message to broadcast
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := broadcastMessage(req.NodeIndex, req.Message); err != nil {
		json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
	})
}

// serveHTML serves the main HTML page for the web UI.
func serveHTML(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "cmd/tob/visual-demo/index.html")
}
