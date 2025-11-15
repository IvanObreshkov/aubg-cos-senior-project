package main

import (
	"aubg-cos-senior-project/internal/raft/server"
	"context"
	"sync"
	"time"
)

// ServerInfo holds the runtime information about a Raft server for visualization.
type ServerInfo struct {
	ID                string            `json:"id"`                // Friendly name (Server-1, Server-2, etc.)
	Address           string            `json:"address"`           // Server address (localhost:50051)
	State             string            `json:"state"`             // Current Raft state (Leader, Follower, Candidate)
	Term              uint64            `json:"term"`              // Current election term
	CommitIndex       uint64            `json:"commitIndex"`       // Index of highest committed log entry
	LastApplied       uint64            `json:"lastApplied"`       // Index of highest entry applied to state machine
	LastLogIndex      uint64            `json:"lastLogIndex"`      // Index of last log entry
	LogEntries        []string          `json:"logEntries"`        // Recent log entries for display
	IsLeader          bool              `json:"isLeader"`          // True if this server is the current leader
	RecentEvents      []EventMessage    `json:"recentEvents"`      // Last few events for this server
	StateMachineState map[string]string `json:"stateMachineState"` // Current KV store contents
}

// ClusterState represents the entire cluster state for visualization.
type ClusterState struct {
	Servers   []ServerInfo `json:"servers"`   // All servers in the cluster
	Timestamp time.Time    `json:"timestamp"` // When this state was captured
}

// EventMessage represents a Raft event for the frontend visualization.
type EventMessage struct {
	Type      string    `json:"type"`              // Event type (election_won, log_appended, etc.)
	ServerID  string    `json:"serverId"`          // Which server this event is about
	Message   string    `json:"message"`           // Human-readable event description
	Timestamp time.Time `json:"timestamp"`         // When the event occurred
	Details   any       `json:"details,omitempty"` // Optional additional event data
}

// Global state for the visual demo application.
var (
	// Raft cluster
	servers     []*server.Server // All Raft server instances
	serverAddrs []string         // Server addresses (localhost:50051, etc.)

	// Event tracking
	eventsMu  sync.RWMutex
	events    []EventMessage // Global event log
	maxEvents = 100          // Maximum events to keep in memory

	// Per-server state tracking
	lastStateMu     sync.RWMutex
	lastServerState map[string]ServerInfo // Last known state per server (for diff detection)

	// Per-server event logs
	serverEventsMu  sync.RWMutex
	serverEvents    map[string][]EventMessage // Event log per server
	maxServerEvents = 10                      // Maximum events per server

	// Shutdown coordination
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc

	// Server ID mapping
	serverIDToName   map[server.ServerID]string // Map UUID to friendly name
	serverIDToNameMu sync.RWMutex
)

// Configuration constants.
const (
	numServers = 3     // Number of servers in the cluster
	basePort   = 50051 // Starting port for Raft servers
	httpPort   = 8080  // Port for the web UI
)
