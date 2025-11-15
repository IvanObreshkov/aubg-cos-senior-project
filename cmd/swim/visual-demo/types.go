package main

import (
	"aubg-cos-senior-project/internal/swim"
	"context"
	"sync"
	"time"
)

// NodeInfo holds the runtime information about a SWIM node for visualization.
type NodeInfo struct {
	ID           string         `json:"id"`           // Friendly name (Node-1, Node-2, etc.)
	Address      string         `json:"address"`      // Node address (localhost:7946)
	Status       string         `json:"status"`       // Current status (Alive, Suspect, Failed, Left)
	Incarnation  uint64         `json:"incarnation"`  // Incarnation number
	MemberCount  int            `json:"memberCount"`  // Total members known
	AliveCount   int            `json:"aliveCount"`   // Alive members
	SuspectCount int            `json:"suspectCount"` // Suspect members
	FailedCount  int            `json:"failedCount"`  // Failed members
	Members      []MemberInfo   `json:"members"`      // Known members
	RecentEvents []EventMessage `json:"recentEvents"` // Last few events for this node
	Metrics      MetricsInfo    `json:"metrics"`      // Protocol metrics
}

// MemberInfo represents a member in the membership list.
type MemberInfo struct {
	ID          string `json:"id"`          // Member ID
	Address     string `json:"address"`     // Member address
	Status      string `json:"status"`      // Member status
	Incarnation uint64 `json:"incarnation"` // Incarnation number
}

// MetricsInfo holds protocol metrics.
type MetricsInfo struct {
	ProbesSent       uint64 `json:"probesSent"`
	ProbesReceived   uint64 `json:"probesReceived"`
	AcksSent         uint64 `json:"acksSent"`
	AcksReceived     uint64 `json:"acksReceived"`
	SuspectsRaised   uint64 `json:"suspectsRaised"`
	FailuresDetected uint64 `json:"failuresDetected"`
}

// ClusterState represents the entire cluster state for visualization.
type ClusterState struct {
	Nodes     []NodeInfo `json:"nodes"`     // All nodes in the cluster
	Timestamp time.Time  `json:"timestamp"` // When this state was captured
}

// EventMessage represents a SWIM event for the frontend visualization.
type EventMessage struct {
	Type      string    `json:"type"`              // Event type (probe_sent, ack_received, etc.)
	NodeID    string    `json:"nodeId"`            // Which node this event is about
	Message   string    `json:"message"`           // Human-readable event description
	Timestamp time.Time `json:"timestamp"`         // When the event occurred
	Details   any       `json:"details,omitempty"` // Optional additional event data
}

// Global state for the visual demo application.
var (
	// SWIM cluster
	nodes       []*swim.SWIM   // All SWIM node instances
	nodeAddrs   []string       // Node addresses (localhost:7946, etc.)
	nodeConfigs []*swim.Config // Node configurations

	// Event tracking
	eventsMu  sync.RWMutex
	events    []EventMessage // Global event log
	maxEvents = 100          // Maximum events to keep in memory

	// Per-node state tracking
	lastStateMu   sync.RWMutex
	lastNodeState map[string]NodeInfo // Last known state per node (for diff detection)

	// Per-node event logs
	nodeEventsMu  sync.RWMutex
	nodeEvents    map[string][]EventMessage // Event log per node
	maxNodeEvents = 15                      // Maximum events per node

	// Shutdown coordination
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc

	// Node ID mapping
	nodeIDToName   map[string]string // Map node ID to friendly name
	nodeIDToNameMu sync.RWMutex
)

// Configuration constants.
const (
	numNodes = 5    // Number of nodes in the cluster
	basePort = 7946 // Starting port for SWIM nodes
	httpPort = 8081 // Port for the web UI
)
