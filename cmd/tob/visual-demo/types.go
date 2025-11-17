package main

import (
	"aubg-cos-senior-project/internal/tob"
	"context"
	"sync"
	"time"
)

// NodeInfo holds the runtime information about a TOB node for visualization.
type NodeInfo struct {
	ID                string         `json:"id"`                // Friendly name (Node-1, Node-2, Sequencer)
	Address           string         `json:"address"`           // Node address
	IsSequencer       bool           `json:"isSequencer"`       // Whether this node is the sequencer
	MessagesSent      int            `json:"messagesSent"`      // Total broadcasts initiated
	MessagesDelivered int            `json:"messagesDelivered"` // Total messages delivered
	NextSequenceNum   uint64         `json:"nextSequenceNum"`   // Next sequence number (sequencer only)
	PendingMessages   int            `json:"pendingMessages"`   // Messages waiting for sequencing
	RecentMessages    []MessageInfo  `json:"recentMessages"`    // Recent delivered messages
	RecentEvents      []EventMessage `json:"recentEvents"`      // Last few events for this node
	Metrics           MetricsInfo    `json:"metrics"`           // Protocol metrics
}

// MessageInfo represents a delivered message.
type MessageInfo struct {
	SequenceNumber uint64    `json:"sequenceNumber"` // Assigned sequence number
	From           string    `json:"from"`           // Sender node
	Payload        string    `json:"payload"`        // Message content
	Timestamp      time.Time `json:"timestamp"`      // When delivered
}

// MetricsInfo holds protocol metrics.
type MetricsInfo struct {
	TotalBroadcasts   uint64  `json:"totalBroadcasts"`   // Messages broadcast
	TotalDeliveries   uint64  `json:"totalDeliveries"`   // Messages delivered
	AvgLatencyMs      float64 `json:"avgLatencyMs"`      // Average delivery latency
	MessageThroughput float64 `json:"messageThroughput"` // Messages per second
}

// ClusterState represents the entire cluster state for visualization.
type ClusterState struct {
	Nodes     []NodeInfo `json:"nodes"`     // All nodes in the cluster
	Timestamp time.Time  `json:"timestamp"` // When this state was captured
}

// EventMessage represents a TOB event for the frontend visualization.
type EventMessage struct {
	Type      string    `json:"type"`              // Event type (broadcast, sequenced, delivered, etc.)
	NodeID    string    `json:"nodeId"`            // Which node this event is about
	Message   string    `json:"message"`           // Human-readable event description
	Timestamp time.Time `json:"timestamp"`         // When the event occurred
	Details   any       `json:"details,omitempty"` // Optional additional event data
}

// Global state for the visual demo application.
var (
	// TOB cluster
	nodes       []*tob.TOBroadcast // All TOB node instances
	nodeAddrs   []string           // Node addresses
	nodeConfigs []*tob.Config      // Node configurations

	// Tracking
	messagesSent      map[string]int           // Messages sent per node
	messagesDelivered map[string]int           // Messages delivered per node
	deliveredMessages map[string][]MessageInfo // Recent delivered messages per node
	trackingMu        sync.RWMutex

	// Event tracking
	eventsMu  sync.RWMutex
	events    []EventMessage // Global event log
	maxEvents = 100          // Maximum events to keep in memory

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
	numNodes       = 4    // Number of nodes in the cluster (1 sequencer + 3 regular)
	basePort       = 9000 // Starting port for TOB nodes
	httpPort       = 8082 // Port for the web UI
	sequencerIndex = 0    // Index of the sequencer node
)
