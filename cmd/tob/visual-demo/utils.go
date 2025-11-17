package main

import (
	"aubg-cos-senior-project/internal/tob"
	"fmt"
	"time"
)

// addEvent adds an event to both the global event log and the per-node event log.
func addEvent(event EventMessage) {
	eventsMu.Lock()
	defer eventsMu.Unlock()
	events = append(events, event)
	if len(events) > maxEvents {
		events = events[len(events)-maxEvents:]
	}

	// Also add to per-node event log if it's a node event
	if event.NodeID != "" && event.NodeID != "Client" {
		nodeEventsMu.Lock()
		defer nodeEventsMu.Unlock()

		if nodeEvents[event.NodeID] == nil {
			nodeEvents[event.NodeID] = []EventMessage{}
		}
		nodeEvents[event.NodeID] = append(nodeEvents[event.NodeID], event)
		if len(nodeEvents[event.NodeID]) > maxNodeEvents {
			nodeEvents[event.NodeID] = nodeEvents[event.NodeID][len(nodeEvents[event.NodeID])-maxNodeEvents:]
		}
	}
}

// getNodeName returns the friendly name (Sequencer, Node-2, etc.) for a node ID.
// Falls back to showing the node ID if mapping doesn't exist.
func getNodeName(nodeID string) string {
	nodeIDToNameMu.RLock()
	defer nodeIDToNameMu.RUnlock()
	if name, ok := nodeIDToName[nodeID]; ok {
		return name
	}
	return nodeID
}

// getNodeInfo queries a TOB node for its current state.
// Returns NodeInfo struct with all relevant information.
func getNodeInfo(node *tob.TOBroadcast, nodeName string, config *tob.Config, nodeIndex int) NodeInfo {
	info := NodeInfo{
		ID:          nodeName,
		Address:     config.AdvertiseAddr,
		IsSequencer: config.IsSequencer,
	}

	// Get message counts
	trackingMu.RLock()
	info.MessagesSent = messagesSent[nodeName]
	info.MessagesDelivered = messagesDelivered[nodeName]

	// Get recent messages
	if msgs, ok := deliveredMessages[nodeName]; ok {
		// Return in reverse order (newest first)
		reversed := make([]MessageInfo, len(msgs))
		for i, m := range msgs {
			reversed[len(msgs)-1-i] = m
		}
		info.RecentMessages = reversed
	}
	trackingMu.RUnlock()

	// Get sequencer-specific info
	if config.IsSequencer {
		// For now, track these separately since TOB doesn't expose sequencer internals
		// We'll need to add getter methods to the sequencer
		info.NextSequenceNum = 0 // Will implement getter
		info.PendingMessages = 0 // Will implement getter
	}

	// Get metrics
	metrics := node.GetMetrics()
	if metrics != nil {
		report := metrics.GetReport(numNodes)
		info.Metrics = MetricsInfo{
			TotalBroadcasts:   report.DataMsgCount,          // Data messages sent (broadcasts)
			TotalDeliveries:   report.MessagesDelivered,     // Messages delivered
			AvgLatencyMs:      report.BroadcastLatency.Mean, // Average broadcast latency
			MessageThroughput: report.MessageThroughput,     // Messages/second
		}
	}

	// Get recent events for this node
	nodeEventsMu.RLock()
	if events, ok := nodeEvents[info.ID]; ok {
		// Return events in reverse order (newest first)
		reversed := make([]EventMessage, len(events))
		for i, e := range events {
			reversed[len(events)-1-i] = e
		}
		info.RecentEvents = reversed
	}
	nodeEventsMu.RUnlock()

	return info
}

// broadcastMessage broadcasts a message from a specific node.
func broadcastMessage(nodeIndex int, message string) error {
	if nodeIndex < 0 || nodeIndex >= len(nodes) {
		return fmt.Errorf("invalid node index")
	}

	node := nodes[nodeIndex]
	nodeName := getNodeNameByIndex(nodeIndex)

	// Track broadcast
	trackingMu.Lock()
	messagesSent[nodeName]++
	trackingMu.Unlock()

	// Add event
	addEvent(EventMessage{
		Type:      "message_broadcast",
		NodeID:    nodeName,
		Message:   fmt.Sprintf("%s initiated broadcast: %s", nodeName, message),
		Timestamp: now(),
		Details: map[string]any{
			"payload": message,
		},
	})

	// Broadcast the message
	return node.Broadcast([]byte(message))
}

// now returns the current time.
func now() time.Time {
	return time.Now()
}
