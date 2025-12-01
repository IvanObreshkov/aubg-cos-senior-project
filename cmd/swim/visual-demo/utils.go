package main

import (
	"aubg-cos-senior-project/internal/swim"
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

// getNodeName returns the friendly name (Node-1, Node-2) for a node ID.
// Falls back to showing the node ID if mapping doesn't exist.
func getNodeName(nodeID string) string {
	nodeIDToNameMu.RLock()
	defer nodeIDToNameMu.RUnlock()
	if name, ok := nodeIDToName[nodeID]; ok {
		return name
	}
	return nodeID
}

// getNodeInfo queries a SWIM node for its current state.
// Returns NodeInfo struct with all relevant information.
func getNodeInfo(node *swim.SWIM, nodeName string, config *swim.Config) NodeInfo {
	info := NodeInfo{
		ID:      nodeName,
		Address: config.AdvertiseAddr,
	}

	// Get local node information
	localNode := node.LocalNode()
	if localNode != nil {
		info.Status = localNode.Status.String()
		info.Incarnation = localNode.Incarnation
	} else {
		info.Status = "Unknown"
		info.Incarnation = 0
	}

	// Get all known members
	members := node.GetMembers()
	info.MemberCount = len(members)

	// Count members by status and build member list
	var memberInfos []MemberInfo
	info.AliveCount = 0
	info.SuspectCount = 0
	info.FailedCount = 0

	for _, member := range members {
		memberInfos = append(memberInfos, MemberInfo{
			ID:          member.ID,
			Address:     member.Address,
			Status:      member.Status.String(),
			Incarnation: member.Incarnation,
		})

		switch member.Status {
		case swim.Alive:
			info.AliveCount++
		case swim.Suspect:
			info.SuspectCount++
		case swim.Failed:
			info.FailedCount++
		}
	}
	info.Members = memberInfos

	// Get protocol metrics
	metrics := node.GetMetrics()
	if metrics != nil {
		info.Metrics = MetricsInfo{
			ProbesSent:       metrics.GetPingCount(),
			ProbesReceived:   metrics.GetTotalMessagesIn(), // Approximation
			AcksSent:         metrics.GetAckCount(),
			AcksReceived:     metrics.GetAckCount(), // Approximation
			SuspectsRaised:   metrics.GetSuspicionCount(),
			FailuresDetected: metrics.GetFailureCount(),
		}
	} else {
		info.Metrics = MetricsInfo{
			ProbesSent:       0,
			ProbesReceived:   0,
			AcksSent:         0,
			AcksReceived:     0,
			SuspectsRaised:   0,
			FailuresDetected: 0,
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

// simulateNodeFailure simulates a node failure for testing.
func simulateNodeFailure(nodeIndex int) error {
	if nodeIndex < 0 || nodeIndex >= len(nodes) {
		return fmt.Errorf("invalid node index")
	}

	node := nodes[nodeIndex]
	if node == nil {
		return fmt.Errorf("node not found")
	}

	// Crash the node (simulates abrupt failure without announcing leave)
	_ = node.Crash()

	addEvent(EventMessage{
		Type:      "node_crashed",
		NodeID:    fmt.Sprintf("Node-%d", nodeIndex+1),
		Message:   fmt.Sprintf("Node-%d crashed (simulated failure)", nodeIndex+1),
		Timestamp: time.Now(),
	})

	return nil
}
