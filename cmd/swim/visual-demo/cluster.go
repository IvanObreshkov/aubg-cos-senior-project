package main

import (
	"aubg-cos-senior-project/internal/swim"
	"fmt"
	"log"
	"time"
)

// createCluster initializes and returns a 5-node SWIM cluster.
// It creates all nodes and establishes membership.
func createCluster() ([]*swim.SWIM, []string, []*swim.Config) {
	var swimNodes []*swim.SWIM
	var addrs []string
	var configs []*swim.Config

	// Create addresses
	for i := 0; i < numNodes; i++ {
		addrs = append(addrs, fmt.Sprintf("127.0.0.1:%d", basePort+i))
	}

	// Create all nodes
	for i := 0; i < numNodes; i++ {
		nodeID := fmt.Sprintf("node-%d", i+1)
		bindAddr := addrs[i]
		advertiseAddr := addrs[i]

		config := swim.DefaultConfig()
		config.NodeID = nodeID
		config.BindAddr = bindAddr
		config.AdvertiseAddr = advertiseAddr
		config.ProbeInterval = 2 * time.Second
		config.ProbeTimeout = 500 * time.Millisecond
		config.SuspicionTimeout = 5 * time.Second
		config.IndirectProbeCount = 3
		// Logger is already set by DefaultConfig()

		// First node doesn't join anyone, others join the first node
		if i > 0 {
			config.JoinNodes = []string{addrs[0]}
		}

		swimNode, err := swim.New(config)
		if err != nil {
			log.Fatalf("Failed to create SWIM node %d: %v", i+1, err)
		}

		swimNodes = append(swimNodes, swimNode)
		configs = append(configs, config)

		// Map node ID to friendly name
		nodeIDToNameMu.Lock()
		nodeIDToName[nodeID] = fmt.Sprintf("Node-%d", i+1)
		nodeIDToNameMu.Unlock()

		// Set up event callbacks
		setupEventCallbacks(swimNode, i)
	}

	return swimNodes, addrs, configs
}

// setupEventCallbacks configures event callbacks for a SWIM node.
func setupEventCallbacks(node *swim.SWIM, idx int) {
	nodeName := fmt.Sprintf("Node-%d", idx+1)

	// Member join event
	node.OnMemberJoin(func(member *swim.Member) {
		addEvent(EventMessage{
			Type:      "member_join",
			NodeID:    nodeName,
			Message:   fmt.Sprintf("%s detected new member: %s", nodeName, getNodeName(member.ID)),
			Timestamp: time.Now(),
			Details: map[string]any{
				"memberID": member.ID,
				"address":  member.Address,
			},
		})
	})

	// Member leave event
	node.OnMemberLeave(func(member *swim.Member) {
		addEvent(EventMessage{
			Type:      "member_leave",
			NodeID:    nodeName,
			Message:   fmt.Sprintf("%s detected member left: %s", nodeName, getNodeName(member.ID)),
			Timestamp: time.Now(),
			Details: map[string]any{
				"memberID": member.ID,
			},
		})
	})

	// Member failed event
	node.OnMemberFailed(func(member *swim.Member) {
		addEvent(EventMessage{
			Type:      "member_failed",
			NodeID:    nodeName,
			Message:   fmt.Sprintf("%s detected member FAILED: %s", nodeName, getNodeName(member.ID)),
			Timestamp: time.Now(),
			Details: map[string]any{
				"memberID": member.ID,
			},
		})
	})

	// Member update event (status change)
	node.OnMemberUpdate(func(member *swim.Member) {
		addEvent(EventMessage{
			Type:      "member_update",
			NodeID:    nodeName,
			Message:   fmt.Sprintf("%s updated member status: %s → %s", nodeName, getNodeName(member.ID), member.Status.String()),
			Timestamp: time.Now(),
			Details: map[string]any{
				"memberID":    member.ID,
				"status":      member.Status.String(),
				"incarnation": member.Incarnation,
			},
		})
	})
}
