package main

import (
	"aubg-cos-senior-project/internal/tob"
	"fmt"
	"log"
)

// SimpleLogger implements the tob.Logger interface for the visual demo.
type SimpleLogger struct {
	nodeID string
}

func (l *SimpleLogger) Debugf(format string, args ...interface{}) {
	// Skip debug logs for cleaner output
}

func (l *SimpleLogger) Infof(format string, args ...interface{}) {
	log.Printf("[%s] INFO: "+format, append([]interface{}{l.nodeID}, args...)...)
}

func (l *SimpleLogger) Warnf(format string, args ...interface{}) {
	log.Printf("[%s] WARN: "+format, append([]interface{}{l.nodeID}, args...)...)
}

func (l *SimpleLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[%s] ERROR: "+format, append([]interface{}{l.nodeID}, args...)...)
}

// createCluster initializes and returns a 4-node TOB cluster (1 sequencer + 3 regular nodes).
func createCluster() ([]*tob.TOBroadcast, []string, []*tob.Config) {
	var tobNodes []*tob.TOBroadcast
	var addrs []string
	var configs []*tob.Config

	// Create addresses for all nodes
	for i := 0; i < numNodes; i++ {
		addrs = append(addrs, fmt.Sprintf("127.0.0.1:%d", basePort+i))
	}

	// Create all nodes
	for i := 0; i < numNodes; i++ {
		nodeID := fmt.Sprintf("node-%d", i+1)
		bindAddr := addrs[i]
		advertiseAddr := addrs[i]
		isSequencer := (i == sequencerIndex)

		config := tob.DefaultConfig()
		config.NodeID = nodeID
		config.BindAddr = bindAddr
		config.AdvertiseAddr = advertiseAddr
		config.IsSequencer = isSequencer
		config.SequencerAddr = addrs[sequencerIndex]
		config.SequencerID = "node-1"
		config.Nodes = addrs
		config.Logger = &SimpleLogger{nodeID: nodeID}

		tobNode, err := tob.New(config)
		if err != nil {
			log.Fatalf("Failed to create TOB node %d: %v", i+1, err)
		}

		tobNodes = append(tobNodes, tobNode)
		configs = append(configs, config)

		// Map node ID to friendly name
		nodeIDToNameMu.Lock()
		if isSequencer {
			nodeIDToName[nodeID] = "Sequencer"
		} else {
			nodeIDToName[nodeID] = fmt.Sprintf("Node-%d", i)
		}
		nodeIDToNameMu.Unlock()

		// Set up delivery callback
		setupDeliveryCallback(tobNode, i)
	}

	return tobNodes, addrs, configs
}

// setupDeliveryCallback configures the delivery callback for a TOB node.
func setupDeliveryCallback(node *tob.TOBroadcast, idx int) {
	nodeName := getNodeNameByIndex(idx)

	node.SetDeliveryCallback(func(msg *tob.Message) {
		seqNum := msg.SequenceNumber
		from := msg.From
		payload := msg.Payload
		// Track delivery
		trackingMu.Lock()
		messagesDelivered[nodeName]++

		// Store message info
		msgInfo := MessageInfo{
			SequenceNumber: seqNum,
			From:           getNodeName(from),
			Payload:        string(payload),
			Timestamp:      now(),
		}
		deliveredMessages[nodeName] = append(deliveredMessages[nodeName], msgInfo)
		// Keep only last 10 messages
		if len(deliveredMessages[nodeName]) > 10 {
			deliveredMessages[nodeName] = deliveredMessages[nodeName][len(deliveredMessages[nodeName])-10:]
		}
		trackingMu.Unlock()

		// Add event
		addEvent(EventMessage{
			Type:      "message_delivered",
			NodeID:    nodeName,
			Message:   fmt.Sprintf("%s delivered message #%d from %s: %s", nodeName, seqNum, getNodeName(from), string(payload)),
			Timestamp: now(),
			Details: map[string]any{
				"sequenceNumber": seqNum,
				"from":           from,
				"payload":        string(payload),
			},
		})
	})
}

// getNodeNameByIndex returns the friendly name for a node by its index.
func getNodeNameByIndex(idx int) string {
	if idx == sequencerIndex {
		return "Sequencer"
	}
	return fmt.Sprintf("Node-%d", idx)
}
