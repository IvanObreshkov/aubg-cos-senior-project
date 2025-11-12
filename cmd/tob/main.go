package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"aubg-cos-senior-project/internal/tob"
)

// SimpleLogger implements the tob.Logger interface
type SimpleLogger struct {
	nodeID string
}

func (l *SimpleLogger) Debugf(format string, args ...interface{}) {
	log.Printf("[%s] DEBUG: "+format, append([]interface{}{l.nodeID}, args...)...)
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

func main() {
	// Command line flags
	nodeID := flag.String("id", "node1", "Node ID")
	bindAddr := flag.String("bind", "127.0.0.1:8001", "Bind address")
	advertiseAddr := flag.String("advertise", "", "Advertise address (defaults to bind address)")
	isSequencer := flag.Bool("sequencer", false, "Is this node the sequencer")
	sequencerAddr := flag.String("seq-addr", "127.0.0.1:8001", "Sequencer address")
	sequencerID := flag.String("seq-id", "node1", "Sequencer ID")
	nodesStr := flag.String("nodes", "127.0.0.1:8001", "Comma-separated list of all node addresses")
	flag.Parse()

	// Use bind address as advertise if not specified
	if *advertiseAddr == "" {
		*advertiseAddr = *bindAddr
	}

	// Parse nodes
	nodes := strings.Split(*nodesStr, ",")

	// Create TOB configuration
	config := tob.DefaultConfig()
	config.NodeID = *nodeID
	config.BindAddr = *bindAddr
	config.AdvertiseAddr = *advertiseAddr
	config.IsSequencer = *isSequencer
	config.SequencerAddr = *sequencerAddr
	config.SequencerID = *sequencerID
	config.Nodes = nodes
	config.Logger = &SimpleLogger{nodeID: *nodeID}

	// Create TOB instance
	log.Printf("Creating Total Order Broadcast node %s on %s", *nodeID, *bindAddr)
	if *isSequencer {
		log.Printf("This node is the SEQUENCER")
	} else {
		log.Printf("Sequencer is at %s (%s)", *sequencerAddr, *sequencerID)
	}

	tobNode, err := tob.New(config)
	if err != nil {
		log.Fatalf("Failed to create TOB node: %v", err)
	}

	// Set delivery callback
	deliveryCount := 0
	tobNode.SetDeliveryCallback(func(msg *tob.Message) {
		deliveryCount++
		log.Printf("[%s] DELIVERED #%d: Message from %s (seq=%d, payload=%s)",
			*nodeID, deliveryCount, msg.From, msg.SequenceNumber, string(msg.Payload))
	})

	// Start TOB node
	if err := tobNode.Start(); err != nil {
		log.Fatalf("Failed to start TOB node: %v", err)
	}

	log.Printf("[%s] Total Order Broadcast node started successfully", *nodeID)

	// Broadcast messages periodically
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		msgCount := 0
		for range ticker.C {
			msgCount++
			payload := fmt.Sprintf("Message %d from %s at %s", msgCount, *nodeID, time.Now().Format("15:04:05"))

			log.Printf("[%s] Broadcasting: %s", *nodeID, payload)

			if err := tobNode.Broadcast([]byte(payload)); err != nil {
				log.Printf("[%s] Failed to broadcast: %v", *nodeID, err)
			}
		}
	}()

	// Print statistics periodically
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			metrics := tobNode.GetMetrics()
			report := metrics.GetReport(1) // Single node report

			log.Printf("[%s] Metrics: DataMsg=%d, Delivered=%d, Throughput=%.2f msg/s",
				*nodeID,
				report.DataMsgCount,
				report.MessagesDelivered,
				report.MessageThroughput)

			log.Printf("[%s] Latency: P50=%.2fms, P95=%.2fms, P99=%.2fms",
				*nodeID,
				report.BroadcastLatency.P50,
				report.BroadcastLatency.P95,
				report.BroadcastLatency.P99)

			log.Printf("[%s] State: Sequencer=%s, NextExpected=%d, Pending=%d",
				*nodeID,
				tobNode.GetCurrentSequencer(),
				tobNode.GetNextExpectedSequence(),
				tobNode.GetPendingMessageCount())
		}
	}()

	// Wait for termination signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
	if err := tobNode.Stop(); err != nil {
		log.Printf("Error stopping node: %v", err)
	}

	log.Printf("[%s] Total Order Broadcast node stopped", *nodeID)
}
