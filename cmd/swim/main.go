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

	"aubg-cos-senior-project/internal/swim"
)

// SimpleLogger implements the swim.Logger interface
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
	bindAddr := flag.String("bind", "127.0.0.1:7946", "Bind address")
	advertiseAddr := flag.String("advertise", "", "Advertise address (defaults to bind address)")
	joinAddrs := flag.String("join", "", "Comma-separated list of seed node addresses to join")
	flag.Parse()

	// Use bind address as advertise if not specified
	if *advertiseAddr == "" {
		*advertiseAddr = *bindAddr
	}

	// Parse join addresses
	var joinNodes []string
	if *joinAddrs != "" {
		joinNodes = strings.Split(*joinAddrs, ",")
	}

	// Create SWIM configuration
	config := swim.DefaultConfig()
	config.NodeID = *nodeID
	config.BindAddr = *bindAddr
	config.AdvertiseAddr = *advertiseAddr
	config.JoinNodes = joinNodes
	config.Logger = &SimpleLogger{nodeID: *nodeID}

	// Create SWIM instance
	log.Printf("Creating SWIM node %s on %s", *nodeID, *bindAddr)
	node, err := swim.New(config)
	if err != nil {
		log.Fatalf("Failed to create SWIM node: %v", err)
	}

	// Register event callbacks
	node.OnMemberJoin(func(member *swim.Member) {
		log.Printf("[%s] Member JOINED: %s (%s)", *nodeID, member.ID, member.Address)
	})

	node.OnMemberLeave(func(member *swim.Member) {
		log.Printf("[%s] Member LEFT: %s (%s)", *nodeID, member.ID, member.Address)
	})

	node.OnMemberFailed(func(member *swim.Member) {
		log.Printf("[%s] Member FAILED: %s (%s)", *nodeID, member.ID, member.Address)
	})

	node.OnMemberUpdate(func(member *swim.Member) {
		log.Printf("[%s] Member UPDATED: %s (%s) - Status: %s",
			*nodeID, member.ID, member.Address, member.Status)
	})

	// Start SWIM node
	if err := node.Start(); err != nil {
		log.Fatalf("Failed to start SWIM node: %v", err)
	}

	log.Printf("[%s] SWIM node started successfully", *nodeID)

	// Print membership status periodically
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			members := node.GetMembers()
			log.Printf("[%s] Current cluster members (%d):", *nodeID, len(members))
			for _, member := range members {
				log.Printf("  - %s (%s) - Status: %s, Incarnation: %d",
					member.ID, member.Address, member.Status, member.Incarnation)
			}
		}
	}()

	// Wait for termination signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
	if err := node.Stop(); err != nil {
		log.Printf("Error stopping node: %v", err)
	}

	log.Printf("[%s] SWIM node stopped", *nodeID)
}
