package main

import (
	"aubg-cos-senior-project/internal/pubsub"
	"aubg-cos-senior-project/internal/raft/proto"
	"aubg-cos-senior-project/internal/raft/server"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Command line flags
	port := flag.Int("port", 50054, "Port to run the server on")
	serverID := flag.String("id", "", "Server ID (will be generated if not provided)")
	leader := flag.String("leader", "localhost:50051", "Leader address to connect to")
	flag.Parse()

	// Create data directory for BBolt databases
	if err := os.MkdirAll("./data", 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	addr := fmt.Sprintf("localhost:%d", *port)

	log.Printf("Starting standalone Raft server on %s", addr)
	log.Printf("Will join cluster by contacting leader at %s", *leader)

	// Create server
	pubSub := pubsub.NewPubSub()
	srv := server.NewServer(0, server.ServerAddress(addr), nil, pubSub)

	// Override server ID if provided (for testing/demo purposes)
	if *serverID != "" {
		srv.ID = server.ServerID(*serverID)
		log.Printf("Using provided server ID: %s", *serverID)
	}

	log.Printf("Server ID: %s", srv.ID)
	log.Printf("Server Address: %s", addr)

	// Start server
	go func() {
		if err := srv.StartServer(*port); err != nil {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// Wait for server to be ready
	time.Sleep(500 * time.Millisecond)

	// Contact leader to add ourselves to the cluster
	log.Printf("Contacting leader to join cluster...")
	if err := joinCluster(*leader, string(srv.ID), addr); err != nil {
		log.Printf("Warning: Failed to join cluster: %v", err)
		log.Printf("Server is running but not part of the cluster yet")
	} else {
		log.Printf("✓ Successfully joined the cluster!")
	}

	// Create orchestrator and start it
	orch := server.NewOrchestrator(srv.GetPubSub(), srv)
	go orch.Run()

	// DO NOT START ELECTION TIMER HERE!
	// The server should wait to receive its first AppendEntries from the leader.
	// The election timer will be started automatically when the server receives
	// its first AppendEntries RPC, which will set it as a follower.

	log.Printf("Server is fully operational (waiting for leader contact)")

	// Wait for shutdown signal
	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-signalCtx.Done()

	log.Println("Shutting down...")
	srv.GracefulShutdown()
	log.Println("Server stopped")
}

func joinCluster(leaderAddr string, serverID string, serverAddress string) error {
	conn, err := grpc.NewClient(leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to leader: %w", err)
	}
	defer conn.Close()

	client := proto.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.AddServer(ctx, &proto.AddServerRequest{
		ServerId:      serverID,
		ServerAddress: serverAddress,
	})
	if err != nil {
		return fmt.Errorf("AddServer RPC failed: %w", err)
	}

	if resp.Status != proto.ConfigChangeStatus_OK {
		return fmt.Errorf("AddServer returned status: %v", resp.Status)
	}

	return nil
}
