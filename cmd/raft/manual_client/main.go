package main

import (
	"aubg-cos-senior-project/internal/raft/proto"
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Command line flags
	serverAddr := flag.String("server", "localhost:50051", "Server address to connect to")
	command := flag.String("cmd", "SET test=hello", "Command to send (e.g., 'SET key=value' or 'DEL key')")
	flag.Parse()

	fmt.Printf("================================================\n")
	fmt.Printf("Raft Client - Submitting Command to Cluster\n")
	fmt.Printf("================================================\n")
	fmt.Printf("Server:  %s\n", *serverAddr)
	fmt.Printf("Command: %s\n", *command)
	fmt.Printf("================================================\n\n")

	log.Printf("Connecting to Raft server at %s", *serverAddr)

	// Connect to the server
	conn, err := grpc.NewClient(*serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("❌ Failed to connect to server: %v\n\nMake sure the cluster is running:\n  go run ./cmd/app/main.go\n", err)
	}
	defer conn.Close()

	client := proto.NewRaftServiceClient(conn)

	// Send the command
	log.Printf("Sending command: %s", *command)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ClientCommand(ctx, &proto.ClientCommandRequest{
		Command: []byte(*command),
	})

	if err != nil {
		log.Fatalf("❌ Error sending command: %v\n\nTroubleshooting:\n  - Is the cluster running?\n  - Is the server at %s reachable?\n  - Try a different server port (50051, 50052, or 50053)\n", err, *serverAddr)
	}

	fmt.Println()
	if resp.Success {
		fmt.Printf("✅ SUCCESS!\n")
		fmt.Printf("   Command committed at index: %d\n", resp.Index)
		fmt.Printf("   The entry has been replicated to the cluster.\n")
	} else {
		fmt.Printf("❌ FAILED - Server is not the leader\n")
		if resp.LeaderId != "" {
			fmt.Printf("   Leader hint: %s\n", resp.LeaderId)
			fmt.Printf("\n   Try connecting to the leader instead:\n")
			fmt.Printf("   go run ./cmd/manual_client/main.go -server %s -cmd \"%s\"\n", resp.LeaderId, *command)
		} else {
			fmt.Printf("\n   This server is a Follower. Try other ports:\n")
			fmt.Printf("   go run ./cmd/manual_client/main.go -server localhost:50051 -cmd \"%s\"\n", *command)
			fmt.Printf("   go run ./cmd/manual_client/main.go -server localhost:50052 -cmd \"%s\"\n", *command)
			fmt.Printf("   go run ./cmd/manual_client/main.go -server localhost:50053 -cmd \"%s\"\n", *command)
		}
	}
	fmt.Println()
}
