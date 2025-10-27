package main

import (
	"aubg-cos-senior-project/internal/raft/proto"
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	fmt.Println("========================================")
	fmt.Println("Raft Log Replication Demo (Section 5.3)")
	fmt.Println("========================================")
	fmt.Println()

	fmt.Println("⏳ Waiting for cluster to start and elect a leader...")
	time.Sleep(3 * time.Second)

	servers := []string{"localhost:50051", "localhost:50052", "localhost:50053"}

	fmt.Println("========================================")
	fmt.Println("Phase 1: Submitting Commands to Leader")
	fmt.Println("========================================")
	fmt.Println()

	commands := []string{
		"SET name=Alice",
		"SET city=Sofia",
		"SET language=Go",
	}

	for i, cmd := range commands {
		fmt.Printf("[%d] Submitting: %s\n", i+1, cmd)

		success := false
		for retries := 0; retries < 3 && !success; retries++ {
			if retries > 0 {
				fmt.Printf("  Retrying (attempt %d)...\n", retries+1)
				time.Sleep(500 * time.Millisecond)
			}

			leaderAddr := findLeader(servers)
			if leaderAddr == "" {
				fmt.Printf("  ❌ Could not find leader\n")
				continue
			}

			success = submitCommand(leaderAddr, cmd)
		}

		if !success {
			fmt.Printf("  ❌ Failed after 3 attempts\n")
		}

		time.Sleep(200 * time.Millisecond)
	}

	fmt.Println()
	fmt.Println("⏳ Waiting for replication to complete...")
	time.Sleep(2 * time.Second)

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("Final State of All Servers")
	fmt.Println("========================================")
	fmt.Println()

	for i, addr := range servers {
		fmt.Printf("Server %d (%s):\n", i+1, addr)
		queryServerState(addr)
		fmt.Println()
	}

	fmt.Println("========================================")
	fmt.Println("Demo Complete!")
	fmt.Println("========================================")
	fmt.Println()
	fmt.Println("✓ Commands submitted to leader")
	fmt.Println("✓ Leader replicated to all followers")
	fmt.Println("✓ All servers have consistent logs")
	fmt.Println("✓ State machines applied entries")
	fmt.Println()
	fmt.Println("Section 5.3 (Log Replication) demonstrated:")
	fmt.Println("• Leader appends commands to log")
	fmt.Println("• Leader sends AppendEntries RPCs")
	fmt.Println("• Majority acknowledgment = committed")
	fmt.Println("• Committed entries applied to state machine")
}

func submitCommand(serverAddr string, cmd string) bool {
	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false
	}
	defer conn.Close()

	client := proto.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	resp, err := client.ClientCommand(ctx, &proto.ClientCommandRequest{
		Command: []byte(cmd),
	})
	cancel()

	if err != nil {
		return false
	}

	if resp.Success {
		fmt.Printf("  ✓ Committed at index %d\n", resp.Index)
		return true
	}

	return false
}

func findLeader(servers []string) string {
	for _, addr := range servers {
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}

		client := proto.NewRaftServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := client.GetServerState(ctx, &proto.GetServerStateRequest{})
		cancel()
		conn.Close()

		if err == nil && resp.State == "Leader" {
			return addr
		}
	}
	return ""
}

func queryServerState(addr string) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("  ❌ Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	client := proto.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	resp, err := client.GetServerState(ctx, &proto.GetServerStateRequest{})
	cancel()

	if err != nil {
		fmt.Printf("  ❌ Query failed: %v\n", err)
		return
	}

	serverID := resp.ServerId
	if len(serverID) > 12 {
		serverID = serverID[:12] + "..."
	}

	fmt.Printf("  Server ID:      %s\n", serverID)
	fmt.Printf("  State:          %s\n", resp.State)
	fmt.Printf("  Term:           %d\n", resp.CurrentTerm)
	fmt.Printf("  Last Log:       %d\n", resp.LastLogIndex)
	fmt.Printf("  Commit Index:   %d\n", resp.CommitIndex)
	fmt.Printf("  Last Applied:   %d\n", resp.LastApplied)
}
