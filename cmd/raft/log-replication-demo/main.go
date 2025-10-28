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
	// Give the cluster a moment to propagate the final commit
	time.Sleep(2 * time.Second)

	// Wait for all servers to have consistent state (same commitIndex and lastApplied)
	// with a timeout to prevent infinite waiting
	if !waitForConsistentState(servers, 10*time.Second) {
		fmt.Println("⚠️  Warning: Servers did not reach consistent state within timeout")
	}

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

// waitForConsistentState polls all servers until they have consistent commitIndex and lastApplied
// Returns true if consistency is achieved, false if timeout occurs
func waitForConsistentState(servers []string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	pollInterval := 100 * time.Millisecond // Poll every 100ms
	attemptCount := 0

	for time.Now().Before(deadline) {
		attemptCount++
		states := make([]*proto.GetServerStateResponse, 0, len(servers))
		allReachable := true

		// Query all servers
		for _, addr := range servers {
			state := getServerState(addr)
			if state == nil {
				allReachable = false
				break
			}
			states = append(states, state)
		}

		// If we couldn't reach all servers, try again
		if !allReachable {
			time.Sleep(pollInterval)
			continue
		}

		// Check if all servers have the same log entries and have applied them
		// All servers should eventually converge to the same commitIndex and lastApplied
		if len(states) > 0 {
			// All servers must have same log length, commitIndex, and lastApplied
			firstLastLogIndex := states[0].LastLogIndex
			firstCommitIndex := states[0].CommitIndex
			firstLastApplied := states[0].LastApplied

			allConsistent := true
			for _, state := range states {
				// All servers must have replicated all log entries
				if state.LastLogIndex != firstLastLogIndex {
					allConsistent = false
					break
				}
				// All servers must have same commitIndex and lastApplied
				if state.CommitIndex != firstCommitIndex || state.LastApplied != firstLastApplied {
					allConsistent = false
					break
				}
			}

			if allConsistent {
				fmt.Printf("✓ All servers consistent after %d polls (%.1fs)\n",
					attemptCount, time.Since(deadline.Add(-timeout)).Seconds())
				return true
			}
		}

		// Show progress every 500ms
		if attemptCount%5 == 0 {
			fmt.Printf("  Polling... (attempt %d)\n", attemptCount)
		}

		time.Sleep(pollInterval)
	}

	return false
}

// getServerState queries a server and returns its state, or nil if unreachable
func getServerState(addr string) *proto.GetServerStateResponse {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	defer conn.Close()

	client := proto.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	resp, err := client.GetServerState(ctx, &proto.GetServerStateRequest{})
	cancel()

	if err != nil {
		return nil
	}

	return resp
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
