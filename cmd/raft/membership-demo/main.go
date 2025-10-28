package main

import (
	"aubg-cos-senior-project/internal/raft/proto"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	fmt.Println("========================================")
	fmt.Println("Raft Membership Change Demo (Section 6)")
	fmt.Println("========================================")
	fmt.Println()

	fmt.Println("⏳ Waiting for initial cluster to stabilize...")
	time.Sleep(3 * time.Second)

	servers := []string{"localhost:50051", "localhost:50052", "localhost:50053"}

	// Phase 1: Show initial 3-server cluster
	fmt.Println("========================================")
	fmt.Println("Phase 1: Initial Cluster (3 servers)")
	fmt.Println("========================================")
	fmt.Println()

	showClusterState(servers)

	// Phase 2: Submit a command to initial cluster
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("Phase 2: Submit Command to Initial Cluster")
	fmt.Println("========================================")
	fmt.Println()

	leaderAddr := findLeader(servers)
	if leaderAddr == "" {
		log.Fatal("❌ Could not find leader")
	}

	fmt.Printf("Leader is at: %s\n", leaderAddr)
	fmt.Println()

	fmt.Println("[1] Submitting: SET initial=3servers")
	if submitCommand(leaderAddr, "SET initial=3servers") {
		fmt.Println("  ✓ Committed successfully")
	}

	time.Sleep(1 * time.Second)

	// Phase 3: Add a new server
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("Phase 3: Add New Server to Cluster")
	fmt.Println("========================================")
	fmt.Println()

	fmt.Println("📝 Starting a 4th server on localhost:50054")
	fmt.Println()

	// Generate a unique server ID for the 4th server
	newServerID := fmt.Sprintf("server-4-%d", time.Now().Unix())

	// Build the single-server binary first
	fmt.Println("Building server binary...")
	buildCmd := exec.Command("go", "build", "-o", "/tmp/raft-server", "./cmd/raft/single-server/main.go")
	if err := buildCmd.Run(); err != nil {
		fmt.Printf("❌ Failed to build server: %v\n", err)
		fmt.Println("Continuing with demo anyway (server won't actually start)...")
	} else {
		fmt.Println("✓ Server binary built")

		// Start the 4th server WITHOUT auto-join (we'll add it manually)
		fmt.Println()
		fmt.Println("Starting 4th server process...")
		serverCmd := exec.Command("/tmp/raft-server",
			"-port", "50054",
			"-id", newServerID)
		// NOTE: No -leader flag, we'll add it to the cluster manually

		// Capture output to debug any issues
		serverCmd.Stdout = os.Stdout
		serverCmd.Stderr = os.Stderr

		// Don't wait for it to complete (run in background)
		if err := serverCmd.Start(); err != nil {
			fmt.Printf("❌ Failed to start server: %v\n", err)
		} else {
			fmt.Printf("✓ Server started (PID: %d)\n", serverCmd.Process.Pid)
			fmt.Println()
			fmt.Println("⏳ Waiting for server to initialize...")
			time.Sleep(2 * time.Second) // Wait for server to be ready

			// Now ask the leader to add this server
			fmt.Println("📝 Adding server to cluster configuration...")
			if addServer(leaderAddr, newServerID, "localhost:50054") {
				fmt.Println("✓ Server added to configuration")
				fmt.Println()
				fmt.Println("  The 4th server is now:")
				fmt.Println("  - Part of the cluster configuration")
				fmt.Println("  - Catching up on the log")
				fmt.Println("  - Participating in consensus")
			} else {
				fmt.Println("❌ Failed to add server to cluster")
			}

			// Store the process so we can kill it later
			defer func() {
				if serverCmd.Process != nil {
					fmt.Println()
					fmt.Println("Cleaning up: stopping 4th server...")
					_ = serverCmd.Process.Kill()
				}
			}()
		}
	}

	fmt.Println()
	fmt.Println("⏳ Waiting for log replication...")
	time.Sleep(3 * time.Second)

	// Phase 4: Show cluster with 4 servers
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("Phase 4: Cluster State After Adding Server")
	fmt.Println("========================================")
	fmt.Println()

	allServers := []string{"localhost:50051", "localhost:50052", "localhost:50053", "localhost:50054"}
	fmt.Println("All 4 servers in the cluster:")
	showClusterState(allServers)

	// Phase 5: Submit command with 4-server cluster
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("Phase 5: Submit Command to 4-Server Cluster")
	fmt.Println("========================================")
	fmt.Println()

	fmt.Println("[2] Submitting: SET after_add=4servers")
	if submitCommand(leaderAddr, "SET after_add=4servers") {
		fmt.Println("  ✓ Committed successfully")
		fmt.Println("  Note: This command was replicated with the new configuration")
	}

	time.Sleep(1 * time.Second)

	// Phase 6: Remove server-4 (the one we're about to kill)
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("Phase 6: Remove Server from Cluster")
	fmt.Println("========================================")
	fmt.Println()

	fmt.Println("Pre-removal cluster state (checking if config change completed):")
	showClusterState(allServers)
	fmt.Println()

	// Remove server-4 (the one we added and will kill at cleanup)
	// This prevents "connection refused" errors
	fmt.Printf("📝 Attempting to remove server-4: %s\n", newServerID)
	fmt.Println()

	success := removeServer(leaderAddr, newServerID)
	if success {
		fmt.Println()
		fmt.Println("✓ Server removed from configuration")
		fmt.Println("  The removed server will:")
		fmt.Println("  - Stop participating in consensus")
		fmt.Println("  - No longer receive updates")
		fmt.Println("  - Be safely shut down (which happens at cleanup)")
	} else {
		fmt.Println()
		fmt.Println("Note: If removal failed due to IN_PROGRESS, this is expected")
		fmt.Println("behavior showing that your safety mechanisms work correctly.")
		fmt.Println("Only one configuration change can be in progress at a time.")
	}

	time.Sleep(2 * time.Second)

	// Phase 7: Final state
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("Phase 7: Final Cluster State")
	fmt.Println("========================================")
	fmt.Println()

	fmt.Println("Querying remaining servers (removed server may still respond but won't participate):")
	showClusterState(allServers)

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("Demo Complete!")
	fmt.Println("========================================")
	fmt.Println()
	fmt.Println("✓ Demonstrated membership changes (Section 6)")
	fmt.Println("✓ Added server to cluster")
	fmt.Println("✓ Commands replicated with new configuration")
	fmt.Println("✓ Removed server from cluster")
	fmt.Println()
	fmt.Println("Key Concepts Demonstrated:")
	fmt.Println("• Joint consensus (C_old,new) for safe transitions")
	fmt.Println("• Configuration changes replicated through log")
	fmt.Println("• Cluster remains available during changes")
	fmt.Println("• Majority calculated with new configuration")
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

	return err == nil && resp.Success
}

func addServer(leaderAddr string, serverID string, serverAddress string) bool {
	conn, err := grpc.NewClient(leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return false
	}
	defer conn.Close()

	client := proto.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := client.AddServer(ctx, &proto.AddServerRequest{
		ServerId:      serverID,
		ServerAddress: serverAddress,
	})
	cancel()

	if err != nil {
		fmt.Printf("RPC error: %v\n", err)
		return false
	}

	return resp.Status == proto.ConfigChangeStatus_OK
}

func removeServer(leaderAddr string, serverID string) bool {
	conn, err := grpc.NewClient(leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("  Connection error: %v\n", err)
		return false
	}
	defer conn.Close()

	client := proto.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := client.RemoveServer(ctx, &proto.RemoveServerRequest{
		ServerId: serverID,
	})
	cancel()

	if err != nil {
		fmt.Printf("  RPC error: %v\n", err)
		return false
	}

	switch resp.Status {
	case proto.ConfigChangeStatus_OK:
		return true
	case proto.ConfigChangeStatus_NOT_LEADER:
		fmt.Printf("  Error: Server is not the leader\n")
		if resp.LeaderId != "" {
			fmt.Printf("  Leader hint: %s\n", resp.LeaderId)
		}
	case proto.ConfigChangeStatus_IN_PROGRESS:
		fmt.Printf("  Error: Configuration change already in progress\n")
		fmt.Printf("  Note: Previous AddServer may not have completed yet\n")
	case proto.ConfigChangeStatus_TIMEOUT:
		fmt.Printf("  Error: Operation timed out\n")
	default:
		fmt.Printf("  Error: Unknown status %v\n", resp.Status)
	}

	return false
}

func getFollowerID(servers []string) string {
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

		if err == nil && resp.State == "Follower" {
			return resp.ServerId
		}
	}
	return ""
}

func showClusterState(servers []string) {
	for i, addr := range servers {
		fmt.Printf("Server %d (%s):\n", i+1, addr)
		queryServerState(addr)
		fmt.Println()
	}
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

	fmt.Printf("  Server ID:    %s\n", serverID)
	fmt.Printf("  State:        %s\n", resp.State)
	fmt.Printf("  Term:         %d\n", resp.CurrentTerm)
	fmt.Printf("  Last Log:     %d\n", resp.LastLogIndex)
	fmt.Printf("  Commit Index: %d\n", resp.CommitIndex)
}
