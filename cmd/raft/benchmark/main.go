package main

import (
	"aubg-cos-senior-project/internal/pubsub"
	"aubg-cos-senior-project/internal/raft/metrics"
	"aubg-cos-senior-project/internal/raft/proto"
	"aubg-cos-senior-project/internal/raft/server"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Global mapping from server ID to network address (for leader redirection)
var serverIDToAddr sync.Map // map[server.ServerID]string

func main() {
	// Parse command-line flags
	clusterSize := flag.Int("cluster-size", 3, "Number of nodes in the cluster")
	numCommands := flag.Int("commands", 100, "Number of commands to submit")
	outputFile := flag.String("output", "", "Output JSON file for metrics (optional)")
	flag.Parse()

	if *clusterSize < 3 {
		log.Fatal("Cluster size must be at least 3")
	}

	fmt.Printf("\n")
	fmt.Println("========================================")
	fmt.Println("RAFT PERFORMANCE BENCHMARK")
	fmt.Println("========================================")
	fmt.Printf("Cluster Size: %d nodes\n", *clusterSize)
	fmt.Printf("Commands: %d\n", *numCommands)
	fmt.Println("========================================")
	fmt.Println()

	basePort := 50051

	// Create data directory and clean any previous data
	fmt.Println("Preparing data directory...")
	if err := os.RemoveAll("./data"); err != nil && !os.IsNotExist(err) {
		log.Printf("Warning: Failed to remove old data directory: %v", err)
	}
	if err := os.MkdirAll("./data", 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}
	fmt.Println("✓ Data directory ready (cleaned)")
	fmt.Println()

	// Reserve addresses
	addrs := reserveAddresses(*clusterSize, basePort)

	// Create shared metrics collector
	sharedMetrics := metrics.NewMetrics()

	// Create cluster with metrics
	serverOrchestratorMap := createClusterWithMetrics(addrs, sharedMetrics)

	// Start cluster
	fmt.Println("🚀 Starting cluster...")
	bootCluster(serverOrchestratorMap, basePort)
	fmt.Println("✓ Cluster started")

	// Wait for leader election
	fmt.Println("⏳ Waiting for leader election...")
	time.Sleep(2 * time.Second)

	// Find leader
	leaderAddr := findLeader(addrs, basePort)
	if leaderAddr == "" {
		log.Fatal("❌ Could not find leader after startup")
	}
	fmt.Printf("✓ Leader elected at %s\n", leaderAddr)
	fmt.Println()

	// Setup graceful shutdown
	done := make(chan bool, 1)
	go listenForShutdown(serverOrchestratorMap, done)

	// Run benchmark
	fmt.Println("========================================")
	fmt.Println("RUNNING BENCHMARK")
	fmt.Println("========================================")
	fmt.Println()

	runBenchmark(leaderAddr, *numCommands, sharedMetrics)

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("BENCHMARK COMPLETE")
	fmt.Println("========================================")
	fmt.Println()

	// Generate and print report
	report := sharedMetrics.GetReport(*clusterSize)
	report.PrintReport()

	// Save JSON if requested
	if *outputFile != "" {
		saveReportJSON(&report, *outputFile)
	}

	fmt.Println("\nShutting down cluster...")

	// Trigger shutdown
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGTERM)

	// Wait for graceful shutdown
	<-done
}

func reserveAddresses(clusterSize int, basePort int) []server.ServerAddress {
	var addrs []server.ServerAddress
	for i := 0; i < clusterSize; i++ {
		addr := fmt.Sprintf("localhost:%d", basePort+i)
		addrs = append(addrs, server.ServerAddress(addr))
	}
	return addrs
}

func createClusterWithMetrics(addrs []server.ServerAddress, metrics *metrics.Metrics) map[*server.Server]*server.Orchestrator {
	serverToOrchestratorMap := make(map[*server.Server]*server.Orchestrator)

	// First pass: create all servers
	var servers []*server.Server
	serverAddresses := make(map[server.ServerID]server.ServerAddress)

	for _, addr := range addrs {
		pubSub := pubsub.NewPubSub()
		srv := server.NewServer(0, addr, nil, pubSub)

		// Set metrics collector for this server
		srv.SetMetrics(metrics)

		servers = append(servers, srv)
		serverAddresses[srv.ID] = addr

		// Store in global map for leader redirection
		serverIDToAddr.Store(string(srv.ID), string(addr))
	}

	// Second pass: update peers
	for _, srv := range servers {
		peers := make(map[server.ServerID]server.ServerAddress)
		for id, addr := range serverAddresses {
			if id != srv.ID {
				peers[id] = addr
			}
		}
		srv.SetPeersWithAddresses(peers)

		orch := server.NewOrchestrator(srv.GetPubSub(), srv)
		serverToOrchestratorMap[srv] = orch
	}

	return serverToOrchestratorMap
}

func bootCluster(serverToOrchestratorMap map[*server.Server]*server.Orchestrator, basePort int) {
	var wg sync.WaitGroup

	servers := make([]*server.Server, 0, len(serverToOrchestratorMap))
	for srv := range serverToOrchestratorMap {
		servers = append(servers, srv)
	}

	// Start all servers
	for i, srv := range servers {
		wg.Add(1)
		port := basePort + i
		go func(s *server.Server, p int) {
			defer wg.Done()
			if err := s.StartServer(p); err != nil {
				log.Printf("Server %v failed to boot: %v", s.ID, err)
			}
		}(srv, port)
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	// Start orchestrators
	for _, orch := range serverToOrchestratorMap {
		go orch.Run()
	}
	time.Sleep(100 * time.Millisecond)

	// Start election timers
	for _, srv := range servers {
		srv.StartElectionTimer()
	}
}

func findLeader(addrs []server.ServerAddress, basePort int) string {
	// Try multiple times to find leader
	for attempt := 0; attempt < 10; attempt++ {
		for i := range addrs {
			addr := fmt.Sprintf("localhost:%d", basePort+i)
			conn, err := grpc.NewClient(addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}

			client := proto.NewRaftServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			resp, err := client.GetServerState(ctx, &proto.GetServerStateRequest{})
			cancel()
			_ = conn.Close()

			if err == nil && resp.State == "Leader" {
				return addr
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return ""
}

func runBenchmark(leaderAddr string, numCommands int, metrics *metrics.Metrics) {
	commandsSent := 0
	successCount := 0
	failCount := 0
	consecutiveFailures := 0

	// Track current leader address (may change during benchmark)
	currentLeader := leaderAddr
	var leaderMu sync.RWMutex

	// Track pending commands for latency measurement
	type pendingCommand struct {
		submitTime time.Time
		index      uint64
	}
	pendingCmds := make(map[uint64]pendingCommand)
	var pendingMu sync.Mutex

	// Track failed commands for retry
	type failedCommand struct {
		cmdNum      int
		cmd         string
		retryCount  int
		lastAttempt time.Time
	}
	failedCmds := make(map[int]failedCommand) // map[commandNumber]failedCommand
	var failedMu sync.Mutex

	// Start a goroutine to check for commits
	commitCheckDone := make(chan bool)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				pendingMu.Lock()
				if len(pendingCmds) > 0 {
					// Query leader for commit status
					leaderMu.RLock()
					leader := currentLeader
					leaderMu.RUnlock()

					conn, err := grpc.NewClient(leader,
						grpc.WithTransportCredentials(insecure.NewCredentials()))
					if err != nil {
						pendingMu.Unlock()
						continue
					}

					client := proto.NewRaftServiceClient(conn)
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					resp, err := client.GetServerState(ctx, &proto.GetServerStateRequest{})
					cancel()
					_ = conn.Close()

					if err == nil {
						// Check which commands have been committed
						for idx, pending := range pendingCmds {
							if idx <= resp.CommitIndex {
								// Command is committed!
								latency := time.Since(pending.submitTime)
								metrics.RecordCommandLatency(latency)
								metrics.RecordCommandCommitted()
								delete(pendingCmds, idx)
							}
						}
					}
				}
				pendingMu.Unlock()

			case <-commitCheckDone:
				return
			}
		}
	}()

	// Submit commands
	for commandsSent < numCommands {
		cmd := fmt.Sprintf("SET key%d=value%d", commandsSent, commandsSent)

		submitTime := time.Now()

		leaderMu.RLock()
		leader := currentLeader
		leaderMu.RUnlock()

		success, index, suggestedLeaderID, err := submitCommand(leader, cmd)

		if success {
			successCount++
			consecutiveFailures = 0 // Reset failure counter on success
			// Track this command for latency measurement
			pendingMu.Lock()
			pendingCmds[index] = pendingCommand{
				submitTime: submitTime,
				index:      index,
			}
			pendingMu.Unlock()
		} else {
			failCount++
			consecutiveFailures++

			// Track this command for retry
			failedMu.Lock()
			failedCmds[commandsSent] = failedCommand{
				cmdNum:      commandsSent,
				cmd:         cmd,
				retryCount:  1,
				lastAttempt: submitTime,
			}
			failedMu.Unlock()

			// Log the error with context
			if err != nil {
				fmt.Printf("  ⚠️  Command %d failed to %s: %v\n", commandsSent, leader, err)
			} else {
				fmt.Printf("  ⚠️  Command %d rejected by %s (no error details)\n", commandsSent, leader)
			}

			// If server suggested a leader, use it immediately
			if suggestedLeaderID != "" {
				// Convert server ID to address
				if addrInterface, ok := serverIDToAddr.Load(suggestedLeaderID); ok {
					suggestedAddr := addrInterface.(string)
					if suggestedAddr != leader {
						leaderMu.Lock()
						currentLeader = suggestedAddr
						leaderMu.Unlock()
						fmt.Printf("  → Redirected to suggested leader: %s (ID: %s)\n", suggestedAddr, suggestedLeaderID)
						consecutiveFailures = 0 // Reset since we have a new leader
						// Don't retry immediately - let the next iteration use this leader
						continue
					}
				} else {
					fmt.Printf("  → Server suggested leader ID %s, but address not found in mapping\n", suggestedLeaderID)
				}
			}

			// If we get here, either no suggestion or suggestion didn't work
			// Try to find the leader, but with backoff for elections
			if consecutiveFailures >= 3 {
				fmt.Printf("  → %d consecutive failures, searching for new leader...\n", consecutiveFailures)

				// Try multiple times with backoff (elections may be in progress)
				var newLeader string
				for attempt := 0; attempt < 5; attempt++ {
					newLeader = findLeaderFromCluster(leader)
					if newLeader != "" {
						break
					}
					// Wait for election to complete
					fmt.Printf("  → No leader found (attempt %d/5), waiting for election...\n", attempt+1)
					time.Sleep(200 * time.Millisecond)
				}

				if newLeader != "" && newLeader != leader {
					leaderMu.Lock()
					currentLeader = newLeader
					leaderMu.Unlock()
					fmt.Printf("  ✓ New leader found: %s\n", newLeader)
					consecutiveFailures = 0 // Reset counter after finding new leader
				} else if newLeader == "" {
					fmt.Printf("  ⚠️  Could not find leader after 5 attempts (cluster unstable)\n")
				}
			} else {
				// Just wait a bit before retrying
				fmt.Printf("  → Retrying after short delay (%d/%d failures)...\n", consecutiveFailures, 3)
				time.Sleep(50 * time.Millisecond)
			}
		}

		commandsSent++

		// Print progress every 10 commands
		if commandsSent%10 == 0 {
			fmt.Printf("Progress: %d/%d commands sent (success=%d, failed=%d)\n",
				commandsSent, numCommands, successCount, failCount)
		}

		// Small delay to avoid overwhelming the leader
		time.Sleep(10 * time.Millisecond)
	}

	// Retry failed commands (up to 3 attempts per command)
	const maxRetries = 3
	fmt.Println()
	failedMu.Lock()
	numInitialFailures := len(failedCmds)
	failedMu.Unlock()

	if numInitialFailures > 0 {
		fmt.Printf("Initial send complete. Retrying %d failed commands...\n", numInitialFailures)

		retryRound := 0
		for {
			failedMu.Lock()
			numFailed := len(failedCmds)
			if numFailed == 0 {
				failedMu.Unlock()
				break
			}

			// Copy failed commands for this retry round
			toRetry := make([]failedCommand, 0, len(failedCmds))
			for _, fc := range failedCmds {
				if fc.retryCount <= maxRetries {
					toRetry = append(toRetry, fc)
				} else {
					fmt.Printf("  ⚠️  Command %d exceeded max retries (%d), giving up\n", fc.cmdNum, maxRetries)
					delete(failedCmds, fc.cmdNum)
				}
			}
			failedMu.Unlock()

			if len(toRetry) == 0 {
				break
			}

			retryRound++
			fmt.Printf("\n→ Retry round %d: attempting %d failed commands\n", retryRound, len(toRetry))

			for _, fc := range toRetry {
				leaderMu.RLock()
				leader := currentLeader
				leaderMu.RUnlock()

				submitTime := time.Now()
				success, index, suggestedLeaderID, err := submitCommand(leader, fc.cmd)

				if success {
					successCount++
					failCount-- // Remove from fail count
					fmt.Printf("  ✓ Command %d succeeded on retry %d\n", fc.cmdNum, fc.retryCount)

					// Track for latency
					pendingMu.Lock()
					pendingCmds[index] = pendingCommand{
						submitTime: submitTime,
						index:      index,
					}
					pendingMu.Unlock()

					// Remove from failed list
					failedMu.Lock()
					delete(failedCmds, fc.cmdNum)
					failedMu.Unlock()
				} else {
					// Update retry count
					failedMu.Lock()
					if existing, ok := failedCmds[fc.cmdNum]; ok {
						existing.retryCount++
						existing.lastAttempt = submitTime
						failedCmds[fc.cmdNum] = existing
						fmt.Printf("  ⚠️  Command %d failed retry %d/%d", fc.cmdNum, existing.retryCount, maxRetries)
						if err != nil {
							fmt.Printf(": %v\n", err)
						} else {
							fmt.Println()
						}
					}
					failedMu.Unlock()

					// Handle leader redirection
					if suggestedLeaderID != "" {
						if addrInterface, ok := serverIDToAddr.Load(suggestedLeaderID); ok {
							suggestedAddr := addrInterface.(string)
							if suggestedAddr != leader {
								leaderMu.Lock()
								currentLeader = suggestedAddr
								leaderMu.Unlock()
								fmt.Printf("  → Redirected to suggested leader: %s\n", suggestedAddr)
							}
						}
					}
				}

				time.Sleep(20 * time.Millisecond)
			}

			// Wait before next retry round
			time.Sleep(500 * time.Millisecond)
		}

		fmt.Println()
		failedMu.Lock()
		finalFailures := len(failedCmds)
		failedMu.Unlock()
		if finalFailures > 0 {
			fmt.Printf("⚠️  %d commands could not be successfully submitted after retries\n", finalFailures)
		} else {
			fmt.Printf("✓ All failed commands successfully retried\n")
		}
	}

	fmt.Println()
	fmt.Printf("Waiting for final commands to commit...\n")

	// Wait a bit for final commands to commit
	time.Sleep(2 * time.Second)

	// Final check for commits
	pendingMu.Lock()
	if len(pendingCmds) > 0 {
		fmt.Printf("Note: %d commands still pending (may not have committed yet)\n", len(pendingCmds))
	}
	pendingMu.Unlock()

	close(commitCheckDone)

	fmt.Printf("\n✓ Benchmark completed\n")
	fmt.Printf("  Commands sent: %d\n", commandsSent)
	fmt.Printf("  Successful: %d\n", successCount)
	fmt.Printf("  Failed: %d\n", failCount)
}

func submitCommand(leaderAddr string, cmd string) (success bool, index uint64, suggestedLeaderID string, err error) {
	conn, err := grpc.NewClient(leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false, 0, "", fmt.Errorf("failed to create gRPC client: %w", err)
	}
	defer func() { _ = conn.Close() }()

	client := proto.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := client.ClientCommand(ctx, &proto.ClientCommandRequest{
		Command: []byte(cmd),
	})
	cancel()

	if err != nil {
		return false, 0, "", fmt.Errorf("ClientCommand RPC failed: %w", err)
	}

	if !resp.Success {
		// Server rejected the command - return the suggested leader if available
		if resp.LeaderId != "" {
			return false, resp.Index, resp.LeaderId, fmt.Errorf("not the leader (server suggests leader: %s)", resp.LeaderId)
		}
		return false, resp.Index, "", fmt.Errorf("not the leader (server doesn't know current leader, cluster may be in election)")
	}

	return resp.Success, resp.Index, "", nil
}

// findLeaderFromCluster attempts to find the current leader by querying all known servers
func findLeaderFromCluster(oldLeaderAddr string) string {
	// Collect all known addresses from the global map
	var addresses []string
	serverIDToAddr.Range(func(key, value interface{}) bool {
		addresses = append(addresses, value.(string))
		return true
	})

	// Try to find leader among all servers
	for _, addr := range addresses {
		// Skip the old leader initially (check it last)
		if addr == oldLeaderAddr {
			continue
		}

		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}

		client := proto.NewRaftServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err := client.GetServerState(ctx, &proto.GetServerStateRequest{})
		cancel()
		_ = conn.Close()

		if err == nil && resp.State == "Leader" {
			return addr
		}
	}

	// If no other server is leader, check the old leader again
	// (it might have become leader again in a new term)
	if oldLeaderAddr != "" {
		conn, err := grpc.NewClient(oldLeaderAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			client := proto.NewRaftServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			resp, err := client.GetServerState(ctx, &proto.GetServerStateRequest{})
			cancel()
			_ = conn.Close()

			if err == nil && resp.State == "Leader" {
				return oldLeaderAddr
			}
		}
	}

	return ""
}

func listenForShutdown(serverToOrchestratorMap map[*server.Server]*server.Orchestrator, done chan bool) {
	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	<-signalCtx.Done()

	fmt.Println("\n🛑 Shutting down...")
	stop()

	// Graceful shutdown
	forceShutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var servers []*server.Server
	for srv := range serverToOrchestratorMap {
		servers = append(servers, srv)
	}

	gracefulShutdownDone := gracefullyShutdownCluster(serverToOrchestratorMap)

	select {
	case <-gracefulShutdownDone:
		fmt.Println("✓ Cluster shutdown complete")
	case <-forceShutdownCtx.Done():
		fmt.Println("⚠️  Forcing shutdown...")
		for _, raftServer := range servers {
			go raftServer.ForceShutdown()
		}
		time.Sleep(500 * time.Millisecond)
	}

	done <- true
}

func gracefullyShutdownCluster(serverToOrchestratorMap map[*server.Server]*server.Orchestrator) chan struct{} {
	var wg sync.WaitGroup

	for raftServer := range serverToOrchestratorMap {
		wg.Add(1)
		go func(s *server.Server) {
			defer wg.Done()
			s.GracefulShutdown()
		}(raftServer)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	return done
}

func saveReportJSON(report *metrics.Report, filename string) {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal report: %v", err)
		return
	}

	err = os.WriteFile(filename, data, 0644)
	if err != nil {
		log.Printf("Failed to write report to %s: %v", filename, err)
		return
	}

	fmt.Printf("\n✓ Report saved to %s\n", filename)
}
