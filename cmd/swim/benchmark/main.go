package main

import (
	"aubg-cos-senior-project/internal/swim"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// SimpleLogger implements the swim.Logger interface
type SimpleLogger struct {
	nodeID string
	quiet  bool
}

func (l *SimpleLogger) Debugf(format string, args ...interface{}) {
	if !l.quiet {
		log.Printf("[%s] DEBUG: "+format, append([]interface{}{l.nodeID}, args...)...)
	}
}

func (l *SimpleLogger) Infof(format string, args ...interface{}) {
	if !l.quiet {
		log.Printf("[%s] INFO: "+format, append([]interface{}{l.nodeID}, args...)...)
	}
}

func (l *SimpleLogger) Warnf(format string, args ...interface{}) {
	log.Printf("[%s] WARN: "+format, append([]interface{}{l.nodeID}, args...)...)
}

func (l *SimpleLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[%s] ERROR: "+format, append([]interface{}{l.nodeID}, args...)...)
}

// GroundTruth maintains external knowledge of which nodes are actually alive
// This helps verify false positive detection: if SWIM marks a node Failed but
// GroundTruth knows it's alive, that's a true false positive per SWIM paper
type GroundTruth struct {
	mu          sync.RWMutex
	aliveNodes  map[string]bool // nodeID -> is process actually running
	partitioned map[string]bool // nodeID -> is currently network partitioned
}

func NewGroundTruth() *GroundTruth {
	return &GroundTruth{
		aliveNodes:  make(map[string]bool),
		partitioned: make(map[string]bool),
	}
}

func (gt *GroundTruth) MarkAlive(nodeID string) {
	gt.mu.Lock()
	defer gt.mu.Unlock()
	gt.aliveNodes[nodeID] = true
	gt.partitioned[nodeID] = false
}

func (gt *GroundTruth) MarkCrashed(nodeID string) {
	gt.mu.Lock()
	defer gt.mu.Unlock()
	gt.aliveNodes[nodeID] = false
	gt.partitioned[nodeID] = false
}

func (gt *GroundTruth) MarkPartitioned(nodeID string) {
	gt.mu.Lock()
	defer gt.mu.Unlock()
	// Node is still alive (process running), just network isolated
	gt.aliveNodes[nodeID] = true
	gt.partitioned[nodeID] = true
}

func (gt *GroundTruth) EndPartition(nodeID string) {
	gt.mu.Lock()
	defer gt.mu.Unlock()
	gt.partitioned[nodeID] = false
}

func (gt *GroundTruth) IsActuallyAlive(nodeID string) bool {
	gt.mu.RLock()
	defer gt.mu.RUnlock()
	return gt.aliveNodes[nodeID]
}

func (gt *GroundTruth) IsPartitioned(nodeID string) bool {
	gt.mu.RLock()
	defer gt.mu.RUnlock()
	return gt.partitioned[nodeID]
}

func main() {
	// Parse command-line flags
	clusterSize := flag.Int("cluster-size", 3, "Number of nodes in the cluster")
	testDuration := flag.Int("duration", 60, "Test duration in seconds")
	outputFile := flag.String("output", "", "Output JSON file for metrics (optional)")
	quiet := flag.Bool("quiet", false, "Reduce log verbosity")
	withFailures := flag.Bool("with-failures", false, "Inject node failures during benchmark")
	failureType := flag.String("failure-type", "partitions", "Type of failures: 'partitions' or 'crashstop'")
	flag.Parse()

	if *clusterSize < 3 {
		log.Fatal("Cluster size must be at least 3")
	}

	fmt.Printf("\n")
	fmt.Println("========================================")
	fmt.Println("SWIM PERFORMANCE BENCHMARK")
	fmt.Println("========================================")
	fmt.Printf("Cluster Size: %d nodes\n", *clusterSize)
	fmt.Printf("Test Duration: %d seconds\n", *testDuration)
	if *withFailures {
		if *failureType == "crashstop" {
			fmt.Println("Mode: CRASH-STOP FAILURES (Real node crashes)")
		} else {
			fmt.Println("Mode: NETWORK PARTITIONS (Temporary isolation)")
		}
	} else {
		fmt.Println("Mode: STEADY-STATE OBSERVATION")
	}
	fmt.Println("========================================")
	fmt.Println()

	basePort := 7946

	// Create shared metrics collector
	sharedMetrics := swim.NewMetrics()

	// Create ground truth tracker (external knowledge of node states)
	groundTruth := NewGroundTruth()

	// Create cluster
	fmt.Println("🚀 Creating cluster...")
	nodes := createCluster(*clusterSize, basePort, sharedMetrics, *quiet)

	// Mark all nodes as alive in ground truth
	for _, node := range nodes {
		groundTruth.MarkAlive(node.LocalNode().ID)
	}

	// Start all nodes
	fmt.Println("🚀 Starting cluster...")
	for _, node := range nodes {
		if err := node.Start(); err != nil {
			log.Fatalf("Failed to start node: %v", err)
		}
	}
	fmt.Println("✓ Cluster started")

	// Wait for cluster to stabilize
	fmt.Println("⏳ Waiting for cluster to stabilize...")
	time.Sleep(5 * time.Second)

	// Verify cluster membership
	fmt.Println("✓ Cluster stabilized")
	printClusterStatus(nodes)

	// Setup graceful shutdown
	done := make(chan bool, 1)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\nShutting down cluster...")
		for _, node := range nodes {
			if err := node.Stop(); err != nil {
				log.Printf("Error stopping node: %v", err)
			}
		}
		done <- true
	}()

	// Run benchmark
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("RUNNING BENCHMARK")
	fmt.Println("========================================")
	fmt.Println()

	if *withFailures {
		runBenchmarkWithFailures(nodes, *testDuration, sharedMetrics, *failureType, groundTruth)
	} else {
		runBenchmark(nodes, *testDuration, sharedMetrics)
	}

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

func createCluster(clusterSize int, basePort int, metrics *swim.Metrics, quiet bool) []*swim.SWIM {
	nodes := make([]*swim.SWIM, 0, clusterSize)

	// First, create all nodes
	for i := 0; i < clusterSize; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		bindAddr := fmt.Sprintf("127.0.0.1:%d", basePort+i)

		config := swim.DefaultConfig()
		config.NodeID = nodeID
		config.BindAddr = bindAddr
		config.AdvertiseAddr = bindAddr
		config.Logger = &SimpleLogger{nodeID: nodeID, quiet: quiet}

		// For nodes after the first, set the first node as seed
		if i > 0 {
			config.JoinNodes = []string{fmt.Sprintf("127.0.0.1:%d", basePort)}
		}

		node, err := swim.New(config)
		if err != nil {
			log.Fatalf("Failed to create node %s: %v", nodeID, err)
		}

		// Set shared metrics
		node.SetMetrics(metrics)

		nodes = append(nodes, node)
	}

	return nodes
}

func printClusterStatus(nodes []*swim.SWIM) {
	fmt.Println("\nCluster Membership Status:")
	for _, node := range nodes {
		members := node.GetMembers()
		aliveCount := 0
		for _, m := range members {
			if m.Status == swim.Alive {
				aliveCount++
			}
		}
		fmt.Printf("  %s: %d total members, %d alive\n",
			node.LocalNode().ID, len(members), aliveCount)
	}
}

// runBenchmark runs a steady-state observation benchmark
// This measures normal protocol behavior without injecting failures
func runBenchmark(nodes []*swim.SWIM, durationSeconds int, metrics *swim.Metrics) {
	startTime := time.Now()
	duration := time.Duration(durationSeconds) * time.Second

	fmt.Printf("Running for %d seconds...\n", durationSeconds)
	fmt.Println("Observing failure detection, gossip propagation, and message throughput...")
	fmt.Println("📊 This measures STEADY-STATE performance:")
	fmt.Println("   - How fast are probes (ping-ack)?")
	fmt.Println("   - How many messages per second?")
	fmt.Println("   - What's the message load per node?")
	fmt.Println()

	// Periodically print status
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	done := make(chan bool)
	go func() {
		time.Sleep(duration)
		done <- true
	}()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			elapsed := time.Since(startTime)
			report := metrics.GetReport(len(nodes))
			fmt.Printf("  [%.0fs] Collecting metrics... (Messages: In=%d, Out=%d, Throughput: %.2f msg/s)\n",
				elapsed.Seconds(),
				report.TotalMessagesIn,
				report.TotalMessagesOut,
				report.MessageThroughput)
			printClusterStatus(nodes)
		}
	}
}

// runBenchmarkWithFailures routes to the appropriate failure injection benchmark
func runBenchmarkWithFailures(nodes []*swim.SWIM, durationSeconds int, metrics *swim.Metrics, failureType string, groundTruth *GroundTruth) {
	if failureType == "crashstop" {
		runCrashStopBenchmark(nodes, durationSeconds, metrics, groundTruth)
	} else {
		runPartitionBenchmark(nodes, durationSeconds, metrics, groundTruth)
	}
}

// runPartitionBenchmark measures false positive rate via network partitions
func runPartitionBenchmark(nodes []*swim.SWIM, durationSeconds int, metrics *swim.Metrics, groundTruth *GroundTruth) {
	startTime := time.Now()
	duration := time.Duration(durationSeconds) * time.Second

	fmt.Printf("Running for %d seconds with NETWORK PARTITIONS...\n", durationSeconds)
	fmt.Println("📊 This measures TRUE FALSE POSITIVE RATE (per SWIM paper):")
	fmt.Println("   - Healthy processes (running) incorrectly marked as Failed")
	fmt.Println("   - Partition duration: 10s (exceeds 5s timeout → triggers Failed state)")
	fmt.Println("   - When partition heals, node proves it was alive → false positive counted")
	fmt.Println("   - Also tracks refutation rate for suspicions that succeed")
	fmt.Println()

	// Set up failure detection callbacks for all nodes
	// This tracks when nodes are marked Failed (for denominator of false positive rate)
	for _, node := range nodes {
		node.OnMemberFailed(func(member *swim.Member) {
			// Record the failure detection (without latency timing, since we're measuring false positives not speed)
			metrics.RecordFailureDetection(0)
		})
	}

	// Create a context for controlling the failure injection
	stopFailureInjection := make(chan bool)

	// Inject network partitions to measure false positives
	go func() {
		// Need at least 3 nodes
		if len(nodes) < 3 {
			return
		}

		// Wait for initial stabilization
		time.Sleep(10 * time.Second)

		// Inject network partitions every 15 seconds to generate false positives
		partitionInterval := time.NewTicker(15 * time.Second)
		defer partitionInterval.Stop()

		// Rotate which node gets partitioned to distribute the load
		targetNodeIndex := 0

		for {
			select {
			case <-stopFailureInjection:
				return
			case <-partitionInterval.C:
				// Rotate through nodes (skip node index 0 as it's often the seed)
				targetNodeIndex = (targetNodeIndex % (len(nodes) - 1)) + 1
				injectNetworkPartition(nodes, metrics, groundTruth, targetNodeIndex)
			}
		}
	}()

	// Monitor progress
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	done := make(chan bool)
	go func() {
		time.Sleep(duration)
		done <- true
	}()

	for {
		select {
		case <-done:
			close(stopFailureInjection)
			return
		case <-ticker.C:
			elapsed := time.Since(startTime)
			report := metrics.GetReport(len(nodes))
			fmt.Printf("  [%.0fs] Suspicions: %d, Refuted: %d (%.1f%%), False Positives: %d (%.1f%%)\n",
				elapsed.Seconds(),
				report.SuspicionCount,
				report.RefutedSuspicionCount,
				report.RefutationRate*100,
				report.FalsePositiveCount,
				report.FalsePositiveRate*100)
		}
	}
}

// runCrashStopBenchmark measures failure detection with real node crashes
func runCrashStopBenchmark(nodes []*swim.SWIM, durationSeconds int, metrics *swim.Metrics, groundTruth *GroundTruth) {
	startTime := time.Now()
	duration := time.Duration(durationSeconds) * time.Second

	fmt.Printf("Running for %d seconds with CRASH-STOP FAILURES...\n", durationSeconds)
	fmt.Println("📊 This measures FAILURE DETECTION:")
	fmt.Println("   - How long to detect crashed nodes?")
	fmt.Println("   - Suspicion → Failed transition timing")
	fmt.Println("   - Detection latency and accuracy")
	fmt.Println("   - Note: Crashed nodes CANNOT refute (not false positives)")
	fmt.Println()

	// Create a context for controlling the failure injection
	stopFailureInjection := make(chan bool)

	// Inject crash-stop failures
	go func() {
		// Need at least 3 nodes
		if len(nodes) < 3 {
			return
		}

		// Wait for initial stabilization
		time.Sleep(10 * time.Second)

		// Inject a crash every 30 seconds
		crashInterval := time.NewTicker(30 * time.Second)
		defer crashInterval.Stop()

		targetNodeIndex := len(nodes) - 1 // Always crash the last node

		for {
			select {
			case <-stopFailureInjection:
				return
			case <-crashInterval.C:
				injectCrashStop(nodes, metrics, groundTruth, targetNodeIndex)
			}
		}
	}()

	// Monitor progress
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	done := make(chan bool)
	go func() {
		time.Sleep(duration)
		done <- true
	}()

	for {
		select {
		case <-done:
			close(stopFailureInjection)
			return
		case <-ticker.C:
			elapsed := time.Since(startTime)
			report := metrics.GetReport(len(nodes))
			fmt.Printf("  [%.0fs] Detections: %d, Suspicions: %d, Avg Detection Latency: %.2fms\n",
				elapsed.Seconds(),
				report.FailureDetectionCount,
				report.SuspicionCount,
				report.FailureDetectionLatency.Mean)
		}
	}
}

// injectNetworkPartition simulates a prolonged network partition (creates true false positive)
// Per SWIM paper: partitions exceeding suspicion timeout cause healthy processes to be marked Failed
func injectNetworkPartition(nodes []*swim.SWIM, metrics *swim.Metrics, groundTruth *GroundTruth, targetNodeIndex int) {
	if targetNodeIndex >= len(nodes) {
		return
	}

	targetNodeID := nodes[targetNodeIndex].LocalNode().ID

	fmt.Printf("\n🟡 NETWORK PARTITION: Isolating %s (will exceed timeout → Failed)\n", targetNodeID)

	// Mark in ground truth: node is still alive (process running), just partitioned
	groundTruth.MarkPartitioned(targetNodeID)
	partitionStartTime := time.Now()

	// Block incoming messages AND pause probes
	// This simulates a node that appears unresponsive but will come back
	nodes[targetNodeIndex].SimulateNetworkPartition()
	nodes[targetNodeIndex].PauseProbes()

	// Wait long enough to EXCEED suspicion timeout (5s) so node is marked Failed
	// Per SWIM paper: This creates a FALSE POSITIVE because the process is still running
	// When partition heals, the node will prove it was alive (creating measurable false positive)
	partitionDuration := 10 * time.Second
	fmt.Printf("   Partition duration: %.0fs (exceeds 5s timeout → will be marked Failed)\n", partitionDuration.Seconds())

	time.Sleep(partitionDuration)

	// End partition - node will prove it's alive, revealing the false positive
	fmt.Printf("🟢 PARTITION ENDED: %s will now prove it was alive (FALSE POSITIVE)\n", targetNodeID)
	nodes[targetNodeIndex].EndNetworkPartition()
	nodes[targetNodeIndex].ResumeProbes()

	// Mark partition ended in ground truth
	groundTruth.EndPartition(targetNodeID)

	// Give more time for:
	// 1. Node to rejoin and send Alive messages
	// 2. Other nodes to receive the Alive update
	// 3. Metrics to be recorded (false positive detection on Failed→Alive)
	time.Sleep(5 * time.Second)

	// Measure recovery latency (time from partition end to full recovery)
	recoveryLatency := time.Since(partitionStartTime.Add(partitionDuration))

	// Check metrics
	report := metrics.GetReport(len(nodes))
	fmt.Printf("✓ Recovery complete in %.2fs (Refuted: %d, False Positives: %d)\n\n",
		recoveryLatency.Seconds(), report.RefutedSuspicionCount, report.FalsePositiveCount)
}

// injectCrashStop crashes a node permanently (no refutation possible)
func injectCrashStop(nodes []*swim.SWIM, metrics *swim.Metrics, groundTruth *GroundTruth, targetNodeIndex int) {
	if targetNodeIndex >= len(nodes) {
		return
	}

	failingNodeID := nodes[targetNodeIndex].LocalNode().ID

	// Check if node is already crashed in ground truth
	if !groundTruth.IsActuallyAlive(failingNodeID) {
		fmt.Printf("\n⚠️  Node %s is already crashed, skipping\n", failingNodeID)
		return
	}

	fmt.Printf("\n🔴 CRASH-STOP: Crashing %s (permanent failure)\n", failingNodeID)
	failTime := time.Now()

	// Mark as crashed in ground truth (process actually stopped)
	groundTruth.MarkCrashed(failingNodeID)

	if err := nodes[targetNodeIndex].Crash(); err != nil {
		log.Printf("Error crashing node: %v", err)
		return
	}

	// Monitor for detection
	go func(nodeID string, fTime time.Time) {
		detectionTimeout := time.After(15 * time.Second)
		checkTicker := time.NewTicker(500 * time.Millisecond)
		defer checkTicker.Stop()

		for {
			select {
			case <-detectionTimeout:
				fmt.Println("⚠️  Failure detection timeout")
				return
			case <-checkTicker.C:
				for i, node := range nodes {
					if i == targetNodeIndex {
						continue
					}
					members := node.GetMembers()
					for _, m := range members {
						if m.ID == nodeID && (m.Status == swim.Suspect || m.Status == swim.Failed) {
							detectTime := time.Now()
							detectionLatency := detectTime.Sub(fTime)
							fmt.Printf("✓ Failure detected in %.2fs by %s (status: %v)\n",
								detectionLatency.Seconds(), node.LocalNode().ID, m.Status)
							metrics.RecordFailureDetection(detectionLatency)
							return
						}
					}
				}
			}
		}
	}(failingNodeID, failTime)

	fmt.Printf("   Note: Node will NOT rejoin (testing permanent failures)\n\n")
}

func saveReportJSON(report *swim.Report, filename string) {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal report: %v", err)
		return
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		log.Printf("Failed to write report to file: %v", err)
		return
	}

	fmt.Printf("\n✓ Report saved to %s\n", filename)
}
