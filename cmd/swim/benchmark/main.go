package main

import (
	"aubg-cos-senior-project/internal/swim"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
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

func main() {
	// Parse command-line flags
	clusterSize := flag.Int("cluster-size", 3, "Number of nodes in the cluster")
	testDuration := flag.Int("duration", 60, "Test duration in seconds")
	outputFile := flag.String("output", "", "Output JSON file for metrics (optional)")
	quiet := flag.Bool("quiet", false, "Reduce log verbosity")
	withFailures := flag.Bool("with-failures", false, "Inject node failures during benchmark")
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
		fmt.Println("Mode: WITH FAILURE INJECTION")
	} else {
		fmt.Println("Mode: STEADY-STATE OBSERVATION")
	}
	fmt.Println("========================================")
	fmt.Println()

	basePort := 7946

	// Create shared metrics collector
	sharedMetrics := swim.NewMetrics()

	// Create cluster
	fmt.Println("🚀 Creating cluster...")
	nodes := createCluster(*clusterSize, basePort, sharedMetrics, *quiet)

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
		runBenchmarkWithFailures(nodes, *testDuration, sharedMetrics)
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

// runBenchmarkWithFailures runs an active benchmark with injected failures
// This measures failure detection latency and recovery behavior
func runBenchmarkWithFailures(nodes []*swim.SWIM, durationSeconds int, metrics *swim.Metrics) {
	startTime := time.Now()
	duration := time.Duration(durationSeconds) * time.Second

	fmt.Printf("Running for %d seconds WITH FAILURE INJECTION...\n", durationSeconds)
	fmt.Println("📊 This measures FAILURE DETECTION performance:")
	fmt.Println("   - How long to detect a failed node?")
	fmt.Println("   - What's the false positive rate?")
	fmt.Println("   - How fast do nodes rejoin?")
	fmt.Println()

	// Create a context for controlling the failure injection
	stopFailureInjection := make(chan bool)

	// Inject failures at intervals
	go func() {
		// Track failure injection times for latency calculation
		type failureEvent struct {
			nodeIndex  int
			nodeID     string
			failTime   time.Time
			detectTime time.Time
		}

		// Need at least 3 nodes to safely kill one (2 remain alive)
		if len(nodes) < 3 {
			return
		}

		// Wait a bit for initial stabilization
		time.Sleep(10 * time.Second)

		// Inject a failure every 20 seconds
		failureInterval := time.NewTicker(20 * time.Second)
		defer failureInterval.Stop()

		targetNodeIndex := len(nodes) - 1 // Pick last node

		for {
			select {
			case <-stopFailureInjection:
				return
			case <-failureInterval.C:

				// Store node ID BEFORE crashing (can't access after)
				failingNodeID := nodes[targetNodeIndex].LocalNode().ID

				// Simulate real CRASH by not sending leave messages
				// This forces other nodes to detect via probe timeout → Suspect → Failed
				fmt.Printf("\n🔴 INJECTING FAILURE: Crashing %s\n", failingNodeID)
				failTime := time.Now()

				// Use Crash() instead of Stop() to skip graceful leave announcement
				if err := nodes[targetNodeIndex].Crash(); err != nil {
					log.Printf("Error crashing node: %v", err)
					continue
				}

				// Set up detection callback for remaining nodes
				event := failureEvent{
					nodeIndex: targetNodeIndex,
					nodeID:    failingNodeID,
					failTime:  failTime,
				}

				// Wait for detection (monitor other nodes)
				go func(evt failureEvent) {
					detectionTimeout := time.After(15 * time.Second)
					checkTicker := time.NewTicker(500 * time.Millisecond)
					defer checkTicker.Stop()

					for {
						select {
						case <-detectionTimeout:
							fmt.Println("⚠️  Failure detection timeout")
							return
						case <-checkTicker.C:
							// Check if any OTHER node detected the departure
							for i, node := range nodes {
								if i == evt.nodeIndex {
									// Skip the stopped node itself
									continue
								}
								members := node.GetMembers()
								for _, m := range members {
									if m.ID == evt.nodeID {
										// After calling Crash(), nodes detect failure via timeout
										// They first mark as Suspect, then Failed after suspicion timeout
										if m.Status == swim.Suspect || m.Status == swim.Failed {
											detectTime := time.Now()
											detectionLatency := detectTime.Sub(evt.failTime)
											fmt.Printf("✓ Failure detected in %.2fs by %s (status: %v)\n",
												detectionLatency.Seconds(), node.LocalNode().ID, m.Status)
											metrics.RecordFailureDetection(detectionLatency)
											// Note: Suspicions are already recorded by SWIM protocol itself
											// in handleSuspicion() when nodes mark others as Suspect
											return
										}
									}
								}
							}
						}
					}
				}(event)

				// Wait 15 seconds to allow detection to complete
				// (probe interval 1s + suspicion timeout 5s + retries ~8s)
				time.Sleep(15 * time.Second)

				fmt.Printf("🟢 RECOVERY: Restarting %s\n\n", failingNodeID)
				rejoinStart := time.Now()

				// Recreate and restart the node
				nodeID := fmt.Sprintf("node%d", targetNodeIndex+1)
				bindAddr := fmt.Sprintf("127.0.0.1:%d", 7946+targetNodeIndex)

				config := swim.DefaultConfig()
				config.NodeID = nodeID
				config.BindAddr = bindAddr
				config.AdvertiseAddr = bindAddr
				config.Logger = &SimpleLogger{nodeID: nodeID, quiet: true}
				config.JoinNodes = []string{"127.0.0.1:7946"}

				newNode, err := swim.New(config)
				if err != nil {
					log.Printf("Failed to recreate node: %v", err)
					continue
				}
				newNode.SetMetrics(metrics)

				if err := newNode.Start(); err != nil {
					log.Printf("Failed to restart node: %v", err)
					continue
				}

				nodes[targetNodeIndex] = newNode

				// Wait for the node to actually rejoin the cluster
				// Check if other nodes see it as Alive
				rejoinDetected := false
				rejoinTimeout := time.After(20 * time.Second)
				rejoinTicker := time.NewTicker(500 * time.Millisecond)
				defer rejoinTicker.Stop()

				for !rejoinDetected {
					select {
					case <-rejoinTimeout:
						fmt.Printf("⚠️  Rejoin timeout for %s\n\n", nodeID)
						rejoinDetected = true
					case <-rejoinTicker.C:
						// Check if any other node sees this node as Alive
						for i, node := range nodes {
							if i == targetNodeIndex {
								continue
							}
							members := node.GetMembers()
							for _, m := range members {
								if m.ID == nodeID && m.Status == swim.Alive {
									rejoinLatency := time.Since(rejoinStart)
									metrics.RecordMemberJoin(rejoinLatency)
									fmt.Printf("✓ Node rejoined in %.2fs\n\n", rejoinLatency.Seconds())
									rejoinDetected = true
									break
								}
							}
							if rejoinDetected {
								break
							}
						}
					}
				}

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
			close(stopFailureInjection) // Stop failure injection
			return
		case <-ticker.C:
			elapsed := time.Since(startTime)
			report := metrics.GetReport(len(nodes))
			fmt.Printf("  [%.0fs] Metrics... (Detections: %d, Suspicions: %d, FP Rate: %.1f%%)\n",
				elapsed.Seconds(),
				report.FailureDetectionCount,
				report.SuspicionCount,
				report.FalsePositiveRate*100)
		}
	}
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
