package main

import (
	"aubg-cos-senior-project/internal/tob"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// SimpleLogger implements the tob.Logger interface
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
	numMessages := flag.Int("messages", 100, "Number of messages to broadcast")
	testDuration := flag.Int("duration", 60, "Test duration in seconds (for continuous mode)")
	outputFile := flag.String("output", "", "Output JSON file for metrics (optional)")
	quiet := flag.Bool("quiet", false, "Reduce log verbosity")
	continuous := flag.Bool("continuous", false, "Continuous broadcast mode (uses duration)")
	flag.Parse()

	if *clusterSize < 3 {
		log.Fatal("Cluster size must be at least 3")
	}

	fmt.Printf("\n")
	fmt.Println("========================================")
	fmt.Println("TOTAL ORDER BROADCAST PERFORMANCE BENCHMARK")
	fmt.Println("========================================")
	fmt.Printf("Cluster Size: %d nodes\n", *clusterSize)
	if *continuous {
		fmt.Printf("Mode: Continuous broadcast for %d seconds\n", *testDuration)
	} else {
		fmt.Printf("Messages to broadcast: %d\n", *numMessages)
	}
	fmt.Println("========================================")
	fmt.Println()

	basePort := 8000

	// Create shared metrics collector
	sharedMetrics := tob.NewMetrics()

	// Create cluster
	fmt.Println("🚀 Creating cluster...")
	nodes := createCluster(*clusterSize, basePort, sharedMetrics, *quiet)

	// Start all nodes
	fmt.Println("🚀 Starting cluster...")
	for i, node := range nodes {
		if err := node.Start(); err != nil {
			log.Fatalf("Failed to start node %d: %v", i, err)
		}
	}
	fmt.Println("✓ Cluster started")

	// Wait for cluster to stabilize
	fmt.Println("⏳ Waiting for cluster to stabilize...")
	time.Sleep(3 * time.Second)
	fmt.Println("✓ Cluster stabilized")

	// Identify sequencer
	sequencerID := ""
	for _, node := range nodes {
		if node.IsSequencer() {
			sequencerID = node.GetCurrentSequencer()
			break
		}
	}
	fmt.Printf("✓ Sequencer: %s\n", sequencerID)
	fmt.Println()

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
	fmt.Println("========================================")
	fmt.Println("RUNNING BENCHMARK")
	fmt.Println("========================================")
	fmt.Println()

	if *continuous {
		runContinuousBenchmark(nodes, *testDuration, sharedMetrics)
	} else {
		runBatchBenchmark(nodes, *numMessages, sharedMetrics)
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

func createCluster(clusterSize int, basePort int, metrics *tob.Metrics, quiet bool) []*tob.TOBroadcast {
	nodes := make([]*tob.TOBroadcast, 0, clusterSize)

	// Build list of all node addresses
	var nodeAddrs []string
	for i := 0; i < clusterSize; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)
		nodeAddrs = append(nodeAddrs, addr)
	}

	// Create all nodes
	for i := 0; i < clusterSize; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		bindAddr := fmt.Sprintf("127.0.0.1:%d", basePort+i)

		config := tob.DefaultConfig()
		config.NodeID = nodeID
		config.BindAddr = bindAddr
		config.AdvertiseAddr = bindAddr
		config.Nodes = nodeAddrs
		config.Logger = &SimpleLogger{nodeID: nodeID, quiet: quiet}

		// First node is the initial sequencer
		if i == 0 {
			config.IsSequencer = true
			config.SequencerID = nodeID
			config.SequencerAddr = bindAddr
		} else {
			config.IsSequencer = false
			config.SequencerID = "node1"
			config.SequencerAddr = fmt.Sprintf("127.0.0.1:%d", basePort)
		}

		node, err := tob.New(config)
		if err != nil {
			log.Fatalf("Failed to create node %s: %v", nodeID, err)
		}

		// Set shared metrics
		node.SetMetrics(metrics)

		nodes = append(nodes, node)
	}

	return nodes
}

// runBatchBenchmark broadcasts a fixed number of messages
func runBatchBenchmark(nodes []*tob.TOBroadcast, numMessages int, metrics *tob.Metrics) {
	fmt.Printf("Broadcasting %d messages...\n", numMessages)
	fmt.Println("📊 Measuring end-to-end latency (broadcast to delivery)")
	fmt.Println()

	var deliveredCount atomic.Uint64
	var mu sync.Mutex
	deliveryTimes := make(map[string]time.Time)

	// Set up delivery callback to track when messages are delivered
	for _, node := range nodes {
		node.SetDeliveryCallback(func(msg *tob.Message) {
			mu.Lock()
			if _, exists := deliveryTimes[msg.MessageID]; !exists {
				deliveryTimes[msg.MessageID] = time.Now()
				count := deliveredCount.Add(1)
				if count%10 == 0 || count == uint64(numMessages) {
					fmt.Printf("  Delivered: %d/%d messages\n", count, numMessages)
				}
			}
			mu.Unlock()
		})
	}

	startTime := time.Now()

	// Broadcast messages from random nodes
	for i := 0; i < numMessages; i++ {
		// Pick a random non-sequencer node to broadcast from
		nodeIdx := (i % (len(nodes) - 1)) + 1
		if nodeIdx >= len(nodes) {
			nodeIdx = 0
		}

		payload := []byte(fmt.Sprintf("message-%d", i))

		if err := nodes[nodeIdx].Broadcast(payload); err != nil {
			log.Printf("Failed to broadcast message %d: %v", i, err)
		}

		// Small delay between broadcasts to avoid overwhelming the sequencer
		time.Sleep(10 * time.Millisecond)
	}

	fmt.Println("\n✓ All messages broadcast, waiting for delivery...")

	// Wait for all messages to be delivered (with timeout)
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			fmt.Printf("⚠️  Timeout waiting for delivery. Delivered: %d/%d\n",
				deliveredCount.Load(), numMessages)
			return
		case <-ticker.C:
			if deliveredCount.Load() >= uint64(numMessages) {
				elapsed := time.Since(startTime)
				fmt.Printf("\n✓ All messages delivered in %.2f seconds\n", elapsed.Seconds())
				return
			}
		}
	}
}

// runContinuousBenchmark continuously broadcasts messages for a duration
func runContinuousBenchmark(nodes []*tob.TOBroadcast, durationSeconds int, metrics *tob.Metrics) {
	fmt.Printf("Continuously broadcasting for %d seconds...\n", durationSeconds)
	fmt.Println("📊 This measures STEADY-STATE performance:")
	fmt.Println("   - Message throughput (broadcasts/sec)")
	fmt.Println("   - End-to-end latency (P50/P95/P99)")
	fmt.Println("   - Sequencer processing latency")
	fmt.Println()

	startTime := time.Now()
	duration := time.Duration(durationSeconds) * time.Second

	var broadcastCount atomic.Uint64
	var deliveredCount atomic.Uint64

	// Set up delivery callback
	for _, node := range nodes {
		node.SetDeliveryCallback(func(msg *tob.Message) {
			deliveredCount.Add(1)
		})
	}

	// Broadcast continuously from multiple nodes
	stopBroadcast := make(chan bool)
	var wg sync.WaitGroup

	// Start broadcasters on different nodes
	for nodeIdx := 1; nodeIdx < len(nodes); nodeIdx++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			msgCounter := 0
			for {
				select {
				case <-stopBroadcast:
					return
				default:
					payload := []byte(fmt.Sprintf("node%d-msg-%d", idx, msgCounter))
					if err := nodes[idx].Broadcast(payload); err != nil {
						log.Printf("Broadcast error from node %d: %v", idx, err)
					}
					broadcastCount.Add(1)
					msgCounter++
					time.Sleep(50 * time.Millisecond) // Moderate broadcast rate
				}
			}
		}(nodeIdx)
	}

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
			close(stopBroadcast)
			wg.Wait()
			fmt.Printf("\n✓ Benchmark complete\n")
			fmt.Printf("  Total broadcast: %d messages\n", broadcastCount.Load())
			fmt.Printf("  Total delivered: %d messages\n", deliveredCount.Load())
			return
		case <-ticker.C:
			elapsed := time.Since(startTime)
			report := metrics.GetReport(len(nodes))
			fmt.Printf("  [%.0fs] Broadcast: %d, Delivered: %d, Throughput: %.2f msg/s\n",
				elapsed.Seconds(),
				broadcastCount.Load(),
				deliveredCount.Load(),
				report.MessageThroughput)
		}
	}
}

func saveReportJSON(report *tob.Report, filename string) {
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
