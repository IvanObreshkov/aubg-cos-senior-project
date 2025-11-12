package tob

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects performance metrics for Total Order Broadcast operations
type Metrics struct {
	mu sync.RWMutex

	// Broadcast latencies (time from broadcast to delivery)
	broadcastLatencies []time.Duration

	// Sequencing latencies (time from sequencer receiving to multicasting)
	sequencingLatencies []time.Duration

	// Message counters
	dataMsgCount      atomic.Uint64
	sequencedMsgCount atomic.Uint64
	heartbeatMsgCount atomic.Uint64
	totalMessagesIn   atomic.Uint64
	totalMessagesOut  atomic.Uint64

	// Protocol events
	messagesDelivered atomic.Uint64
	outOfOrderCount   atomic.Uint64
	sequenceGapCount  atomic.Uint64

	// Throughput tracking
	startTime time.Time
}

// NewMetrics creates a new metrics collector
func NewMetrics() *Metrics {
	return &Metrics{
		broadcastLatencies:  make([]time.Duration, 0, 10000),
		sequencingLatencies: make([]time.Duration, 0, 10000),
		startTime:           time.Now(),
	}
}

// RecordBroadcastLatency records the end-to-end latency from broadcast to delivery
func (m *Metrics) RecordBroadcastLatency(latency time.Duration) {
	m.mu.Lock()
	m.broadcastLatencies = append(m.broadcastLatencies, latency)
	m.mu.Unlock()
}

// RecordSequencingLatency records the time for sequencer to process and multicast a message
func (m *Metrics) RecordSequencingLatency(latency time.Duration) {
	m.mu.Lock()
	m.sequencingLatencies = append(m.sequencingLatencies, latency)
	m.mu.Unlock()
}

// RecordDataMsg increments the data message counter
func (m *Metrics) RecordDataMsg() {
	m.dataMsgCount.Add(1)
	m.totalMessagesOut.Add(1)
}

// RecordSequencedMsg increments the sequenced message counter
func (m *Metrics) RecordSequencedMsg() {
	m.sequencedMsgCount.Add(1)
	m.totalMessagesOut.Add(1)
}

// RecordHeartbeatMsg increments the heartbeat message counter
func (m *Metrics) RecordHeartbeatMsg() {
	m.heartbeatMsgCount.Add(1)
	m.totalMessagesOut.Add(1)
}

// RecordMessageIn increments the incoming message counter
func (m *Metrics) RecordMessageIn() {
	m.totalMessagesIn.Add(1)
}

// RecordMessageOut increments the outgoing message counter
func (m *Metrics) RecordMessageOut() {
	m.totalMessagesOut.Add(1)
}

// RecordMessageDelivered increments the messages delivered counter
func (m *Metrics) RecordMessageDelivered() {
	m.messagesDelivered.Add(1)
}

// RecordOutOfOrder records when a message is received out of order
func (m *Metrics) RecordOutOfOrder() {
	m.outOfOrderCount.Add(1)
}

// RecordSequenceGap records when a gap in sequence numbers is detected
func (m *Metrics) RecordSequenceGap() {
	m.sequenceGapCount.Add(1)
}

// LatencyStats contains percentile statistics for latencies
type LatencyStats struct {
	Count  int     `json:"count"`
	Min    float64 `json:"min_ms"`
	Max    float64 `json:"max_ms"`
	Mean   float64 `json:"mean_ms"`
	P50    float64 `json:"p50_ms"`
	P95    float64 `json:"p95_ms"`
	P99    float64 `json:"p99_ms"`
	StdDev float64 `json:"stddev_ms"`
}

// computeLatencyStats computes percentile statistics from latencies
func computeLatencyStats(latencies []time.Duration) LatencyStats {
	if len(latencies) == 0 {
		return LatencyStats{}
	}

	// Sort for percentile calculation
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	// Convert to milliseconds
	latenciesMs := make([]float64, len(sorted))
	var sum float64
	for i, lat := range sorted {
		ms := float64(lat.Microseconds()) / 1000.0
		latenciesMs[i] = ms
		sum += ms
	}

	mean := sum / float64(len(latenciesMs))

	// Calculate standard deviation
	var variance float64
	for _, lat := range latenciesMs {
		diff := lat - mean
		variance += diff * diff
	}
	stddev := math.Sqrt(variance / float64(len(latenciesMs)))

	return LatencyStats{
		Count:  len(sorted),
		Min:    latenciesMs[0],
		Max:    latenciesMs[len(latenciesMs)-1],
		Mean:   mean,
		P50:    percentile(latenciesMs, 50),
		P95:    percentile(latenciesMs, 95),
		P99:    percentile(latenciesMs, 99),
		StdDev: stddev,
	}
}

// percentile calculates the nth percentile from sorted data
func percentile(sorted []float64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := float64(p) / 100.0 * float64(len(sorted)-1)
	lower := int(math.Floor(index))
	upper := int(math.Ceil(index))
	if lower == upper {
		return sorted[lower]
	}
	// Linear interpolation
	weight := index - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

// GetBroadcastLatencyStats returns statistics for broadcast latencies
func (m *Metrics) GetBroadcastLatencyStats() LatencyStats {
	m.mu.RLock()
	latencies := make([]time.Duration, len(m.broadcastLatencies))
	copy(latencies, m.broadcastLatencies)
	m.mu.RUnlock()
	return computeLatencyStats(latencies)
}

// GetSequencingLatencyStats returns statistics for sequencing latencies
func (m *Metrics) GetSequencingLatencyStats() LatencyStats {
	m.mu.RLock()
	latencies := make([]time.Duration, len(m.sequencingLatencies))
	copy(latencies, m.sequencingLatencies)
	m.mu.RUnlock()
	return computeLatencyStats(latencies)
}

// GetMessageThroughput returns the message throughput in messages/second
func (m *Metrics) GetMessageThroughput() float64 {
	elapsed := time.Since(m.startTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(m.messagesDelivered.Load()) / elapsed
}

// GetBroadcastThroughput returns the broadcast throughput in broadcasts/second
func (m *Metrics) GetBroadcastThroughput() float64 {
	elapsed := time.Since(m.startTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(m.dataMsgCount.Load()) / elapsed
}

// Report contains all collected TOB metrics
type Report struct {
	// Test configuration
	ClusterSize  int       `json:"cluster_size"`
	TestDuration float64   `json:"test_duration_seconds"`
	StartTime    time.Time `json:"start_time"`
	EndTime      time.Time `json:"end_time"`

	// NFR1 - Performance Metrics (Latency)
	BroadcastLatency  LatencyStats `json:"broadcast_latency"`
	SequencingLatency LatencyStats `json:"sequencing_latency"`

	// NFR2 - Throughput and Message Load
	MessagesDelivered   uint64  `json:"messages_delivered"`
	MessageThroughput   float64 `json:"message_throughput_msg_per_sec"`
	BroadcastThroughput float64 `json:"broadcast_throughput_broadcasts_per_sec"`
	TotalMessagesIn     uint64  `json:"total_messages_in"`
	TotalMessagesOut    uint64  `json:"total_messages_out"`
	MessagesPerNode     float64 `json:"messages_per_node"`

	// Message breakdown
	DataMsgCount      uint64 `json:"data_msg_count"`
	SequencedMsgCount uint64 `json:"sequenced_msg_count"`
	HeartbeatMsgCount uint64 `json:"heartbeat_msg_count"`

	// Protocol metrics
	OutOfOrderCount  uint64 `json:"out_of_order_count"`
	SequenceGapCount uint64 `json:"sequence_gap_count"`

	// Ordering guarantees
	OrderingViolations uint64  `json:"ordering_violations"`
	OrderingAccuracy   float64 `json:"ordering_accuracy_percent"`
}

// GetReport generates a comprehensive performance report
func (m *Metrics) GetReport(clusterSize int) Report {
	endTime := time.Now()
	duration := endTime.Sub(m.startTime).Seconds()

	totalMessagesOut := m.totalMessagesOut.Load()
	totalMessagesIn := m.totalMessagesIn.Load()
	messagesDelivered := m.messagesDelivered.Load()

	messagesPerNode := 0.0
	if clusterSize > 0 {
		messagesPerNode = float64(totalMessagesOut) / float64(clusterSize)
	}

	// Calculate ordering accuracy
	outOfOrder := m.outOfOrderCount.Load()
	orderingAccuracy := 100.0
	if messagesDelivered > 0 {
		orderingAccuracy = (1.0 - float64(outOfOrder)/float64(messagesDelivered)) * 100.0
	}

	return Report{
		ClusterSize:         clusterSize,
		TestDuration:        duration,
		StartTime:           m.startTime,
		EndTime:             endTime,
		BroadcastLatency:    m.GetBroadcastLatencyStats(),
		SequencingLatency:   m.GetSequencingLatencyStats(),
		MessagesDelivered:   messagesDelivered,
		MessageThroughput:   m.GetMessageThroughput(),
		BroadcastThroughput: m.GetBroadcastThroughput(),
		TotalMessagesIn:     totalMessagesIn,
		TotalMessagesOut:    totalMessagesOut,
		MessagesPerNode:     messagesPerNode,
		DataMsgCount:        m.dataMsgCount.Load(),
		SequencedMsgCount:   m.sequencedMsgCount.Load(),
		HeartbeatMsgCount:   m.heartbeatMsgCount.Load(),
		OutOfOrderCount:     outOfOrder,
		SequenceGapCount:    m.sequenceGapCount.Load(),
		OrderingViolations:  outOfOrder,
		OrderingAccuracy:    orderingAccuracy,
	}
}

// PrintReport prints the report in a human-readable format
func (r *Report) PrintReport() {
	fmt.Println("\n========================================")
	fmt.Println("TOTAL ORDER BROADCAST PERFORMANCE REPORT")
	fmt.Println("========================================")
	fmt.Printf("\nTest Configuration:\n")
	fmt.Printf("  Cluster Size: %d nodes\n", r.ClusterSize)
	fmt.Printf("  Duration: %.2f seconds\n", r.TestDuration)
	fmt.Printf("  Start: %s\n", r.StartTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("  End: %s\n", r.EndTime.Format("2006-01-02 15:04:05"))

	fmt.Printf("\n----------------------------------------\n")
	fmt.Printf("NFR1 - Performance Metrics (Latency)\n")
	fmt.Printf("----------------------------------------\n")

	fmt.Printf("\nBroadcast End-to-End Latency:\n")
	fmt.Printf("  (Time from broadcast submission to total order delivery)\n")
	printLatencyStats(r.BroadcastLatency)

	fmt.Printf("\nSequencer Processing Latency:\n")
	fmt.Printf("  (Time for sequencer to assign number and multicast)\n")
	printLatencyStats(r.SequencingLatency)

	fmt.Printf("\n----------------------------------------\n")
	fmt.Printf("NFR2 - Scalability Metrics (Throughput & Message Load)\n")
	fmt.Printf("----------------------------------------\n")

	fmt.Printf("\nThroughput:\n")
	fmt.Printf("  Messages Delivered: %d\n", r.MessagesDelivered)
	fmt.Printf("  Message Throughput: %.2f msg/sec\n", r.MessageThroughput)
	fmt.Printf("  Broadcast Throughput: %.2f broadcasts/sec\n", r.BroadcastThroughput)

	fmt.Printf("\nMessage Load:\n")
	fmt.Printf("  Total Messages In: %d\n", r.TotalMessagesIn)
	fmt.Printf("  Total Messages Out: %d\n", r.TotalMessagesOut)
	fmt.Printf("  Total Messages: %d\n", r.TotalMessagesIn+r.TotalMessagesOut)
	fmt.Printf("  Messages per Node: %.2f\n", r.MessagesPerNode)

	fmt.Printf("\nMessage Breakdown:\n")
	fmt.Printf("  Data Messages: %d\n", r.DataMsgCount)
	fmt.Printf("  Sequenced Messages: %d\n", r.SequencedMsgCount)
	fmt.Printf("  Heartbeat Messages: %d\n", r.HeartbeatMsgCount)

	fmt.Printf("\n----------------------------------------\n")
	fmt.Printf("Protocol Metrics\n")
	fmt.Printf("----------------------------------------\n")

	fmt.Printf("\nOrdering Guarantees:\n")
	fmt.Printf("  Out-of-Order Messages: %d\n", r.OutOfOrderCount)
	fmt.Printf("  Sequence Gaps Detected: %d\n", r.SequenceGapCount)
	fmt.Printf("  Ordering Accuracy: %.4f%%\n", r.OrderingAccuracy)

	fmt.Println("\n========================================")
}

func printLatencyStats(stats LatencyStats) {
	if stats.Count > 0 {
		fmt.Printf("  Count: %d\n", stats.Count)
		fmt.Printf("  Min: %.3f ms\n", stats.Min)
		fmt.Printf("  Mean: %.3f ms\n", stats.Mean)
		fmt.Printf("  P50: %.3f ms\n", stats.P50)
		fmt.Printf("  P95: %.3f ms\n", stats.P95)
		fmt.Printf("  P99: %.3f ms\n", stats.P99)
		fmt.Printf("  Max: %.3f ms\n", stats.Max)
		fmt.Printf("  StdDev: %.3f ms\n", stats.StdDev)
	} else {
		fmt.Printf("  No data collected\n")
	}
}

// SaveJSON saves the report to a JSON file (prints to stdout for now)
func (r *Report) SaveJSON(filename string) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal report: %w", err)
	}

	fmt.Printf("\nSaving metrics to %s...\n", filename)
	fmt.Println(string(data))
	return nil
}

// Reset clears all collected metrics
func (m *Metrics) Reset() {
	m.mu.Lock()
	m.broadcastLatencies = make([]time.Duration, 0, 10000)
	m.sequencingLatencies = make([]time.Duration, 0, 10000)
	m.mu.Unlock()

	m.dataMsgCount.Store(0)
	m.sequencedMsgCount.Store(0)
	m.heartbeatMsgCount.Store(0)
	m.totalMessagesIn.Store(0)
	m.totalMessagesOut.Store(0)
	m.messagesDelivered.Store(0)
	m.outOfOrderCount.Store(0)
	m.sequenceGapCount.Store(0)
	m.startTime = time.Now()
}
