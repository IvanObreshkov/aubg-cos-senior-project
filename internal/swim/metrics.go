package swim

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects performance metrics for SWIM operations
type Metrics struct {
	mu sync.RWMutex

	// Failure detection latencies (time from failure to detection)
	failureDetectionLatencies []time.Duration

	// Probe latencies (round-trip time for successful probes)
	probeLatencies []time.Duration

	// Gossip propagation latencies (time for updates to propagate)
	gossipPropagationLatencies []time.Duration

	// Message counters
	pingCount         atomic.Uint64
	ackCount          atomic.Uint64
	pingReqCount      atomic.Uint64
	indirectPingCount atomic.Uint64
	suspectMsgCount   atomic.Uint64
	aliveMsgCount     atomic.Uint64
	confirmMsgCount   atomic.Uint64
	totalMessagesIn   atomic.Uint64
	totalMessagesOut  atomic.Uint64

	// Protocol events
	failureDetectionCount atomic.Uint64
	falsePositiveCount    atomic.Uint64
	suspicionCount        atomic.Uint64

	// Throughput tracking
	startTime time.Time

	// Membership events timing
	memberJoinLatencies []time.Duration
	memberJoinMu        sync.Mutex
}

// NewMetrics creates a new metrics collector
func NewMetrics() *Metrics {
	return &Metrics{
		failureDetectionLatencies:  make([]time.Duration, 0, 1000),
		probeLatencies:             make([]time.Duration, 0, 10000),
		gossipPropagationLatencies: make([]time.Duration, 0, 10000),
		memberJoinLatencies:        make([]time.Duration, 0, 100),
		startTime:                  time.Now(),
	}
}

// RecordFailureDetection records the latency to detect a failure
func (m *Metrics) RecordFailureDetection(latency time.Duration) {
	m.mu.Lock()
	m.failureDetectionLatencies = append(m.failureDetectionLatencies, latency)
	m.mu.Unlock()
	m.failureDetectionCount.Add(1)
}

// RecordProbeLatency records the round-trip time for a successful probe
func (m *Metrics) RecordProbeLatency(latency time.Duration) {
	m.mu.Lock()
	m.probeLatencies = append(m.probeLatencies, latency)
	m.mu.Unlock()
}

// RecordGossipPropagation records the time for a gossip update to propagate
func (m *Metrics) RecordGossipPropagation(latency time.Duration) {
	m.mu.Lock()
	m.gossipPropagationLatencies = append(m.gossipPropagationLatencies, latency)
	m.mu.Unlock()
}

// RecordMemberJoin records the time for a new member to join
func (m *Metrics) RecordMemberJoin(latency time.Duration) {
	m.memberJoinMu.Lock()
	m.memberJoinLatencies = append(m.memberJoinLatencies, latency)
	m.memberJoinMu.Unlock()
}

// RecordPing increments the ping message counter
func (m *Metrics) RecordPing() {
	m.pingCount.Add(1)
	m.totalMessagesOut.Add(1)
}

// RecordAck increments the ack message counter
func (m *Metrics) RecordAck() {
	m.ackCount.Add(1)
	m.totalMessagesOut.Add(1)
}

// RecordPingReq increments the ping-req message counter
func (m *Metrics) RecordPingReq() {
	m.pingReqCount.Add(1)
	m.totalMessagesOut.Add(1)
}

// RecordIndirectPing increments the indirect ping message counter
func (m *Metrics) RecordIndirectPing() {
	m.indirectPingCount.Add(1)
	m.totalMessagesOut.Add(1)
}

// RecordSuspectMsg increments the suspect message counter
func (m *Metrics) RecordSuspectMsg() {
	m.suspectMsgCount.Add(1)
	m.totalMessagesOut.Add(1)
}

// RecordAliveMsg increments the alive message counter
func (m *Metrics) RecordAliveMsg() {
	m.aliveMsgCount.Add(1)
	m.totalMessagesOut.Add(1)
}

// RecordConfirmMsg increments the confirm message counter
func (m *Metrics) RecordConfirmMsg() {
	m.confirmMsgCount.Add(1)
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

// RecordSuspicion records when a member is suspected
func (m *Metrics) RecordSuspicion() {
	m.suspicionCount.Add(1)
}

// RecordFalsePositive records when a suspected member turns out to be alive
func (m *Metrics) RecordFalsePositive() {
	m.falsePositiveCount.Add(1)
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

// GetFailureDetectionStats returns statistics for failure detection
func (m *Metrics) GetFailureDetectionStats() LatencyStats {
	m.mu.RLock()
	latencies := make([]time.Duration, len(m.failureDetectionLatencies))
	copy(latencies, m.failureDetectionLatencies)
	m.mu.RUnlock()
	return computeLatencyStats(latencies)
}

// GetProbeLatencyStats returns statistics for probe latencies
func (m *Metrics) GetProbeLatencyStats() LatencyStats {
	m.mu.RLock()
	latencies := make([]time.Duration, len(m.probeLatencies))
	copy(latencies, m.probeLatencies)
	m.mu.RUnlock()
	return computeLatencyStats(latencies)
}

// GetGossipPropagationStats returns statistics for gossip propagation
func (m *Metrics) GetGossipPropagationStats() LatencyStats {
	m.mu.RLock()
	latencies := make([]time.Duration, len(m.gossipPropagationLatencies))
	copy(latencies, m.gossipPropagationLatencies)
	m.mu.RUnlock()
	return computeLatencyStats(latencies)
}

// GetMemberJoinStats returns statistics for member join latencies
func (m *Metrics) GetMemberJoinStats() LatencyStats {
	m.memberJoinMu.Lock()
	latencies := make([]time.Duration, len(m.memberJoinLatencies))
	copy(latencies, m.memberJoinLatencies)
	m.memberJoinMu.Unlock()
	return computeLatencyStats(latencies)
}

// GetMessageThroughput returns the message throughput in messages/second
func (m *Metrics) GetMessageThroughput() float64 {
	elapsed := time.Since(m.startTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	totalMessages := m.totalMessagesIn.Load() + m.totalMessagesOut.Load()
	return float64(totalMessages) / elapsed
}

// Report contains all collected SWIM metrics
type Report struct {
	// Test configuration
	ClusterSize  int       `json:"cluster_size"`
	TestDuration float64   `json:"test_duration_seconds"`
	StartTime    time.Time `json:"start_time"`
	EndTime      time.Time `json:"end_time"`

	// NFR1 - Performance Metrics
	FailureDetectionLatency  LatencyStats `json:"failure_detection_latency"`
	ProbeLatency             LatencyStats `json:"probe_latency"`
	GossipPropagationLatency LatencyStats `json:"gossip_propagation_latency"`
	MemberJoinLatency        LatencyStats `json:"member_join_latency"`

	// NFR2 - Throughput and Message Load
	TotalMessagesIn   uint64  `json:"total_messages_in"`
	TotalMessagesOut  uint64  `json:"total_messages_out"`
	MessageThroughput float64 `json:"message_throughput_msg_per_sec"`
	MessagesPerNode   float64 `json:"messages_per_node"`

	// Message breakdown
	PingCount         uint64 `json:"ping_count"`
	AckCount          uint64 `json:"ack_count"`
	PingReqCount      uint64 `json:"ping_req_count"`
	IndirectPingCount uint64 `json:"indirect_ping_count"`
	SuspectMsgCount   uint64 `json:"suspect_msg_count"`
	AliveMsgCount     uint64 `json:"alive_msg_count"`
	ConfirmMsgCount   uint64 `json:"confirm_msg_count"`

	// Protocol metrics
	FailureDetectionCount uint64  `json:"failure_detection_count"`
	SuspicionCount        uint64  `json:"suspicion_count"`
	FalsePositiveCount    uint64  `json:"false_positive_count"`
	FalsePositiveRate     float64 `json:"false_positive_rate"`
}

// GetReport generates a comprehensive performance report
func (m *Metrics) GetReport(clusterSize int) Report {
	endTime := time.Now()
	duration := endTime.Sub(m.startTime).Seconds()

	totalMessagesOut := m.totalMessagesOut.Load()
	totalMessagesIn := m.totalMessagesIn.Load()
	messagesPerNode := 0.0
	if clusterSize > 0 {
		messagesPerNode = float64(totalMessagesOut) / float64(clusterSize)
	}

	suspicions := m.suspicionCount.Load()
	falsePositives := m.falsePositiveCount.Load()
	falsePositiveRate := 0.0
	if suspicions > 0 {
		falsePositiveRate = float64(falsePositives) / float64(suspicions)
	}

	return Report{
		ClusterSize:              clusterSize,
		TestDuration:             duration,
		StartTime:                m.startTime,
		EndTime:                  endTime,
		FailureDetectionLatency:  m.GetFailureDetectionStats(),
		ProbeLatency:             m.GetProbeLatencyStats(),
		GossipPropagationLatency: m.GetGossipPropagationStats(),
		MemberJoinLatency:        m.GetMemberJoinStats(),
		TotalMessagesIn:          totalMessagesIn,
		TotalMessagesOut:         totalMessagesOut,
		MessageThroughput:        m.GetMessageThroughput(),
		MessagesPerNode:          messagesPerNode,
		PingCount:                m.pingCount.Load(),
		AckCount:                 m.ackCount.Load(),
		PingReqCount:             m.pingReqCount.Load(),
		IndirectPingCount:        m.indirectPingCount.Load(),
		SuspectMsgCount:          m.suspectMsgCount.Load(),
		AliveMsgCount:            m.aliveMsgCount.Load(),
		ConfirmMsgCount:          m.confirmMsgCount.Load(),
		FailureDetectionCount:    m.failureDetectionCount.Load(),
		SuspicionCount:           m.suspicionCount.Load(),
		FalsePositiveCount:       falsePositives,
		FalsePositiveRate:        falsePositiveRate,
	}
}

// PrintReport prints the report in a human-readable format
func (r *Report) PrintReport() {
	fmt.Println("\n========================================")
	fmt.Println("SWIM PERFORMANCE REPORT")
	fmt.Println("========================================")
	fmt.Printf("\nTest Configuration:\n")
	fmt.Printf("  Cluster Size: %d nodes\n", r.ClusterSize)
	fmt.Printf("  Duration: %.2f seconds\n", r.TestDuration)
	fmt.Printf("  Start: %s\n", r.StartTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("  End: %s\n", r.EndTime.Format("2006-01-02 15:04:05"))

	fmt.Printf("\n----------------------------------------\n")
	fmt.Printf("NFR1 - Performance Metrics (Latency)\n")
	fmt.Printf("----------------------------------------\n")

	fmt.Printf("\nFailure Detection Latency:\n")
	printLatencyStats(r.FailureDetectionLatency)

	fmt.Printf("\nProbe Round-Trip Latency:\n")
	printLatencyStats(r.ProbeLatency)

	fmt.Printf("\nGossip Propagation Latency:\n")
	printLatencyStats(r.GossipPropagationLatency)

	fmt.Printf("\nMember Join Latency:\n")
	printLatencyStats(r.MemberJoinLatency)

	fmt.Printf("\n----------------------------------------\n")
	fmt.Printf("NFR2 - Scalability Metrics (Throughput & Message Load)\n")
	fmt.Printf("----------------------------------------\n")

	fmt.Printf("\nMessage Throughput:\n")
	fmt.Printf("  Total Messages In: %d\n", r.TotalMessagesIn)
	fmt.Printf("  Total Messages Out: %d\n", r.TotalMessagesOut)
	fmt.Printf("  Total Messages: %d\n", r.TotalMessagesIn+r.TotalMessagesOut)
	fmt.Printf("  Throughput: %.2f msg/sec\n", r.MessageThroughput)
	fmt.Printf("  Messages per Node: %.2f\n", r.MessagesPerNode)

	fmt.Printf("\nMessage Breakdown:\n")
	fmt.Printf("  Ping: %d\n", r.PingCount)
	fmt.Printf("  Ack: %d\n", r.AckCount)
	fmt.Printf("  PingReq: %d\n", r.PingReqCount)
	fmt.Printf("  IndirectPing: %d\n", r.IndirectPingCount)
	fmt.Printf("  Suspect: %d\n", r.SuspectMsgCount)
	fmt.Printf("  Alive: %d\n", r.AliveMsgCount)
	fmt.Printf("  Confirm: %d\n", r.ConfirmMsgCount)

	fmt.Printf("\n----------------------------------------\n")
	fmt.Printf("Protocol Metrics\n")
	fmt.Printf("----------------------------------------\n")
	fmt.Printf("\nFailure Detection:\n")
	fmt.Printf("  Detections: %d\n", r.FailureDetectionCount)
	fmt.Printf("  Suspicions: %d\n", r.SuspicionCount)
	fmt.Printf("  False Positives: %d\n", r.FalsePositiveCount)
	fmt.Printf("  False Positive Rate: %.2f%%\n", r.FalsePositiveRate*100)

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

// SaveJSON saves the report to a JSON file
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
	m.failureDetectionLatencies = make([]time.Duration, 0, 1000)
	m.probeLatencies = make([]time.Duration, 0, 10000)
	m.gossipPropagationLatencies = make([]time.Duration, 0, 10000)
	m.mu.Unlock()

	m.memberJoinMu.Lock()
	m.memberJoinLatencies = make([]time.Duration, 0, 100)
	m.memberJoinMu.Unlock()

	m.pingCount.Store(0)
	m.ackCount.Store(0)
	m.pingReqCount.Store(0)
	m.indirectPingCount.Store(0)
	m.suspectMsgCount.Store(0)
	m.aliveMsgCount.Store(0)
	m.confirmMsgCount.Store(0)
	m.totalMessagesIn.Store(0)
	m.totalMessagesOut.Store(0)
	m.failureDetectionCount.Store(0)
	m.falsePositiveCount.Store(0)
	m.suspicionCount.Store(0)
	m.startTime = time.Now()
}
