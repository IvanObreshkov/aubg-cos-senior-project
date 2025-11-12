package metrics

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects performance metrics for Raft operations
type Metrics struct {
	mu sync.RWMutex

	// Command latencies (time from submission to commit)
	commandLatencies []time.Duration

	// RPC counters
	appendEntriesCount atomic.Uint64
	requestVoteCount   atomic.Uint64
	heartbeatCount     atomic.Uint64

	// Throughput tracking
	commandsCommitted atomic.Uint64
	startTime         time.Time

	// Leader election metrics
	electionCount    atomic.Uint64
	electionDuration []time.Duration
	electionMu       sync.Mutex
}

// NewMetrics creates a new metrics collector
func NewMetrics() *Metrics {
	return &Metrics{
		commandLatencies: make([]time.Duration, 0, 10000), // Pre-allocate for performance
		electionDuration: make([]time.Duration, 0, 100),
		startTime:        time.Now(),
	}
}

// RecordCommandLatency records the latency of a single command from submission to commit
func (m *Metrics) RecordCommandLatency(latency time.Duration) {
	m.mu.Lock()
	m.commandLatencies = append(m.commandLatencies, latency)
	m.mu.Unlock()
}

// RecordCommandCommitted increments the count of committed commands
func (m *Metrics) RecordCommandCommitted() {
	m.commandsCommitted.Add(1)
}

// RecordAppendEntries increments the AppendEntries RPC counter
func (m *Metrics) RecordAppendEntries() {
	m.appendEntriesCount.Add(1)
}

// RecordRequestVote increments the RequestVote RPC counter
func (m *Metrics) RecordRequestVote() {
	m.requestVoteCount.Add(1)
}

// RecordHeartbeat increments the heartbeat counter
func (m *Metrics) RecordHeartbeat() {
	m.heartbeatCount.Add(1)
}

// RecordElection records a leader election occurrence
func (m *Metrics) RecordElection() {
	m.electionCount.Add(1)
}

// RecordElectionDuration records how long an election took
func (m *Metrics) RecordElectionDuration(duration time.Duration) {
	m.electionMu.Lock()
	m.electionDuration = append(m.electionDuration, duration)
	m.electionMu.Unlock()
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

// GetLatencyStats computes percentile statistics from recorded latencies
func (m *Metrics) GetLatencyStats() LatencyStats {
	m.mu.RLock()
	latencies := make([]time.Duration, len(m.commandLatencies))
	copy(latencies, m.commandLatencies)
	m.mu.RUnlock()

	if len(latencies) == 0 {
		return LatencyStats{}
	}

	// Sort for percentile calculation
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	// Convert to milliseconds
	latenciesMs := make([]float64, len(latencies))
	var sum float64
	for i, lat := range latencies {
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
		Count:  len(latencies),
		Min:    latenciesMs[0],
		Max:    latenciesMs[len(latenciesMs)-1],
		Mean:   mean,
		P50:    percentile(latenciesMs, 50),
		P95:    percentile(latenciesMs, 95),
		P99:    percentile(latenciesMs, 99),
		StdDev: stddev,
	}
}

// GetElectionStats returns statistics about leader elections
func (m *Metrics) GetElectionStats() LatencyStats {
	m.electionMu.Lock()
	durations := make([]time.Duration, len(m.electionDuration))
	copy(durations, m.electionDuration)
	m.electionMu.Unlock()

	if len(durations) == 0 {
		return LatencyStats{}
	}

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	durationsMs := make([]float64, len(durations))
	var sum float64
	for i, dur := range durations {
		ms := float64(dur.Microseconds()) / 1000.0
		durationsMs[i] = ms
		sum += ms
	}

	mean := sum / float64(len(durationsMs))

	var variance float64
	for _, dur := range durationsMs {
		diff := dur - mean
		variance += diff * diff
	}
	stddev := math.Sqrt(variance / float64(len(durationsMs)))

	return LatencyStats{
		Count:  len(durations),
		Min:    durationsMs[0],
		Max:    durationsMs[len(durationsMs)-1],
		Mean:   mean,
		P50:    percentile(durationsMs, 50),
		P95:    percentile(durationsMs, 95),
		P99:    percentile(durationsMs, 99),
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

// GetThroughput returns the current throughput in commands/second
func (m *Metrics) GetThroughput() float64 {
	elapsed := time.Since(m.startTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(m.commandsCommitted.Load()) / elapsed
}

// Report contains all collected metrics
type Report struct {
	// Test configuration
	ClusterSize  int       `json:"cluster_size"`
	TestDuration float64   `json:"test_duration_seconds"`
	StartTime    time.Time `json:"start_time"`
	EndTime      time.Time `json:"end_time"`

	// Throughput metrics
	CommandsCommitted uint64  `json:"commands_committed"`
	ThroughputCmdSec  float64 `json:"throughput_cmd_per_sec"`

	// Latency metrics
	CommandLatency LatencyStats `json:"command_latency"`

	// Network metrics
	AppendEntriesCount uint64 `json:"append_entries_count"`
	RequestVoteCount   uint64 `json:"request_vote_count"`
	HeartbeatCount     uint64 `json:"heartbeat_count"`

	// Leader election metrics
	ElectionCount uint64       `json:"election_count"`
	ElectionStats LatencyStats `json:"election_stats"`
}

// GetReport generates a comprehensive performance report
func (m *Metrics) GetReport(clusterSize int) Report {
	endTime := time.Now()
	duration := endTime.Sub(m.startTime).Seconds()

	return Report{
		ClusterSize:        clusterSize,
		TestDuration:       duration,
		StartTime:          m.startTime,
		EndTime:            endTime,
		CommandsCommitted:  m.commandsCommitted.Load(),
		ThroughputCmdSec:   m.GetThroughput(),
		CommandLatency:     m.GetLatencyStats(),
		AppendEntriesCount: m.appendEntriesCount.Load(),
		RequestVoteCount:   m.requestVoteCount.Load(),
		HeartbeatCount:     m.heartbeatCount.Load(),
		ElectionCount:      m.electionCount.Load(),
		ElectionStats:      m.GetElectionStats(),
	}
}

// PrintReport prints the report in a human-readable format
func (r *Report) PrintReport() {
	fmt.Println("\n" + string('='))
	fmt.Println("RAFT PERFORMANCE REPORT")
	fmt.Println(string('='))
	fmt.Printf("\nTest Configuration:\n")
	fmt.Printf("  Cluster Size: %d nodes\n", r.ClusterSize)
	fmt.Printf("  Duration: %.2f seconds\n", r.TestDuration)
	fmt.Printf("  Start: %s\n", r.StartTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("  End: %s\n", r.EndTime.Format("2006-01-02 15:04:05"))

	fmt.Printf("\n" + string('-') + "\n")
	fmt.Printf("NFR1 - Performance Metrics\n")
	fmt.Printf(string('-') + "\n")

	fmt.Printf("\nThroughput:\n")
	fmt.Printf("  Commands Committed: %d\n", r.CommandsCommitted)
	fmt.Printf("  Throughput: %.2f cmd/sec\n", r.ThroughputCmdSec)

	fmt.Printf("\nCommand Latency (submission to commit):\n")
	if r.CommandLatency.Count > 0 {
		fmt.Printf("  Count: %d\n", r.CommandLatency.Count)
		fmt.Printf("  Min: %.3f ms\n", r.CommandLatency.Min)
		fmt.Printf("  Mean: %.3f ms\n", r.CommandLatency.Mean)
		fmt.Printf("  P50: %.3f ms\n", r.CommandLatency.P50)
		fmt.Printf("  P95: %.3f ms\n", r.CommandLatency.P95)
		fmt.Printf("  P99: %.3f ms\n", r.CommandLatency.P99)
		fmt.Printf("  Max: %.3f ms\n", r.CommandLatency.Max)
		fmt.Printf("  StdDev: %.3f ms\n", r.CommandLatency.StdDev)
	} else {
		fmt.Printf("  No data collected\n")
	}

	fmt.Printf("\n" + string('-') + "\n")
	fmt.Printf("NFR2 - Network & Message Metrics\n")
	fmt.Printf(string('-') + "\n")
	fmt.Printf("\nRPC Counts:\n")
	fmt.Printf("  AppendEntries: %d\n", r.AppendEntriesCount)
	fmt.Printf("  RequestVote: %d\n", r.RequestVoteCount)
	fmt.Printf("  Heartbeats: %d\n", r.HeartbeatCount)
	fmt.Printf("  Total RPCs: %d\n", r.AppendEntriesCount+r.RequestVoteCount+r.HeartbeatCount)

	fmt.Printf("\nLeader Elections:\n")
	fmt.Printf("  Election Count: %d\n", r.ElectionCount)
	if r.ElectionStats.Count > 0 {
		fmt.Printf("  Avg Duration: %.3f ms\n", r.ElectionStats.Mean)
		fmt.Printf("  P50 Duration: %.3f ms\n", r.ElectionStats.P50)
		fmt.Printf("  P95 Duration: %.3f ms\n", r.ElectionStats.P95)
	}

	fmt.Println("\n" + string('='))
}

// SaveJSON saves the report to a JSON file
func (r *Report) SaveJSON(filename string) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal report: %w", err)
	}

	// TODO: Write to file
	_ = data // Suppress unused variable warning for now
	return nil
}

// Reset clears all collected metrics (useful for running multiple tests)
func (m *Metrics) Reset() {
	m.mu.Lock()
	m.commandLatencies = make([]time.Duration, 0, 10000)
	m.mu.Unlock()

	m.electionMu.Lock()
	m.electionDuration = make([]time.Duration, 0, 100)
	m.electionMu.Unlock()

	m.appendEntriesCount.Store(0)
	m.requestVoteCount.Store(0)
	m.heartbeatCount.Store(0)
	m.commandsCommitted.Store(0)
	m.electionCount.Store(0)
	m.startTime = time.Now()
}
