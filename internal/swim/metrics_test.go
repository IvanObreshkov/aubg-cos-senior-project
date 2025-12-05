package swim

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMetrics(t *testing.T) {
	m := NewMetrics()

	assert.NotNil(t, m)
	assert.NotNil(t, m.failureDetectionLatencies)
	assert.NotNil(t, m.probeLatencies)
	assert.NotNil(t, m.gossipPropagationLatencies)
	assert.NotNil(t, m.memberJoinLatencies)
	assert.False(t, m.startTime.IsZero())
}

func TestMetrics_RecordFailureDetection(t *testing.T) {
	m := NewMetrics()

	m.RecordFailureDetection(100 * time.Millisecond)

	m.mu.RLock()
	assert.Len(t, m.failureDetectionLatencies, 1)
	assert.Equal(t, 100*time.Millisecond, m.failureDetectionLatencies[0])
	m.mu.RUnlock()

	assert.Equal(t, uint64(1), m.failureDetectionCount.Load())

	m.RecordFailureDetection(200 * time.Millisecond)
	assert.Equal(t, uint64(2), m.failureDetectionCount.Load())
}

func TestMetrics_RecordProbeLatency(t *testing.T) {
	m := NewMetrics()

	m.RecordProbeLatency(50 * time.Millisecond)
	m.RecordProbeLatency(100 * time.Millisecond)

	m.mu.RLock()
	assert.Len(t, m.probeLatencies, 2)
	m.mu.RUnlock()
}

func TestMetrics_RecordGossipPropagation(t *testing.T) {
	m := NewMetrics()

	m.RecordGossipPropagation(25 * time.Millisecond)
	m.RecordGossipPropagation(75 * time.Millisecond)

	m.mu.RLock()
	assert.Len(t, m.gossipPropagationLatencies, 2)
	m.mu.RUnlock()
}

func TestMetrics_RecordMemberJoin(t *testing.T) {
	m := NewMetrics()

	m.RecordMemberJoin(10 * time.Millisecond)

	m.memberJoinMu.Lock()
	assert.Len(t, m.memberJoinLatencies, 1)
	m.memberJoinMu.Unlock()
}

func TestMetrics_RecordPing(t *testing.T) {
	m := NewMetrics()

	assert.Equal(t, uint64(0), m.pingCount.Load())

	m.RecordPing()
	assert.Equal(t, uint64(1), m.pingCount.Load())
	assert.Equal(t, uint64(1), m.totalMessagesOut.Load())

	m.RecordPing()
	assert.Equal(t, uint64(2), m.pingCount.Load())
	assert.Equal(t, uint64(2), m.totalMessagesOut.Load())
}

func TestMetrics_RecordAck(t *testing.T) {
	m := NewMetrics()

	m.RecordAck()
	assert.Equal(t, uint64(1), m.ackCount.Load())
	assert.Equal(t, uint64(1), m.totalMessagesOut.Load())
}

func TestMetrics_RecordPingReq(t *testing.T) {
	m := NewMetrics()

	m.RecordPingReq()
	assert.Equal(t, uint64(1), m.pingReqCount.Load())
	assert.Equal(t, uint64(1), m.totalMessagesOut.Load())
}

func TestMetrics_RecordIndirectPing(t *testing.T) {
	m := NewMetrics()

	m.RecordIndirectPing()
	assert.Equal(t, uint64(1), m.indirectPingCount.Load())
	assert.Equal(t, uint64(1), m.totalMessagesOut.Load())
}

func TestMetrics_RecordMessageIn(t *testing.T) {
	m := NewMetrics()

	m.RecordMessageIn()
	assert.Equal(t, uint64(1), m.totalMessagesIn.Load())

	m.RecordMessageIn()
	m.RecordMessageIn()
	assert.Equal(t, uint64(3), m.totalMessagesIn.Load())
}

func TestMetrics_RecordMessageOut(t *testing.T) {
	m := NewMetrics()

	m.RecordMessageOut()
	assert.Equal(t, uint64(1), m.totalMessagesOut.Load())
}

func TestMetrics_RecordSuspicion(t *testing.T) {
	m := NewMetrics()

	m.RecordSuspicion()
	assert.Equal(t, uint64(1), m.suspicionCount.Load())
}

func TestMetrics_RecordRefutedSuspicion(t *testing.T) {
	m := NewMetrics()

	m.RecordRefutedSuspicion()
	assert.Equal(t, uint64(1), m.refutedSuspicionCount.Load())
}

func TestMetrics_RecordTrueFalsePositive(t *testing.T) {
	m := NewMetrics()

	m.RecordTrueFalsePositive()
	assert.Equal(t, uint64(1), m.falseFailureCount.Load())
}

func TestComputeLatencyStats(t *testing.T) {
	t.Run("empty latencies returns zero stats", func(t *testing.T) {
		stats := computeLatencyStats(nil)
		assert.Equal(t, 0, stats.Count)
		assert.Equal(t, 0.0, stats.Min)
		assert.Equal(t, 0.0, stats.Max)
	})

	t.Run("single latency", func(t *testing.T) {
		latencies := []time.Duration{100 * time.Millisecond}
		stats := computeLatencyStats(latencies)

		assert.Equal(t, 1, stats.Count)
		assert.Equal(t, 100.0, stats.Min)
		assert.Equal(t, 100.0, stats.Max)
		assert.Equal(t, 100.0, stats.Mean)
		assert.Equal(t, 0.0, stats.StdDev)
	})

	t.Run("multiple latencies", func(t *testing.T) {
		latencies := []time.Duration{
			100 * time.Millisecond,
			200 * time.Millisecond,
			300 * time.Millisecond,
		}
		stats := computeLatencyStats(latencies)

		assert.Equal(t, 3, stats.Count)
		assert.Equal(t, 100.0, stats.Min)
		assert.Equal(t, 300.0, stats.Max)
		assert.Equal(t, 200.0, stats.Mean)
	})

	t.Run("calculates percentiles correctly", func(t *testing.T) {
		latencies := make([]time.Duration, 100)
		for i := 0; i < 100; i++ {
			latencies[i] = time.Duration(i+1) * time.Millisecond
		}
		stats := computeLatencyStats(latencies)

		assert.Equal(t, 100, stats.Count)
		assert.InDelta(t, 50.5, stats.P50, 1.0)
		assert.InDelta(t, 95.05, stats.P95, 1.0)
		assert.InDelta(t, 99.01, stats.P99, 1.0)
	})
}

func TestPercentile(t *testing.T) {
	t.Run("empty slice returns 0", func(t *testing.T) {
		result := percentile(nil, 50)
		assert.Equal(t, 0.0, result)
	})

	t.Run("single element", func(t *testing.T) {
		result := percentile([]float64{100.0}, 50)
		assert.Equal(t, 100.0, result)
	})

	t.Run("50th percentile of sorted data", func(t *testing.T) {
		sorted := []float64{1, 2, 3, 4, 5}
		result := percentile(sorted, 50)
		assert.Equal(t, 3.0, result)
	})

	t.Run("interpolation for percentile", func(t *testing.T) {
		sorted := []float64{1, 2, 3, 4}
		result := percentile(sorted, 50)
		assert.InDelta(t, 2.5, result, 0.1)
	})
}

func TestMetrics_GetFailureDetectionStats(t *testing.T) {
	m := NewMetrics()

	m.RecordFailureDetection(100 * time.Millisecond)
	m.RecordFailureDetection(200 * time.Millisecond)

	stats := m.GetFailureDetectionStats()

	assert.Equal(t, 2, stats.Count)
	assert.Equal(t, 100.0, stats.Min)
	assert.Equal(t, 200.0, stats.Max)
}

func TestMetrics_GetProbeLatencyStats(t *testing.T) {
	m := NewMetrics()

	m.RecordProbeLatency(50 * time.Millisecond)
	m.RecordProbeLatency(100 * time.Millisecond)

	stats := m.GetProbeLatencyStats()

	assert.Equal(t, 2, stats.Count)
	assert.Equal(t, 50.0, stats.Min)
	assert.Equal(t, 100.0, stats.Max)
}

func TestMetrics_GetGossipPropagationStats(t *testing.T) {
	m := NewMetrics()

	m.RecordGossipPropagation(25 * time.Millisecond)
	m.RecordGossipPropagation(75 * time.Millisecond)

	stats := m.GetGossipPropagationStats()

	assert.Equal(t, 2, stats.Count)
	assert.Equal(t, 25.0, stats.Min)
	assert.Equal(t, 75.0, stats.Max)
}

func TestMetrics_GetMemberJoinStats(t *testing.T) {
	m := NewMetrics()

	m.RecordMemberJoin(10 * time.Millisecond)
	m.RecordMemberJoin(20 * time.Millisecond)

	stats := m.GetMemberJoinStats()

	assert.Equal(t, 2, stats.Count)
	assert.Equal(t, 10.0, stats.Min)
	assert.Equal(t, 20.0, stats.Max)
}

func TestMetrics_GetMessageThroughput(t *testing.T) {
	m := NewMetrics()

	// Wait a bit to ensure non-zero elapsed time
	time.Sleep(10 * time.Millisecond)

	m.RecordMessageIn()
	m.RecordMessageOut()

	throughput := m.GetMessageThroughput()
	assert.Greater(t, throughput, 0.0)
}

func TestMetrics_GetMessageThroughput_ZeroElapsed(t *testing.T) {
	m := &Metrics{
		startTime: time.Now(),
	}

	// If elapsed time is 0, should return 0
	throughput := m.GetMessageThroughput()
	assert.GreaterOrEqual(t, throughput, 0.0)
}

func TestMetrics_GetReport(t *testing.T) {
	m := NewMetrics()

	// Record some metrics
	m.RecordPing()
	m.RecordAck()
	m.RecordPingReq()
	m.RecordIndirectPing()
	m.RecordMessageIn()
	m.RecordSuspicion()
	m.RecordRefutedSuspicion()
	m.RecordTrueFalsePositive()
	m.RecordFailureDetection(100 * time.Millisecond)
	m.RecordProbeLatency(50 * time.Millisecond)
	m.RecordGossipPropagation(25 * time.Millisecond)
	m.RecordMemberJoin(10 * time.Millisecond)

	report := m.GetReport(5)

	assert.Equal(t, 5, report.ClusterSize)
	assert.Greater(t, report.TestDuration, 0.0)
	assert.Equal(t, uint64(1), report.TotalMessagesIn)
	assert.Equal(t, uint64(4), report.TotalMessagesOut)
	assert.Equal(t, uint64(1), report.PingCount)
	assert.Equal(t, uint64(1), report.AckCount)
	assert.Equal(t, uint64(1), report.PingReqCount)
	assert.Equal(t, uint64(1), report.IndirectPingCount)
	assert.Equal(t, uint64(1), report.FailureDetectionCount)
	assert.Equal(t, uint64(1), report.SuspicionCount)
	assert.Equal(t, uint64(1), report.RefutedSuspicionCount)
	assert.Equal(t, uint64(1), report.FalsePositiveCount)
	assert.Greater(t, report.RefutationRate, 0.0)
}

func TestMetrics_GetReport_ZeroClusterSize(t *testing.T) {
	m := NewMetrics()

	report := m.GetReport(0)

	assert.Equal(t, 0, report.ClusterSize)
	assert.Equal(t, 0.0, report.MessagesPerNode)
}

func TestMetrics_GetReport_ZeroSuspicions(t *testing.T) {
	m := NewMetrics()

	report := m.GetReport(3)

	assert.Equal(t, 0.0, report.RefutationRate)
}

func TestMetrics_GetReport_ZeroFailures(t *testing.T) {
	m := NewMetrics()

	report := m.GetReport(3)

	assert.Equal(t, 0.0, report.FalsePositiveRate)
}

func TestReport_PrintReport(t *testing.T) {
	report := Report{
		ClusterSize:              3,
		TestDuration:             10.0,
		StartTime:                time.Now().Add(-10 * time.Second),
		EndTime:                  time.Now(),
		TotalMessagesIn:          100,
		TotalMessagesOut:         200,
		MessageThroughput:        30.0,
		MessagesPerNode:          66.67,
		PingCount:                50,
		AckCount:                 50,
		PingReqCount:             10,
		IndirectPingCount:        5,
		FailureDetectionCount:    2,
		SuspicionCount:           5,
		RefutedSuspicionCount:    3,
		RefutationRate:           0.6,
		FalsePositiveCount:       1,
		FalsePositiveRate:        0.33,
		FailureDetectionLatency:  LatencyStats{Count: 2, Min: 50, Max: 100, Mean: 75},
		ProbeLatency:             LatencyStats{Count: 50, Min: 1, Max: 10, Mean: 5},
		GossipPropagationLatency: LatencyStats{Count: 10, Min: 5, Max: 20, Mean: 10},
		MemberJoinLatency:        LatencyStats{Count: 3, Min: 10, Max: 30, Mean: 20},
	}

	// Should not panic
	assert.NotPanics(t, func() {
		report.PrintReport()
	})
}

func TestReport_PrintReport_EmptyStats(t *testing.T) {
	report := Report{
		ClusterSize:  3,
		TestDuration: 10.0,
		StartTime:    time.Now().Add(-10 * time.Second),
		EndTime:      time.Now(),
	}

	// Should not panic with empty stats
	assert.NotPanics(t, func() {
		report.PrintReport()
	})
}

func TestPrintLatencyStats(t *testing.T) {
	t.Run("with data", func(t *testing.T) {
		stats := LatencyStats{
			Count:  10,
			Min:    1.0,
			Max:    100.0,
			Mean:   50.0,
			P50:    50.0,
			P95:    95.0,
			P99:    99.0,
			StdDev: 25.0,
		}

		assert.NotPanics(t, func() {
			printLatencyStats(stats)
		})
	})

	t.Run("without data", func(t *testing.T) {
		stats := LatencyStats{}

		assert.NotPanics(t, func() {
			printLatencyStats(stats)
		})
	})
}

func TestReport_SaveJSON(t *testing.T) {
	report := Report{
		ClusterSize:  3,
		TestDuration: 10.0,
		StartTime:    time.Now(),
		EndTime:      time.Now(),
	}

	err := report.SaveJSON("test_report.json")
	assert.NoError(t, err)
}

func TestReport_JSONMarshal(t *testing.T) {
	report := Report{
		ClusterSize:             3,
		TestDuration:            10.0,
		TotalMessagesIn:         100,
		TotalMessagesOut:        200,
		PingCount:               50,
		FailureDetectionCount:   2,
		RefutedSuspicionCount:   1,
		FalsePositiveCount:      0,
		FailureDetectionLatency: LatencyStats{Count: 2, Min: 50, Max: 100},
	}

	data, err := json.Marshal(report)
	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	var unmarshaled Report
	err = json.Unmarshal(data, &unmarshaled)
	assert.NoError(t, err)
	assert.Equal(t, report.ClusterSize, unmarshaled.ClusterSize)
}

func TestMetrics_Reset(t *testing.T) {
	m := NewMetrics()

	// Record some metrics
	m.RecordPing()
	m.RecordAck()
	m.RecordPingReq()
	m.RecordIndirectPing()
	m.RecordMessageIn()
	m.RecordMessageOut()
	m.RecordSuspicion()
	m.RecordRefutedSuspicion()
	m.RecordTrueFalsePositive()
	m.RecordFailureDetection(100 * time.Millisecond)
	m.RecordProbeLatency(50 * time.Millisecond)
	m.RecordGossipPropagation(25 * time.Millisecond)
	m.RecordMemberJoin(10 * time.Millisecond)

	// Verify something was recorded
	assert.Equal(t, uint64(1), m.pingCount.Load())

	// Reset
	m.Reset()

	// Verify all counters are reset
	assert.Equal(t, uint64(0), m.pingCount.Load())
	assert.Equal(t, uint64(0), m.ackCount.Load())
	assert.Equal(t, uint64(0), m.pingReqCount.Load())
	assert.Equal(t, uint64(0), m.indirectPingCount.Load())
	assert.Equal(t, uint64(0), m.totalMessagesIn.Load())
	assert.Equal(t, uint64(0), m.totalMessagesOut.Load())
	assert.Equal(t, uint64(0), m.suspicionCount.Load())
	assert.Equal(t, uint64(0), m.refutedSuspicionCount.Load())
	assert.Equal(t, uint64(0), m.falseFailureCount.Load())
	assert.Equal(t, uint64(0), m.failureDetectionCount.Load())

	// Verify slices are reset
	m.mu.RLock()
	assert.Empty(t, m.failureDetectionLatencies)
	assert.Empty(t, m.probeLatencies)
	assert.Empty(t, m.gossipPropagationLatencies)
	m.mu.RUnlock()

	m.memberJoinMu.Lock()
	assert.Empty(t, m.memberJoinLatencies)
	m.memberJoinMu.Unlock()
}

func TestMetrics_SimpleGetters(t *testing.T) {
	m := NewMetrics()

	// Record some metrics
	m.RecordPing()
	m.RecordAck()
	m.RecordPingReq()
	m.RecordSuspicion()
	m.RecordFailureDetection(100 * time.Millisecond)
	m.RecordMessageIn()
	m.RecordMessageOut()

	assert.Equal(t, uint64(1), m.GetPingCount())
	assert.Equal(t, uint64(1), m.GetAckCount())
	assert.Equal(t, uint64(1), m.GetPingReqCount())
	assert.Equal(t, uint64(1), m.GetSuspicionCount())
	assert.Equal(t, uint64(1), m.GetFailureCount())
	assert.Equal(t, uint64(1), m.GetTotalMessagesIn())
	assert.Equal(t, uint64(4), m.GetTotalMessagesOut()) // Ping + Ack + PingReq + 1 manual
}

func TestMetrics_ConcurrentAccess(t *testing.T) {
	m := NewMetrics()

	var wg sync.WaitGroup
	numGoroutines := 10

	// Concurrently record metrics
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.RecordPing()
				m.RecordAck()
				m.RecordMessageIn()
				m.RecordSuspicion()
				m.RecordProbeLatency(time.Duration(j) * time.Millisecond)
				m.RecordFailureDetection(time.Duration(j) * time.Millisecond)
				m.RecordGossipPropagation(time.Duration(j) * time.Millisecond)
				m.RecordMemberJoin(time.Duration(j) * time.Millisecond)
			}
		}()
	}

	// Concurrently read metrics
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.GetPingCount()
				m.GetProbeLatencyStats()
				m.GetFailureDetectionStats()
				m.GetReport(5)
			}
		}()
	}

	wg.Wait()

	// Verify some metrics were recorded
	assert.Greater(t, m.GetPingCount(), uint64(0))
}

func TestLatencyStats_JSONTags(t *testing.T) {
	stats := LatencyStats{
		Count:  10,
		Min:    1.0,
		Max:    100.0,
		Mean:   50.0,
		P50:    50.0,
		P95:    95.0,
		P99:    99.0,
		StdDev: 25.0,
	}

	data, err := json.Marshal(stats)
	assert.NoError(t, err)

	// Verify JSON tags are present
	jsonStr := string(data)
	assert.Contains(t, jsonStr, "count")
	assert.Contains(t, jsonStr, "min_ms")
	assert.Contains(t, jsonStr, "max_ms")
	assert.Contains(t, jsonStr, "mean_ms")
	assert.Contains(t, jsonStr, "p50_ms")
	assert.Contains(t, jsonStr, "p95_ms")
	assert.Contains(t, jsonStr, "p99_ms")
	assert.Contains(t, jsonStr, "stddev_ms")
}
