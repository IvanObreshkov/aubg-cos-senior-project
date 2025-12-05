package tob

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
	assert.NotNil(t, m.broadcastLatencies)
	assert.NotNil(t, m.sequencingLatencies)
	assert.False(t, m.startTime.IsZero())
}

func TestMetrics_RecordBroadcastLatency(t *testing.T) {
	m := NewMetrics()

	m.RecordBroadcastLatency(100 * time.Millisecond)
	m.RecordBroadcastLatency(200 * time.Millisecond)

	m.mu.RLock()
	assert.Len(t, m.broadcastLatencies, 2)
	assert.Equal(t, 100*time.Millisecond, m.broadcastLatencies[0])
	assert.Equal(t, 200*time.Millisecond, m.broadcastLatencies[1])
	m.mu.RUnlock()
}

func TestMetrics_RecordSequencingLatency(t *testing.T) {
	m := NewMetrics()

	m.RecordSequencingLatency(50 * time.Millisecond)
	m.RecordSequencingLatency(75 * time.Millisecond)

	m.mu.RLock()
	assert.Len(t, m.sequencingLatencies, 2)
	m.mu.RUnlock()
}

func TestMetrics_RecordDataMsg(t *testing.T) {
	m := NewMetrics()

	assert.Equal(t, uint64(0), m.dataMsgCount.Load())

	m.RecordDataMsg()
	assert.Equal(t, uint64(1), m.dataMsgCount.Load())
	assert.Equal(t, uint64(1), m.totalMessagesOut.Load())

	m.RecordDataMsg()
	assert.Equal(t, uint64(2), m.dataMsgCount.Load())
	assert.Equal(t, uint64(2), m.totalMessagesOut.Load())
}

func TestMetrics_RecordSequencedMsg(t *testing.T) {
	m := NewMetrics()

	m.RecordSequencedMsg()
	assert.Equal(t, uint64(1), m.sequencedMsgCount.Load())
	assert.Equal(t, uint64(1), m.totalMessagesOut.Load())
}

func TestMetrics_RecordHeartbeatMsg(t *testing.T) {
	m := NewMetrics()

	m.RecordHeartbeatMsg()
	assert.Equal(t, uint64(1), m.heartbeatMsgCount.Load())
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

func TestMetrics_RecordMessageDelivered(t *testing.T) {
	m := NewMetrics()

	m.RecordMessageDelivered()
	assert.Equal(t, uint64(1), m.messagesDelivered.Load())

	m.RecordMessageDelivered()
	assert.Equal(t, uint64(2), m.messagesDelivered.Load())
}

func TestMetrics_RecordOutOfOrder(t *testing.T) {
	m := NewMetrics()

	m.RecordOutOfOrder()
	assert.Equal(t, uint64(1), m.outOfOrderCount.Load())
}

func TestMetrics_RecordSequenceGap(t *testing.T) {
	m := NewMetrics()

	m.RecordSequenceGap()
	assert.Equal(t, uint64(1), m.sequenceGapCount.Load())
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

func TestMetrics_GetBroadcastLatencyStats(t *testing.T) {
	m := NewMetrics()

	m.RecordBroadcastLatency(100 * time.Millisecond)
	m.RecordBroadcastLatency(200 * time.Millisecond)

	stats := m.GetBroadcastLatencyStats()

	assert.Equal(t, 2, stats.Count)
	assert.Equal(t, 100.0, stats.Min)
	assert.Equal(t, 200.0, stats.Max)
}

func TestMetrics_GetSequencingLatencyStats(t *testing.T) {
	m := NewMetrics()

	m.RecordSequencingLatency(50 * time.Millisecond)
	m.RecordSequencingLatency(100 * time.Millisecond)

	stats := m.GetSequencingLatencyStats()

	assert.Equal(t, 2, stats.Count)
	assert.Equal(t, 50.0, stats.Min)
	assert.Equal(t, 100.0, stats.Max)
}

func TestMetrics_GetMessageThroughput(t *testing.T) {
	m := NewMetrics()

	// Wait a bit to ensure non-zero elapsed time
	time.Sleep(10 * time.Millisecond)

	m.RecordMessageDelivered()
	m.RecordMessageDelivered()

	throughput := m.GetMessageThroughput()
	assert.Greater(t, throughput, 0.0)
}

func TestMetrics_GetMessageThroughput_ZeroElapsed(t *testing.T) {
	m := &Metrics{
		startTime: time.Now(),
	}

	// If elapsed time is very small, should handle gracefully
	throughput := m.GetMessageThroughput()
	assert.GreaterOrEqual(t, throughput, 0.0)
}

func TestMetrics_GetBroadcastThroughput(t *testing.T) {
	m := NewMetrics()

	// Wait a bit to ensure non-zero elapsed time
	time.Sleep(10 * time.Millisecond)

	m.RecordDataMsg()
	m.RecordDataMsg()

	throughput := m.GetBroadcastThroughput()
	assert.Greater(t, throughput, 0.0)
}

func TestMetrics_GetReport(t *testing.T) {
	m := NewMetrics()

	// Record some metrics
	m.RecordDataMsg()
	m.RecordSequencedMsg()
	m.RecordHeartbeatMsg()
	m.RecordMessageIn()
	m.RecordMessageDelivered()
	m.RecordOutOfOrder()
	m.RecordSequenceGap()
	m.RecordBroadcastLatency(100 * time.Millisecond)
	m.RecordSequencingLatency(50 * time.Millisecond)

	report := m.GetReport(3)

	assert.Equal(t, 3, report.ClusterSize)
	assert.Greater(t, report.TestDuration, 0.0)
	assert.Equal(t, uint64(1), report.TotalMessagesIn)
	assert.Equal(t, uint64(3), report.TotalMessagesOut)
	assert.Equal(t, uint64(1), report.DataMsgCount)
	assert.Equal(t, uint64(1), report.SequencedMsgCount)
	assert.Equal(t, uint64(1), report.HeartbeatMsgCount)
	assert.Equal(t, uint64(1), report.MessagesDelivered)
	assert.Equal(t, uint64(1), report.OutOfOrderCount)
	assert.Equal(t, uint64(1), report.SequenceGapCount)
}

func TestMetrics_GetReport_ZeroClusterSize(t *testing.T) {
	m := NewMetrics()

	report := m.GetReport(0)

	assert.Equal(t, 0, report.ClusterSize)
	assert.Equal(t, 0.0, report.MessagesPerNode)
}

func TestMetrics_GetReport_OrderingAccuracy(t *testing.T) {
	m := NewMetrics()

	m.RecordMessageDelivered()
	m.RecordMessageDelivered()
	m.RecordMessageDelivered()
	m.RecordMessageDelivered()
	m.RecordOutOfOrder() // 1 out of 4 = 25% out of order = 75% accuracy

	report := m.GetReport(1)

	assert.Equal(t, 75.0, report.OrderingAccuracy)
}

func TestMetrics_GetReport_NoDeliveries(t *testing.T) {
	m := NewMetrics()

	report := m.GetReport(1)

	assert.Equal(t, 100.0, report.OrderingAccuracy) // Default 100% when no deliveries
}

func TestReport_PrintReport(t *testing.T) {
	report := Report{
		ClusterSize:         3,
		TestDuration:        10.0,
		StartTime:           time.Now().Add(-10 * time.Second),
		EndTime:             time.Now(),
		MessagesDelivered:   100,
		MessageThroughput:   10.0,
		BroadcastThroughput: 10.0,
		TotalMessagesIn:     50,
		TotalMessagesOut:    150,
		MessagesPerNode:     50.0,
		DataMsgCount:        50,
		SequencedMsgCount:   50,
		HeartbeatMsgCount:   10,
		OutOfOrderCount:     2,
		SequenceGapCount:    1,
		OrderingViolations:  2,
		OrderingAccuracy:    98.0,
		BroadcastLatency:    LatencyStats{Count: 50, Min: 1, Max: 100, Mean: 50},
		SequencingLatency:   LatencyStats{Count: 50, Min: 1, Max: 50, Mean: 25},
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
		ClusterSize:       3,
		TestDuration:      10.0,
		MessagesDelivered: 100,
		DataMsgCount:      50,
		BroadcastLatency:  LatencyStats{Count: 50, Min: 1, Max: 100},
		SequencingLatency: LatencyStats{Count: 50, Min: 1, Max: 50},
		OrderingAccuracy:  99.5,
	}

	data, err := json.Marshal(report)
	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	var unmarshaled Report
	err = json.Unmarshal(data, &unmarshaled)
	assert.NoError(t, err)
	assert.Equal(t, report.ClusterSize, unmarshaled.ClusterSize)
	assert.Equal(t, report.MessagesDelivered, unmarshaled.MessagesDelivered)
}

func TestMetrics_Reset(t *testing.T) {
	m := NewMetrics()

	// Record some metrics
	m.RecordDataMsg()
	m.RecordSequencedMsg()
	m.RecordHeartbeatMsg()
	m.RecordMessageIn()
	m.RecordMessageOut()
	m.RecordMessageDelivered()
	m.RecordOutOfOrder()
	m.RecordSequenceGap()
	m.RecordBroadcastLatency(100 * time.Millisecond)
	m.RecordSequencingLatency(50 * time.Millisecond)

	// Verify something was recorded
	assert.Equal(t, uint64(1), m.dataMsgCount.Load())

	// Reset
	m.Reset()

	// Verify all counters are reset
	assert.Equal(t, uint64(0), m.dataMsgCount.Load())
	assert.Equal(t, uint64(0), m.sequencedMsgCount.Load())
	assert.Equal(t, uint64(0), m.heartbeatMsgCount.Load())
	assert.Equal(t, uint64(0), m.totalMessagesIn.Load())
	assert.Equal(t, uint64(0), m.totalMessagesOut.Load())
	assert.Equal(t, uint64(0), m.messagesDelivered.Load())
	assert.Equal(t, uint64(0), m.outOfOrderCount.Load())
	assert.Equal(t, uint64(0), m.sequenceGapCount.Load())

	// Verify slices are reset
	m.mu.RLock()
	assert.Empty(t, m.broadcastLatencies)
	assert.Empty(t, m.sequencingLatencies)
	m.mu.RUnlock()
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
				m.RecordDataMsg()
				m.RecordSequencedMsg()
				m.RecordHeartbeatMsg()
				m.RecordMessageIn()
				m.RecordMessageDelivered()
				m.RecordBroadcastLatency(time.Duration(j) * time.Millisecond)
				m.RecordSequencingLatency(time.Duration(j) * time.Millisecond)
			}
		}()
	}

	// Concurrently read metrics
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.GetBroadcastLatencyStats()
				m.GetSequencingLatencyStats()
				m.GetMessageThroughput()
				m.GetReport(5)
			}
		}()
	}

	wg.Wait()

	// Verify some metrics were recorded
	assert.Greater(t, m.dataMsgCount.Load(), uint64(0))
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
