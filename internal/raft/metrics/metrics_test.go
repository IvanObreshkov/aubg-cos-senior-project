package metrics

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMetrics(t *testing.T) {
	m := NewMetrics()

	assert.NotNil(t, m)
	assert.NotNil(t, m.commandLatencies)
	assert.NotNil(t, m.electionDuration)
	assert.False(t, m.startTime.IsZero())
}

func TestMetrics_RecordCommandLatency(t *testing.T) {
	m := NewMetrics()

	t.Run("records single latency", func(t *testing.T) {
		m.RecordCommandLatency(100 * time.Millisecond)

		m.mu.RLock()
		assert.Len(t, m.commandLatencies, 1)
		assert.Equal(t, 100*time.Millisecond, m.commandLatencies[0])
		m.mu.RUnlock()
	})

	t.Run("records multiple latencies", func(t *testing.T) {
		m.RecordCommandLatency(50 * time.Millisecond)
		m.RecordCommandLatency(150 * time.Millisecond)

		m.mu.RLock()
		assert.Len(t, m.commandLatencies, 3) // Including previous test
		m.mu.RUnlock()
	})
}

func TestMetrics_RecordCommandCommitted(t *testing.T) {
	m := NewMetrics()

	assert.Equal(t, uint64(0), m.commandsCommitted.Load())

	m.RecordCommandCommitted()
	assert.Equal(t, uint64(1), m.commandsCommitted.Load())

	m.RecordCommandCommitted()
	m.RecordCommandCommitted()
	assert.Equal(t, uint64(3), m.commandsCommitted.Load())
}

func TestMetrics_RecordAppendEntries(t *testing.T) {
	m := NewMetrics()

	assert.Equal(t, uint64(0), m.appendEntriesCount.Load())

	m.RecordAppendEntries()
	assert.Equal(t, uint64(1), m.appendEntriesCount.Load())

	for i := 0; i < 10; i++ {
		m.RecordAppendEntries()
	}
	assert.Equal(t, uint64(11), m.appendEntriesCount.Load())
}

func TestMetrics_RecordRequestVote(t *testing.T) {
	m := NewMetrics()

	assert.Equal(t, uint64(0), m.requestVoteCount.Load())

	m.RecordRequestVote()
	assert.Equal(t, uint64(1), m.requestVoteCount.Load())
}

func TestMetrics_RecordHeartbeat(t *testing.T) {
	m := NewMetrics()

	assert.Equal(t, uint64(0), m.heartbeatCount.Load())

	m.RecordHeartbeat()
	m.RecordHeartbeat()
	assert.Equal(t, uint64(2), m.heartbeatCount.Load())
}

func TestMetrics_RecordElection(t *testing.T) {
	m := NewMetrics()

	assert.Equal(t, uint64(0), m.electionCount.Load())

	m.RecordElection()
	assert.Equal(t, uint64(1), m.electionCount.Load())
}

func TestMetrics_RecordElectionDuration(t *testing.T) {
	m := NewMetrics()

	m.RecordElectionDuration(200 * time.Millisecond)
	m.RecordElectionDuration(150 * time.Millisecond)

	m.electionMu.Lock()
	assert.Len(t, m.electionDuration, 2)
	assert.Equal(t, 200*time.Millisecond, m.electionDuration[0])
	assert.Equal(t, 150*time.Millisecond, m.electionDuration[1])
	m.electionMu.Unlock()
}

func TestMetrics_GetThroughput(t *testing.T) {
	m := NewMetrics()

	t.Run("returns 0 for no commands", func(t *testing.T) {
		throughput := m.GetThroughput()
		assert.Equal(t, 0.0, throughput)
	})

	t.Run("calculates throughput", func(t *testing.T) {
		// Set start time to 1 second ago
		m.startTime = time.Now().Add(-1 * time.Second)

		m.RecordCommandCommitted()
		m.RecordCommandCommitted()

		throughput := m.GetThroughput()
		assert.Greater(t, throughput, 0.0)
		assert.LessOrEqual(t, throughput, 3.0) // Should be ~2 commands/sec
	})
}

func TestMetrics_GetLatencyStats(t *testing.T) {
	m := NewMetrics()

	t.Run("returns empty stats for no latencies", func(t *testing.T) {
		stats := m.GetLatencyStats()
		assert.Equal(t, 0, stats.Count)
	})

	t.Run("calculates statistics", func(t *testing.T) {
		m.RecordCommandLatency(100 * time.Millisecond)
		m.RecordCommandLatency(200 * time.Millisecond)
		m.RecordCommandLatency(300 * time.Millisecond)

		stats := m.GetLatencyStats()
		assert.Equal(t, 3, stats.Count)
		assert.InDelta(t, 200.0, stats.Mean, 1.0)
		assert.InDelta(t, 200.0, stats.P50, 1.0)
		assert.InDelta(t, 100.0, stats.Min, 1.0)
		assert.InDelta(t, 300.0, stats.Max, 1.0)
		assert.Greater(t, stats.StdDev, 0.0)
	})

	t.Run("calculates percentiles", func(t *testing.T) {
		m2 := NewMetrics()
		// Add 100 samples
		for i := 1; i <= 100; i++ {
			m2.RecordCommandLatency(time.Duration(i) * time.Millisecond)
		}

		stats := m2.GetLatencyStats()
		assert.InDelta(t, 50.0, stats.P50, 5.0)
		assert.InDelta(t, 95.0, stats.P95, 5.0)
		assert.InDelta(t, 99.0, stats.P99, 5.0)
	})
}

func TestMetrics_GetReport(t *testing.T) {
	m := NewMetrics()

	// Add some data
	m.RecordCommandLatency(100 * time.Millisecond)
	m.RecordCommandLatency(200 * time.Millisecond)
	m.RecordCommandCommitted()
	m.RecordAppendEntries()
	m.RecordRequestVote()
	m.RecordElection()

	report := m.GetReport(3)

	// Verify report has expected fields
	assert.Greater(t, report.CommandsCommitted, uint64(0))
	assert.Greater(t, report.AppendEntriesCount, uint64(0))
	assert.Greater(t, report.RequestVoteCount, uint64(0))
	assert.Greater(t, report.ElectionCount, uint64(0))
	assert.NotNil(t, report.CommandLatency)
	assert.Equal(t, 2, report.CommandLatency.Count)
}

func TestMetrics_Reset(t *testing.T) {
	m := NewMetrics()

	// Add data
	m.RecordCommandLatency(100 * time.Millisecond)
	m.RecordCommandCommitted()
	m.RecordAppendEntries()
	m.RecordRequestVote()
	m.RecordElection()
	m.RecordElectionDuration(200 * time.Millisecond)

	// Reset
	m.Reset()

	// Verify everything is cleared
	assert.Equal(t, uint64(0), m.commandsCommitted.Load())
	assert.Equal(t, uint64(0), m.appendEntriesCount.Load())
	assert.Equal(t, uint64(0), m.requestVoteCount.Load())
	assert.Equal(t, uint64(0), m.heartbeatCount.Load())
	assert.Equal(t, uint64(0), m.electionCount.Load())

	m.mu.RLock()
	assert.Len(t, m.commandLatencies, 0)
	m.mu.RUnlock()

	m.electionMu.Lock()
	assert.Len(t, m.electionDuration, 0)
	m.electionMu.Unlock()

	// Start time should be updated
	assert.False(t, m.startTime.IsZero())
}

func TestMetrics_Concurrency(t *testing.T) {
	m := NewMetrics()

	t.Run("handles concurrent updates", func(t *testing.T) {
		var wg sync.WaitGroup
		iterations := 1000

		// Concurrent latency recordings
		for i := 0; i < iterations; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				m.RecordCommandLatency(100 * time.Millisecond)
			}()
		}

		// Concurrent counter increments
		for i := 0; i < iterations; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				m.RecordCommandCommitted()
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				m.RecordAppendEntries()
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				m.RecordRequestVote()
			}()
		}

		wg.Wait()

		assert.Equal(t, uint64(iterations), m.commandsCommitted.Load())
		assert.Equal(t, uint64(iterations), m.appendEntriesCount.Load())
		assert.Equal(t, uint64(iterations), m.requestVoteCount.Load())

		m.mu.RLock()
		assert.Len(t, m.commandLatencies, iterations)
		m.mu.RUnlock()
	})

	t.Run("handles concurrent reads and writes", func(t *testing.T) {
		var wg sync.WaitGroup

		// Concurrent writers
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				m.RecordCommandLatency(100 * time.Millisecond)
				m.RecordCommandCommitted()
			}()
		}

		// Concurrent readers
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				m.GetLatencyStats()
				m.GetThroughput()
				m.GetReport(3)
			}()
		}

		wg.Wait()
	})
}
