package mocks

import (
	"sync"
	"time"
)

// MockMetricsCollector is a mock implementation of server.MetricsCollector for testing
type MockMetricsCollector struct {
	mu                     sync.RWMutex
	CommandLatencies       []time.Duration
	CommandsCommittedCount int
	AppendEntriesCount     int
	RequestVoteCount       int
	HeartbeatCount         int
	ElectionCount          int
	ElectionDurations      []time.Duration
}

// NewMockMetricsCollector creates a new mock metrics collector
func NewMockMetricsCollector() *MockMetricsCollector {
	return &MockMetricsCollector{
		CommandLatencies:  make([]time.Duration, 0),
		ElectionDurations: make([]time.Duration, 0),
	}
}

func (m *MockMetricsCollector) RecordCommandLatency(latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CommandLatencies = append(m.CommandLatencies, latency)
}

func (m *MockMetricsCollector) RecordCommandCommitted() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CommandsCommittedCount++
}

func (m *MockMetricsCollector) RecordAppendEntries() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.AppendEntriesCount++
}

func (m *MockMetricsCollector) RecordRequestVote() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RequestVoteCount++
}

func (m *MockMetricsCollector) RecordHeartbeat() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.HeartbeatCount++
}

func (m *MockMetricsCollector) RecordElection() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ElectionCount++
}

func (m *MockMetricsCollector) RecordElectionDuration(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ElectionDurations = append(m.ElectionDurations, duration)
}

// Reset clears all recorded metrics
func (m *MockMetricsCollector) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.CommandLatencies = make([]time.Duration, 0)
	m.CommandsCommittedCount = 0
	m.AppendEntriesCount = 0
	m.RequestVoteCount = 0
	m.HeartbeatCount = 0
	m.ElectionCount = 0
	m.ElectionDurations = make([]time.Duration, 0)
}
