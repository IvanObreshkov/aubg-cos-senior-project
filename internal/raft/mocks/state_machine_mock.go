package mocks

import (
	"aubg-cos-senior-project/internal/raft/proto"
	"sync"
)

// MockStateMachine is a mock implementation of state_machine.StateMachine for testing
type MockStateMachine struct {
	mu             sync.RWMutex
	AppliedLogs    []proto.LogEntry
	ApplyCallCount int
	ShouldPanic    bool
}

// NewMockStateMachine creates a new mock state machine
func NewMockStateMachine() *MockStateMachine {
	return &MockStateMachine{
		AppliedLogs: make([]proto.LogEntry, 0),
	}
}

func (m *MockStateMachine) Apply(logs []proto.LogEntry) {
	if m.ShouldPanic {
		panic("mock state machine panic")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.AppliedLogs = append(m.AppliedLogs, logs...)
	m.ApplyCallCount++
}

// GetAppliedLogs returns a copy of all applied logs
func (m *MockStateMachine) GetAppliedLogs() []proto.LogEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]proto.LogEntry, len(m.AppliedLogs))
	copy(result, m.AppliedLogs)
	return result
}

// Reset clears the mock state
func (m *MockStateMachine) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.AppliedLogs = make([]proto.LogEntry, 0)
	m.ApplyCallCount = 0
}
