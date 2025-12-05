package mocks

import (
	"aubg-cos-senior-project/internal/raft/proto"
	"fmt"
	"sync"
)

// MockLogStorage is a mock implementation of storage.LogStorage for testing
type MockLogStorage struct {
	mu       sync.RWMutex
	entries  map[uint64]*proto.LogEntry
	term     uint64
	votedFor *string

	// Error injection for testing
	AppendEntryError       error
	AppendEntriesError     error
	GetEntryError          error
	GetEntriesError        error
	GetEntriesFromError    error
	DeleteEntriesFromError error
	GetLastIndexError      error
	GetLastTermError       error
	GetCurrentTermError    error
	SetCurrentTermError    error
	GetVotedForError       error
	SetVotedForError       error
}

// NewMockLogStorage creates a new mock log storage
func NewMockLogStorage() *MockLogStorage {
	return &MockLogStorage{
		entries: make(map[uint64]*proto.LogEntry),
	}
}

func (m *MockLogStorage) AppendEntry(entry *proto.LogEntry) error {
	if m.AppendEntryError != nil {
		return m.AppendEntryError
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[entry.Index] = entry
	return nil
}

func (m *MockLogStorage) AppendEntries(entries []*proto.LogEntry) error {
	if m.AppendEntriesError != nil {
		return m.AppendEntriesError
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, entry := range entries {
		m.entries[entry.Index] = entry
	}
	return nil
}

func (m *MockLogStorage) GetEntry(index uint64) (*proto.LogEntry, error) {
	if m.GetEntryError != nil {
		return nil, m.GetEntryError
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.entries[index]
	if !ok {
		return nil, fmt.Errorf("entry not found at index %d", index)
	}
	return entry, nil
}

func (m *MockLogStorage) GetEntries(startIndex, endIndex uint64) ([]*proto.LogEntry, error) {
	if m.GetEntriesError != nil {
		return nil, m.GetEntriesError
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*proto.LogEntry
	for i := startIndex; i <= endIndex; i++ {
		if entry, ok := m.entries[i]; ok {
			result = append(result, entry)
		}
	}
	return result, nil
}

func (m *MockLogStorage) GetEntriesFrom(startIndex uint64) ([]*proto.LogEntry, error) {
	if m.GetEntriesFromError != nil {
		return nil, m.GetEntriesFromError
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*proto.LogEntry
	maxIndex := m.getLastIndexUnsafe()
	for i := startIndex; i <= maxIndex; i++ {
		if entry, ok := m.entries[i]; ok {
			result = append(result, entry)
		}
	}
	return result, nil
}

func (m *MockLogStorage) DeleteEntriesFrom(index uint64) error {
	if m.DeleteEntriesFromError != nil {
		return m.DeleteEntriesFromError
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	maxIndex := m.getLastIndexUnsafe()
	for i := index; i <= maxIndex; i++ {
		delete(m.entries, i)
	}
	return nil
}

func (m *MockLogStorage) GetLastIndex() (uint64, error) {
	if m.GetLastIndexError != nil {
		return 0, m.GetLastIndexError
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getLastIndexUnsafe(), nil
}

func (m *MockLogStorage) getLastIndexUnsafe() uint64 {
	var maxIndex uint64 = 0
	for index := range m.entries {
		if index > maxIndex {
			maxIndex = index
		}
	}
	return maxIndex
}

func (m *MockLogStorage) GetLastTerm() (uint64, error) {
	if m.GetLastTermError != nil {
		return 0, m.GetLastTermError
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	lastIndex := m.getLastIndexUnsafe()
	if lastIndex == 0 {
		return 0, nil
	}

	if entry, ok := m.entries[lastIndex]; ok {
		return entry.Term, nil
	}
	return 0, nil
}

func (m *MockLogStorage) GetCurrentTerm() (uint64, error) {
	if m.GetCurrentTermError != nil {
		return 0, m.GetCurrentTermError
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.term, nil
}

func (m *MockLogStorage) SetCurrentTerm(term uint64) error {
	if m.SetCurrentTermError != nil {
		return m.SetCurrentTermError
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.term = term
	return nil
}

func (m *MockLogStorage) GetVotedFor() (*string, error) {
	if m.GetVotedForError != nil {
		return nil, m.GetVotedForError
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.votedFor, nil
}

func (m *MockLogStorage) SetVotedFor(candidateID *string) error {
	if m.SetVotedForError != nil {
		return m.SetVotedForError
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.votedFor = candidateID
	return nil
}

func (m *MockLogStorage) Close() error {
	return nil
}
