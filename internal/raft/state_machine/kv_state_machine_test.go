package state_machine

import (
	"aubg-cos-senior-project/internal/raft/proto"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewKVStateMachine(t *testing.T) {
	sm := NewKVStateMachine("test-server")

	assert.NotNil(t, sm)
	assert.NotNil(t, sm.store)
	assert.Equal(t, "test-server", sm.id)
	assert.Len(t, sm.store, 0)
}

func TestKVStateMachine_Apply_SET(t *testing.T) {
	sm := NewKVStateMachine("test-server")

	t.Run("applies SET command", func(t *testing.T) {
		logs := []proto.LogEntry{
			{
				Index:   1,
				Term:    1,
				Type:    proto.LogEntryType_LOG_COMMAND,
				Command: []byte("SET key1=value1"),
			},
		}

		sm.Apply(logs)

		value, ok := sm.Get("key1")
		assert.True(t, ok)
		assert.Equal(t, "value1", value)
	})

	t.Run("applies multiple SET commands", func(t *testing.T) {
		logs := []proto.LogEntry{
			{Index: 2, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("SET key2=value2")},
			{Index: 3, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("SET key3=value3")},
		}

		sm.Apply(logs)

		value, ok := sm.Get("key2")
		assert.True(t, ok)
		assert.Equal(t, "value2", value)

		value, ok = sm.Get("key3")
		assert.True(t, ok)
		assert.Equal(t, "value3", value)
	})

	t.Run("overwrites existing key", func(t *testing.T) {
		logs := []proto.LogEntry{
			{Index: 4, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("SET key1=new_value")},
		}

		sm.Apply(logs)

		value, ok := sm.Get("key1")
		assert.True(t, ok)
		assert.Equal(t, "new_value", value)
	})

	t.Run("handles SET with equals sign in value", func(t *testing.T) {
		logs := []proto.LogEntry{
			{Index: 5, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("SET key4=val=ue")},
		}

		sm.Apply(logs)

		value, ok := sm.Get("key4")
		assert.True(t, ok)
		assert.Equal(t, "val=ue", value)
	})
}

func TestKVStateMachine_Apply_DEL(t *testing.T) {
	sm := NewKVStateMachine("test-server")

	// Set up initial state
	sm.Apply([]proto.LogEntry{
		{Index: 1, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("SET key1=value1")},
		{Index: 2, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("SET key2=value2")},
	})

	t.Run("deletes existing key", func(t *testing.T) {
		logs := []proto.LogEntry{
			{Index: 3, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("DEL key1")},
		}

		sm.Apply(logs)

		_, ok := sm.Get("key1")
		assert.False(t, ok)

		// key2 should still exist
		value, ok := sm.Get("key2")
		assert.True(t, ok)
		assert.Equal(t, "value2", value)
	})

	t.Run("handles delete of non-existent key", func(t *testing.T) {
		logs := []proto.LogEntry{
			{Index: 4, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("DEL nonexistent")},
		}

		// Should not panic
		sm.Apply(logs)
	})
}

func TestKVStateMachine_Apply_InvalidCommands(t *testing.T) {
	sm := NewKVStateMachine("test-server")

	t.Run("handles empty command", func(t *testing.T) {
		logs := []proto.LogEntry{
			{Index: 1, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("")},
		}

		// Should not panic
		sm.Apply(logs)
	})

	t.Run("handles unknown command", func(t *testing.T) {
		logs := []proto.LogEntry{
			{Index: 2, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("UNKNOWN key=value")},
		}

		// Should not panic
		sm.Apply(logs)
	})

	t.Run("handles malformed SET command", func(t *testing.T) {
		logs := []proto.LogEntry{
			{Index: 3, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("SET")},
			{Index: 4, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("SET invalid")},
		}

		// Should not panic
		sm.Apply(logs)
	})

	t.Run("handles malformed DEL command", func(t *testing.T) {
		logs := []proto.LogEntry{
			{Index: 5, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("DEL")},
		}

		// Should not panic
		sm.Apply(logs)
	})
}

func TestKVStateMachine_Apply_SkipsNonCommandEntries(t *testing.T) {
	sm := NewKVStateMachine("test-server")

	logs := []proto.LogEntry{
		{
			Index: 1,
			Term:  1,
			Type:  proto.LogEntryType_LOG_CONFIGURATION,
			Configuration: &proto.Configuration{
				Servers: []*proto.ServerConfig{{Id: "server1"}},
			},
		},
		{
			Index:   2,
			Term:    1,
			Type:    proto.LogEntryType_LOG_COMMAND,
			Command: []byte("SET key1=value1"),
		},
	}

	sm.Apply(logs)

	// Only the command entry should be applied
	value, ok := sm.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", value)
}

func TestKVStateMachine_Get(t *testing.T) {
	sm := NewKVStateMachine("test-server")

	t.Run("returns false for non-existent key", func(t *testing.T) {
		value, ok := sm.Get("nonexistent")
		assert.False(t, ok)
		assert.Equal(t, "", value)
	})

	t.Run("returns value for existing key", func(t *testing.T) {
		sm.Apply([]proto.LogEntry{
			{Index: 1, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("SET mykey=myvalue")},
		})

		value, ok := sm.Get("mykey")
		assert.True(t, ok)
		assert.Equal(t, "myvalue", value)
	})
}

func TestKVStateMachine_GetAll(t *testing.T) {
	sm := NewKVStateMachine("test-server")

	t.Run("returns empty map for empty state machine", func(t *testing.T) {
		all := sm.GetAll()
		assert.NotNil(t, all)
		assert.Len(t, all, 0)
	})

	t.Run("returns copy of all key-value pairs", func(t *testing.T) {
		sm.Apply([]proto.LogEntry{
			{Index: 1, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("SET key1=value1")},
			{Index: 2, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("SET key2=value2")},
			{Index: 3, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("SET key3=value3")},
		})

		all := sm.GetAll()
		assert.Len(t, all, 3)
		assert.Equal(t, "value1", all["key1"])
		assert.Equal(t, "value2", all["key2"])
		assert.Equal(t, "value3", all["key3"])

		// Verify it's a copy - modifying returned map shouldn't affect state machine
		all["key1"] = "modified"

		value, ok := sm.Get("key1")
		assert.True(t, ok)
		assert.Equal(t, "value1", value) // Original value unchanged
	})
}

func TestKVStateMachine_CaseInsensitiveCommands(t *testing.T) {
	sm := NewKVStateMachine("test-server")

	t.Run("handles lowercase commands", func(t *testing.T) {
		sm.Apply([]proto.LogEntry{
			{Index: 1, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("set key1=value1")},
			{Index: 2, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("del key1")},
		})

		_, ok := sm.Get("key1")
		assert.False(t, ok) // Should be deleted
	})

	t.Run("handles mixed case commands", func(t *testing.T) {
		sm.Apply([]proto.LogEntry{
			{Index: 3, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("SeT key2=value2")},
		})

		value, ok := sm.Get("key2")
		assert.True(t, ok)
		assert.Equal(t, "value2", value)
	})
}

func TestKVStateMachine_Concurrency(t *testing.T) {
	sm := NewKVStateMachine("test-server")

	t.Run("handles concurrent Apply calls", func(t *testing.T) {
		var wg sync.WaitGroup
		iterations := 100

		for i := 0; i < iterations; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				logs := []proto.LogEntry{
					{
						Index:   uint64(idx),
						Term:    1,
						Type:    proto.LogEntryType_LOG_COMMAND,
						Command: []byte("SET key" + string(rune(idx)) + "=value"),
					},
				}
				sm.Apply(logs)
			}(i)
		}

		wg.Wait()

		// Verify state is consistent
		all := sm.GetAll()
		assert.NotNil(t, all)
	})

	t.Run("handles concurrent reads and writes", func(t *testing.T) {
		sm.Apply([]proto.LogEntry{
			{Index: 1, Term: 1, Type: proto.LogEntryType_LOG_COMMAND, Command: []byte("SET shared=initial")},
		})

		var wg sync.WaitGroup

		// Concurrent readers
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sm.Get("shared")
				sm.GetAll()
			}()
		}

		// Concurrent writers
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				logs := []proto.LogEntry{
					{
						Index:   uint64(1000 + idx),
						Term:    1,
						Type:    proto.LogEntryType_LOG_COMMAND,
						Command: []byte("SET shared=updated"),
					},
				}
				sm.Apply(logs)
			}(i)
		}

		wg.Wait()

		// Should not panic and maintain consistency
		value, ok := sm.Get("shared")
		assert.True(t, ok)
		assert.Equal(t, "updated", value)
	})
}
