package storage

import (
	"aubg-cos-senior-project/internal/raft/proto"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTempDB(t *testing.T) (*BboltDb, string, func()) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewBboltStorage(dbPath)
	require.NoError(t, err)
	require.NotNil(t, db)

	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return db, dbPath, cleanup
}

func TestNewBboltStorage(t *testing.T) {
	t.Run("creates new database successfully", func(t *testing.T) {
		db, dbPath, cleanup := createTempDB(t)
		defer cleanup()

		assert.NotNil(t, db)
		assert.NotNil(t, db.conn)

		// Verify file was created
		_, err := os.Stat(dbPath)
		assert.NoError(t, err)
	})

	t.Run("opens existing database", func(t *testing.T) {
		db, dbPath, cleanup := createTempDB(t)
		db.Close()

		// Reopen the same database
		db2, err := NewBboltStorage(dbPath)
		defer cleanup()

		require.NoError(t, err)
		assert.NotNil(t, db2)
		db2.Close()
	})

	t.Run("fails with invalid path", func(t *testing.T) {
		db, err := NewBboltStorage("/invalid/path/that/does/not/exist/test.db")
		assert.Error(t, err)
		assert.Nil(t, db)
	})
}

func TestBboltStorage_AppendEntry(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	t.Run("appends single entry", func(t *testing.T) {
		entry := &proto.LogEntry{
			Index:   1,
			Term:    1,
			Type:    proto.LogEntryType_LOG_COMMAND,
			Command: []byte("test command"),
		}

		err := db.AppendEntry(entry)
		assert.NoError(t, err)

		// Verify it was stored
		retrieved, err := db.GetEntry(1)
		require.NoError(t, err)
		assert.Equal(t, entry.Index, retrieved.Index)
		assert.Equal(t, entry.Term, retrieved.Term)
		assert.Equal(t, entry.Command, retrieved.Command)
	})

	t.Run("overwrites existing entry", func(t *testing.T) {
		entry1 := &proto.LogEntry{
			Index:   2,
			Term:    1,
			Command: []byte("first"),
		}
		entry2 := &proto.LogEntry{
			Index:   2,
			Term:    2,
			Command: []byte("second"),
		}

		err := db.AppendEntry(entry1)
		require.NoError(t, err)

		err = db.AppendEntry(entry2)
		require.NoError(t, err)

		retrieved, err := db.GetEntry(2)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), retrieved.Term)
		assert.Equal(t, []byte("second"), retrieved.Command)
	})
}

func TestBboltStorage_AppendEntries(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	t.Run("appends multiple entries", func(t *testing.T) {
		entries := []*proto.LogEntry{
			{Index: 1, Term: 1, Command: []byte("cmd1")},
			{Index: 2, Term: 1, Command: []byte("cmd2")},
			{Index: 3, Term: 2, Command: []byte("cmd3")},
		}

		err := db.AppendEntries(entries)
		assert.NoError(t, err)

		// Verify all entries
		for _, entry := range entries {
			retrieved, err := db.GetEntry(entry.Index)
			require.NoError(t, err)
			assert.Equal(t, entry.Index, retrieved.Index)
			assert.Equal(t, entry.Term, retrieved.Term)
		}
	})

	t.Run("appends empty list", func(t *testing.T) {
		err := db.AppendEntries([]*proto.LogEntry{})
		assert.NoError(t, err)
	})
}

func TestBboltStorage_GetEntry(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	entry := &proto.LogEntry{
		Index:   5,
		Term:    3,
		Command: []byte("test"),
	}
	db.AppendEntry(entry)

	t.Run("retrieves existing entry", func(t *testing.T) {
		retrieved, err := db.GetEntry(5)
		require.NoError(t, err)
		assert.Equal(t, entry.Index, retrieved.Index)
		assert.Equal(t, entry.Term, retrieved.Term)
		assert.Equal(t, entry.Command, retrieved.Command)
	})

	t.Run("fails for non-existent entry", func(t *testing.T) {
		retrieved, err := db.GetEntry(999)
		assert.Error(t, err)
		assert.Nil(t, retrieved)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestBboltStorage_GetEntries(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	// Add test entries
	entries := []*proto.LogEntry{
		{Index: 1, Term: 1, Command: []byte("cmd1")},
		{Index: 2, Term: 1, Command: []byte("cmd2")},
		{Index: 3, Term: 2, Command: []byte("cmd3")},
		{Index: 4, Term: 2, Command: []byte("cmd4")},
		{Index: 5, Term: 3, Command: []byte("cmd5")},
	}
	db.AppendEntries(entries)

	t.Run("retrieves range of entries", func(t *testing.T) {
		retrieved, err := db.GetEntries(2, 4)
		require.NoError(t, err)
		assert.Len(t, retrieved, 3)
		assert.Equal(t, uint64(2), retrieved[0].Index)
		assert.Equal(t, uint64(4), retrieved[2].Index)
	})

	t.Run("retrieves single entry", func(t *testing.T) {
		retrieved, err := db.GetEntries(3, 3)
		require.NoError(t, err)
		assert.Len(t, retrieved, 1)
		assert.Equal(t, uint64(3), retrieved[0].Index)
	})

	t.Run("returns empty for non-existent range", func(t *testing.T) {
		retrieved, err := db.GetEntries(100, 200)
		require.NoError(t, err)
		assert.Len(t, retrieved, 0)
	})
}

func TestBboltStorage_GetEntriesFrom(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	entries := []*proto.LogEntry{
		{Index: 1, Term: 1, Command: []byte("cmd1")},
		{Index: 2, Term: 1, Command: []byte("cmd2")},
		{Index: 3, Term: 2, Command: []byte("cmd3")},
	}
	db.AppendEntries(entries)

	t.Run("retrieves all entries from start index", func(t *testing.T) {
		retrieved, err := db.GetEntriesFrom(2)
		require.NoError(t, err)
		assert.Len(t, retrieved, 2)
		assert.Equal(t, uint64(2), retrieved[0].Index)
		assert.Equal(t, uint64(3), retrieved[1].Index)
	})

	t.Run("retrieves from first entry", func(t *testing.T) {
		retrieved, err := db.GetEntriesFrom(1)
		require.NoError(t, err)
		assert.Len(t, retrieved, 3)
	})

	t.Run("returns empty when start is beyond last index", func(t *testing.T) {
		retrieved, err := db.GetEntriesFrom(100)
		require.NoError(t, err)
		assert.Len(t, retrieved, 0)
	})
}

func TestBboltStorage_DeleteEntriesFrom(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	t.Run("deletes entries from index", func(t *testing.T) {
		entries := []*proto.LogEntry{
			{Index: 1, Term: 1, Command: []byte("cmd1")},
			{Index: 2, Term: 1, Command: []byte("cmd2")},
			{Index: 3, Term: 2, Command: []byte("cmd3")},
			{Index: 4, Term: 2, Command: []byte("cmd4")},
		}
		db.AppendEntries(entries)

		err := db.DeleteEntriesFrom(3)
		assert.NoError(t, err)

		// Verify entries 3 and 4 are gone
		_, err = db.GetEntry(3)
		assert.Error(t, err)

		_, err = db.GetEntry(4)
		assert.Error(t, err)

		// Verify entries 1 and 2 still exist
		entry, err := db.GetEntry(1)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), entry.Index)

		entry, err = db.GetEntry(2)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), entry.Index)
	})

	t.Run("handles delete from non-existent index", func(t *testing.T) {
		err := db.DeleteEntriesFrom(999)
		assert.NoError(t, err) // Should not error
	})
}

func TestBboltStorage_GetLastIndex(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	t.Run("returns 0 for empty log", func(t *testing.T) {
		index, err := db.GetLastIndex()
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), index)
	})

	t.Run("returns last index with entries", func(t *testing.T) {
		entries := []*proto.LogEntry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 5, Term: 2}, // Non-contiguous
		}
		db.AppendEntries(entries)

		index, err := db.GetLastIndex()
		assert.NoError(t, err)
		assert.Equal(t, uint64(5), index)
	})
}

func TestBboltStorage_GetLastTerm(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	t.Run("returns 0 for empty log", func(t *testing.T) {
		term, err := db.GetLastTerm()
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), term)
	})

	t.Run("returns term of last entry", func(t *testing.T) {
		entries := []*proto.LogEntry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 3},
		}
		db.AppendEntries(entries)

		term, err := db.GetLastTerm()
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), term)
	})
}

func TestBboltStorage_CurrentTerm(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	t.Run("default term is 0", func(t *testing.T) {
		term, err := db.GetCurrentTerm()
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), term)
	})

	t.Run("sets and gets current term", func(t *testing.T) {
		err := db.SetCurrentTerm(5)
		require.NoError(t, err)

		term, err := db.GetCurrentTerm()
		assert.NoError(t, err)
		assert.Equal(t, uint64(5), term)
	})

	t.Run("persists across reopens", func(t *testing.T) {
		err := db.SetCurrentTerm(10)
		require.NoError(t, err)

		// Close and reopen
		dbPath := db.conn.Path()
		db.Close()

		db2, err := NewBboltStorage(dbPath)
		require.NoError(t, err)
		defer db2.Close()

		term, err := db2.GetCurrentTerm()
		assert.NoError(t, err)
		assert.Equal(t, uint64(10), term)
	})
}

func TestBboltStorage_VotedFor(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	t.Run("default votedFor is nil", func(t *testing.T) {
		votedFor, err := db.GetVotedFor()
		assert.NoError(t, err)
		assert.Nil(t, votedFor)
	})

	t.Run("sets and gets votedFor", func(t *testing.T) {
		candidateID := "server-123"
		err := db.SetVotedFor(&candidateID)
		require.NoError(t, err)

		votedFor, err := db.GetVotedFor()
		assert.NoError(t, err)
		require.NotNil(t, votedFor)
		assert.Equal(t, candidateID, *votedFor)
	})

	t.Run("clears votedFor with nil", func(t *testing.T) {
		candidateID := "server-456"
		db.SetVotedFor(&candidateID)

		err := db.SetVotedFor(nil)
		require.NoError(t, err)

		votedFor, err := db.GetVotedFor()
		assert.NoError(t, err)
		assert.Nil(t, votedFor)
	})

	t.Run("persists across reopens", func(t *testing.T) {
		candidateID := "server-789"
		err := db.SetVotedFor(&candidateID)
		require.NoError(t, err)

		// Close and reopen
		dbPath := db.conn.Path()
		db.Close()

		db2, err := NewBboltStorage(dbPath)
		require.NoError(t, err)
		defer db2.Close()

		votedFor, err := db2.GetVotedFor()
		assert.NoError(t, err)
		require.NotNil(t, votedFor)
		assert.Equal(t, candidateID, *votedFor)
	})
}

func TestBboltStorage_Close(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	err := db.Close()
	assert.NoError(t, err)

	// Verify operations fail after close
	err = db.AppendEntry(&proto.LogEntry{Index: 1, Term: 1})
	assert.Error(t, err)
}

func TestBboltStorage_ConfigurationEntries(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	t.Run("stores and retrieves configuration entries", func(t *testing.T) {
		config := &proto.Configuration{
			Servers: []*proto.ServerConfig{
				{Id: "server1", Address: "localhost:5001"},
				{Id: "server2", Address: "localhost:5002"},
			},
			IsJoint: false,
		}

		entry := &proto.LogEntry{
			Index:         1,
			Term:          1,
			Type:          proto.LogEntryType_LOG_CONFIGURATION,
			Configuration: config,
		}

		err := db.AppendEntry(entry)
		require.NoError(t, err)

		retrieved, err := db.GetEntry(1)
		require.NoError(t, err)
		assert.Equal(t, proto.LogEntryType_LOG_CONFIGURATION, retrieved.Type)
		assert.NotNil(t, retrieved.Configuration)
		assert.Len(t, retrieved.Configuration.Servers, 2)
		assert.Equal(t, "server1", retrieved.Configuration.Servers[0].Id)
	})
}
