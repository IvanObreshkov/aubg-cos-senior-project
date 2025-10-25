package storage

import (
	"aubg-cos-senior-project/internal/raft/proto"
	"encoding/binary"
	"fmt"

	"go.etcd.io/bbolt"
	pb "google.golang.org/protobuf/proto"
)

var (
	// Bucket names
	logBucket      = []byte("logs")
	metadataBucket = []byte("metadata")

	// Metadata keys
	currentTermKey = []byte("currentTerm")
	votedForKey    = []byte("votedFor")
)

type BboltDb struct {
	conn *bbolt.DB
}

// NewBboltStorage creates a new BBolt-backed storage instance
func NewBboltStorage(path string) (*BboltDb, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt db: %w", err)
	}

	// Initialize buckets
	err = db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(logBucket); err != nil {
			return fmt.Errorf("failed to create log bucket: %w", err)
		}
		if _, err := tx.CreateBucketIfNotExists(metadataBucket); err != nil {
			return fmt.Errorf("failed to create metadata bucket: %w", err)
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	return &BboltDb{conn: db}, nil
}

// AppendEntry appends a single log entry to the log
func (b *BboltDb) AppendEntry(entry *proto.LogEntry) error {
	return b.conn.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(logBucket)

		// Serialize the entry using protobuf
		data, err := pb.Marshal(entry)
		if err != nil {
			return fmt.Errorf("failed to marshal log entry: %w", err)
		}

		// Use the entry's index as the key
		key := uint64ToBytes(entry.Index)
		return bucket.Put(key, data)
	})
}

// AppendEntries appends multiple log entries to the log
func (b *BboltDb) AppendEntries(entries []*proto.LogEntry) error {
	return b.conn.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(logBucket)

		for _, entry := range entries {
			data, err := pb.Marshal(entry)
			if err != nil {
				return fmt.Errorf("failed to marshal log entry: %w", err)
			}

			key := uint64ToBytes(entry.Index)
			if err := bucket.Put(key, data); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetEntry retrieves a log entry at the specified index
func (b *BboltDb) GetEntry(index uint64) (*proto.LogEntry, error) {
	var entry *proto.LogEntry
	err := b.conn.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(logBucket)
		key := uint64ToBytes(index)
		data := bucket.Get(key)

		if data == nil {
			return fmt.Errorf("log entry at index %d not found", index)
		}

		entry = &proto.LogEntry{}
		if err := pb.Unmarshal(data, entry); err != nil {
			return fmt.Errorf("failed to unmarshal log entry: %w", err)
		}
		return nil
	})
	return entry, err
}

// GetEntries retrieves log entries from startIndex (inclusive) to endIndex (inclusive)
func (b *BboltDb) GetEntries(startIndex, endIndex uint64) ([]*proto.LogEntry, error) {
	var entries []*proto.LogEntry
	err := b.conn.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(logBucket)

		for i := startIndex; i <= endIndex; i++ {
			key := uint64ToBytes(i)
			data := bucket.Get(key)

			if data == nil {
				continue // Skip missing entries
			}

			entry := &proto.LogEntry{}
			if err := pb.Unmarshal(data, entry); err != nil {
				return fmt.Errorf("failed to unmarshal log entry at index %d: %w", i, err)
			}
			entries = append(entries, entry)
		}
		return nil
	})
	return entries, err
}

// GetEntriesFrom retrieves all log entries starting from the given index
func (b *BboltDb) GetEntriesFrom(startIndex uint64) ([]*proto.LogEntry, error) {
	var entries []*proto.LogEntry
	err := b.conn.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(logBucket)
		cursor := bucket.Cursor()

		// Seek to the start index
		startKey := uint64ToBytes(startIndex)
		for k, v := cursor.Seek(startKey); k != nil; k, v = cursor.Next() {
			entry := &proto.LogEntry{}
			if err := pb.Unmarshal(v, entry); err != nil {
				return fmt.Errorf("failed to unmarshal log entry: %w", err)
			}
			entries = append(entries, entry)
		}
		return nil
	})
	return entries, err
}

// DeleteEntriesFrom deletes all log entries starting from the given index (inclusive)
func (b *BboltDb) DeleteEntriesFrom(index uint64) error {
	return b.conn.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(logBucket)
		cursor := bucket.Cursor()

		// Seek to the start index and delete everything from there
		startKey := uint64ToBytes(index)
		for k, _ := cursor.Seek(startKey); k != nil; k, _ = cursor.Next() {
			if err := bucket.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetLastIndex returns the index of the last log entry (0 if log is empty)
func (b *BboltDb) GetLastIndex() (uint64, error) {
	var lastIndex uint64
	err := b.conn.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(logBucket)
		cursor := bucket.Cursor()

		// Move to the last entry
		k, _ := cursor.Last()
		if k == nil {
			lastIndex = 0
			return nil
		}

		lastIndex = bytesToUint64(k)
		return nil
	})
	return lastIndex, err
}

// GetLastTerm returns the term of the last log entry (0 if log is empty)
func (b *BboltDb) GetLastTerm() (uint64, error) {
	var lastTerm uint64
	err := b.conn.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(logBucket)
		cursor := bucket.Cursor()

		// Move to the last entry
		_, v := cursor.Last()
		if v == nil {
			lastTerm = 0
			return nil
		}

		entry := &proto.LogEntry{}
		if err := pb.Unmarshal(v, entry); err != nil {
			return fmt.Errorf("failed to unmarshal last log entry: %w", err)
		}
		lastTerm = entry.Term
		return nil
	})
	return lastTerm, err
}

// GetCurrentTerm retrieves the current term from persistent storage
func (b *BboltDb) GetCurrentTerm() (uint64, error) {
	var term uint64
	err := b.conn.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(metadataBucket)
		data := bucket.Get(currentTermKey)

		if data == nil {
			term = 0
			return nil
		}

		term = bytesToUint64(data)
		return nil
	})
	return term, err
}

// SetCurrentTerm persists the current term to storage
func (b *BboltDb) SetCurrentTerm(term uint64) error {
	return b.conn.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(metadataBucket)
		return bucket.Put(currentTermKey, uint64ToBytes(term))
	})
}

// GetVotedFor retrieves the candidate ID this server voted for in the current term
func (b *BboltDb) GetVotedFor() (*string, error) {
	var votedFor *string
	err := b.conn.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(metadataBucket)
		data := bucket.Get(votedForKey)

		if data == nil {
			votedFor = nil
			return nil
		}

		candidateID := string(data)
		votedFor = &candidateID
		return nil
	})
	return votedFor, err
}

// SetVotedFor persists the candidate ID this server voted for
func (b *BboltDb) SetVotedFor(candidateID *string) error {
	return b.conn.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(metadataBucket)

		if candidateID == nil {
			// Delete the key if votedFor is nil (new term)
			return bucket.Delete(votedForKey)
		}

		return bucket.Put(votedForKey, []byte(*candidateID))
	})
}

// Close closes the storage connection
func (b *BboltDb) Close() error {
	return b.conn.Close()
}

// Helper functions for uint64 <-> []byte conversion
func uint64ToBytes(n uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return b
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
