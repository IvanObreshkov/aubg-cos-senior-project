package storage

import (
	"aubg-cos-senior-project/internal/raft/proto"
)

/*
Notes from Section 5.3
Log replication retires are indefinite, until all logs are eventually replicated
Each log entry stores a state machine command along with the term
number when the entry was received by the leader. The
term numbers in log entries are used to detect inconsistencies between logs and to ensure some of the properties
in Figure 3

A LogEntry is considered commited once the leader has replicated it to the majority of the servers
An entry is considered committed if it is safe for that entry to be applied to state machines?
To eliminate problems like the one in Figure 8, Raft
never commits log entries from previous terms by counting replicas.

The leader keeps track of the highest index it knows
to be committed, and it includes that index in future
AppendEntries RPCs (including heartbeats) so that the
other servers eventually find out. Once a follower learns
that a log entry is committed, it applies the entry to its
local state machine (in log order).

• If two entries in different logs have the same index
and term, then they store the same command.
• If two entries in different logs have the same index
and term, then the logs are identical in all preceding
entries. This is guaranteed by a simple consistency check performed by AppendEntries. When sending an AppendEntries RPC,
the leader includes the index
and term of the entry in its log that immediately precedes
the new entries.  If the follower does not find an entry in
its log with the same index and term, then it refuses the
new entries

Raft uses the voting process to prevent a candidate from
winning an election unless its log contains all committed
entries

*** Leader Completeness Property *** TODO: CHECK THIS in More detail when the time comes

Maybe think about stable storage or use a simple k,v data structure in memory. Research BoltDB.

Idempotency for commands as mentioned in Section 8 is implemented via clients sending UID attached to every command
and the state machine is responsible for tracking these UIDs and associated responses
Check Section 8 for more info.
*/
type LogStorage interface {
	// Log Entry Operations

	// AppendEntry appends a single log entry to the log
	AppendEntry(entry *proto.LogEntry) error

	// AppendEntries appends multiple log entries to the log
	AppendEntries(entries []*proto.LogEntry) error

	// GetEntry retrieves a log entry at the specified index
	GetEntry(index uint64) (*proto.LogEntry, error)

	// GetEntries retrieves log entries from startIndex (inclusive) to endIndex (inclusive)
	GetEntries(startIndex, endIndex uint64) ([]*proto.LogEntry, error)

	// GetEntriesFrom retrieves all log entries starting from the given index
	GetEntriesFrom(startIndex uint64) ([]*proto.LogEntry, error)

	// DeleteEntriesFrom deletes all log entries starting from the given index (inclusive)
	// This is used to resolve log conflicts as per Section 5.3
	DeleteEntriesFrom(index uint64) error

	// GetLastIndex returns the index of the last log entry (0 if log is empty)
	GetLastIndex() (uint64, error)

	// GetLastTerm returns the term of the last log entry (0 if log is empty)
	GetLastTerm() (uint64, error)

	// Persistent State Operations (Section 5.2: "Updated on stable storage before responding to RPCs")

	// GetCurrentTerm retrieves the current term from persistent storage
	GetCurrentTerm() (uint64, error)

	// SetCurrentTerm persists the current term to storage
	SetCurrentTerm(term uint64) error

	// GetVotedFor retrieves the candidate ID this server voted for in the current term
	GetVotedFor() (*string, error)

	// SetVotedFor persists the candidate ID this server voted for
	SetVotedFor(candidateID *string) error

	// Utility Operations

	// Close closes the storage connection
	Close() error
}
