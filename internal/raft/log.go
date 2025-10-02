package raft

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
}
