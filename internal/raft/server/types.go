package server

import "time"

import (
	"aubg-cos-senior-project/internal/pubsub"
)

// ServerID is the id of the server in the cluster
type ServerID string

// ServerAddress is the network address of a Server
type ServerAddress string

// A State is a custom type representing the state of a server at any given point: leader, follower, or candidate
type State uint64

// As Golang does not support Enums this is a common pattern for implementing one
const (
	Leader State = iota
	Follower
	Candidate
)

// String returns the string representation of the State
func (s State) String() string {
	switch s {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	default:
		return "Unknown"
	}
}

const (
	// ServerShutDown event is sent when the server is shutting down. The payload for this event is an empty struct.
	ServerShutDown pubsub.EventType = iota
	// ElectionTimeoutExpired is sent when the ElectionTimeout of the server has expired.
	ElectionTimeoutExpired
	// VoteGranted is sent when a server in a Candidate state receives a vote
	VoteGranted
	// ElectionWon is sent when a candidate has received enough votes to become leader
	ElectionWon
)

type serverCtx struct {
	ID    ServerID
	Addr  ServerAddress
	State State
	Term  uint64
}

// VoteGrantedPayload travels with VoteGranted events so the orchestrator can act on these.
type VoteGrantedPayload struct {
	// The Sever which granted us the vote
	From ServerID
	// The Term we received the vote for
	Term uint64
}

// MetricsCollector is an optional interface for collecting performance metrics
type MetricsCollector interface {
	RecordCommandLatency(latency time.Duration)
	RecordCommandCommitted()
	RecordAppendEntries()
	RecordRequestVote()
	RecordHeartbeat()
	RecordElection()
	RecordElectionDuration(duration time.Duration)
}
