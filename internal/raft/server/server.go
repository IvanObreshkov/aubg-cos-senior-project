package server

import (
	"github.com/google/uuid"
)

// A State is a custom type representing the state of a server at any given point: leader, follower, or candidate
type State int

// As Golang does not support Enums this is a common pattern for implementing one
const (
	Leader State = iota
	Follower
	Candidate
)

type Server struct {
	// The ID of the server in the cluster
	ID uuid.UUID
	// The state of the server as per Section 5.1 from the [Raft paper](https://raft.github.io/raft.pdf). When a server
	// initially starts it is a Follower as per Section 5.2 from the paper.
	State State
	// The [logical clock](https://dl.acm.org/doi/pdf/10.1145/359545.359563) used by servers to detect obsolete info,
	// such as stale leaders. It starts from 0 and increases by 1 over time, as per Section 5.1 from the
	// [Raft paper](https://raft.github.io/raft.pdf)
	CurrentTerm int
	// A Log is a collection of LogEntry objects. If State is Leader, this collection is Append Only as per the Leader
	// Append-Only Property in Figure 3 from the [Raft paper](https://raft.github.io/raft.pdf)
	Log []LogEntry
	// TODO: THis is property for each follower, maybe this should be a different type
	// nextIndex is the index of the next LogEntry the leader will send to a follower
	nextIndex int
	// StateMachine is the state machine of the Server as per Section 2 from the
	// [Raft paper](https://raft.github.io/raft.pdf)
	StateMachine StateMachine
}

func NewServer() *Server {
	// https://go.dev/doc/effective_go#composite_literals
	return &Server{
		ID:          uuid.New(),
		State:       Follower,
		CurrentTerm: 0,
	}
}
