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
}

func NewServer() *Server {
	// https://go.dev/doc/effective_go#composite_literals
	return &Server{
		ID:          uuid.New(),
		State:       Follower,
		CurrentTerm: 0,
	}
}
