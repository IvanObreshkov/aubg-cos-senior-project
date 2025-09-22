package server

import (
	"aubg-cos-senior-project/internal/raft"
	"aubg-cos-senior-project/internal/raft/transport"
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

// serverState is container for different state variables
type serverState struct {
	// The state of the server as per Section 5.1 from the [Raft paper](https://raft.github.io/raft.pdf). When a server
	// initially starts it is a Follower as per Section 5.2 from the paper.
	State State
	// The [logical clock](https://dl.acm.org/doi/pdf/10.1145/359545.359563) used by servers to detect obsolete info,
	// such as stale leaders. It starts from 0 and increases by 1 over time, as per Section 5.1 from the
	// [Raft paper](https://raft.github.io/raft.pdf)
	CurrentTerm uint64
	// TODO: THis is property for each follower, maybe this should be a different type
	// nextIndex is the index of the next LogEntry the leader will send to a follower
	nextIndex uint64
}

type Server struct {
	// This makes the Server struct impl the RaftServiceServer interface
	transport.UnimplementedRaftServiceServer

	serverState
	// The ID of the server in the cluster
	ID uuid.UUID
	// The network address of the server
	Address raft.ServerAddress
	// A Log is a collection of LogEntry objects. If State is Leader, this collection is Append Only as per the Leader
	// Append-Only Property in Figure 3 from the [Raft paper](https://raft.github.io/raft.pdf)
	Log LogStorage
	// StateMachine is the state machine of the Server as per Section 2 from the
	// [Raft paper](https://raft.github.io/raft.pdf)
	StateMachine StateMachine
	// Transport is the transport layer used for sending RPC messages
	Transport *transport.Transport
	// A list of NetworkAddresses of the Servers in the cluster
	peers []raft.ServerAddress
}

func NewServer() *Server {
	// https://go.dev/doc/effective_go#composite_literals
	return &Server{
		serverState:  serverState{},
		ID:           uuid.UUID{},
		Address:      "",
		Log:          nil,
		StateMachine: nil,
		Transport:    nil,
		peers:        nil,
	}
}
