package server

import "aubg-cos-senior-project/internal"

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

const (
	// ServerShutDown event is sent when the server is shutting down. The payload for this event is an empty struct.
	ServerShutDown internal.EventType = iota
	// ElectionTimeoutExpired is sent when the ElectionTimeout of the server has expired.
	ElectionTimeoutExpired
	// VoteReceived is sent when a server in a Candidate state receives a vote
	VoteReceived
)

type serverCtx struct {
	ID    ServerID
	Addr  ServerAddress
	State State
}
