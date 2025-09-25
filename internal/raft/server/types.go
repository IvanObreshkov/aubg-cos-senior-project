package server

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
