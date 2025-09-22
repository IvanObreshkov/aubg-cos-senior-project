package server

import (
	"aubg-cos-senior-project/internal/raft"
	"aubg-cos-senior-project/internal/raft/transport"
	"context"
	"fmt"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

// A State is a custom type representing the state of a server at any given point: leader, follower, or candidate
type State int

// As Golang does not support Enums this is a common pattern for implementing one
const (
	Leader State = iota
	Follower
	Candidate
)

// serverState is container for different state variables as defined in Figure 2 from the
// [Raft paper](https://raft.github.io/raft.pdf)
type serverState struct {
	// The state of the server as per Section 5.1 from the [Raft paper](https://raft.github.io/raft.pdf). When a server
	// initially starts it is a Follower as per Section 5.2 from the paper.
	State State
	// The latest term server has seen. It is a [logical clock](https://dl.acm.org/doi/pdf/10.1145/359545.359563) used
	// by servers to detect obsolete info, such as stale leaders. It is initialized to 0 on first boot of the cluster,
	// and  increases monotonically, as per Section 5.1 from the [Raft paper](https://raft.github.io/raft.pdf)
	CurrentTerm uint64
	// The ID of the candidate server that the current server has voted for in the CurrentTerm.
	VotedFor raft.ServerID
	// TODO: THis is property for each follower, maybe this should be a different type
	// nextIndex is the index of the next LogEntry the leader will send to a follower
	//nextIndex uint64
}

type Server struct {
	// This makes the Server struct impl the transport.RaftServiceServer interface
	transport.UnimplementedRaftServiceServer

	serverState
	// The ID of the server in the cluster
	ID raft.ServerID
	// The network address of the server
	Address raft.ServerAddress
	// A Log is a collection of LogEntry objects. If State is Leader, this collection is Append Only as per the Leader
	// Append-Only Property in Figure 3 from the [Raft paper](https://raft.github.io/raft.pdf)
	Log LogStorage
	// StateMachine is the state machine of the Server as per Section 2 from the
	// [Raft paper](https://raft.github.io/raft.pdf)
	StateMachine StateMachine
	// Transport is the transport layer used for sending RPC messages
	transport *transport.Transport
	// A list of NetworkAddresses of the Servers in the cluster
	peers []raft.ServerAddress
	// The underlying gRPC server
	grpcServer *grpc.Server
}

// RequestVote handles the RequestVote RPC call from a peer's client
func (s *Server) RequestVote(ctx context.Context, req *transport.RequestVoteRequest) (*transport.RequestVoteResponse, error) {
	fmt.Print("HANDLING REQ")
	fmt.Print("CTX\n", ctx)
	fmt.Print("REQ\n", req)
	return nil, nil
}

// BeginElection is called when a server does not receive HeartBeat messages from a Leader node over an ElectionTimeout
// period, as per Section 5.2 from the [Raft paper](https://raft.github.io/raft.pdf)
func (s *Server) BeginElection() error {
	req := &transport.RequestVoteRequest{
		Term:         0,
		CandidateId:  "test",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	fmt.Print("SENDING REQ\n")
	resp, _ := s.transport.RequestVote(context.Background(), "localhost:50052", req)
	fmt.Printf("TEST RESP, %v\n", resp)
	return nil
}

// StartServer starts a new Raft node on the given port
func (s *Server) StartServer(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return err
	}

	tcpAddr, ok := lis.Addr().(*net.TCPAddr)
	if !ok {
		log.Fatalf("Failed to get TCP address")
	}

	// Assign the address to the server, as the port is randomly chosen
	s.Address = raft.ServerAddress(tcpAddr.String())

	grpcServer := grpc.NewServer(grpc.ConnectionTimeout(time.Second * 30))
	transport.RegisterRaftServiceServer(grpcServer, s)

	s.grpcServer = grpcServer

	log.Printf("Raft node with ID %s running on %s:%d with peers %v\n", s.ID, tcpAddr.IP, tcpAddr.Port, s.peers)
	// TODO: Add election timeout which will unblock the thread the server is running in and begin election
	return grpcServer.Serve(lis)

}

func (s *Server) GracefulShutdown() {
	log.Printf("Shutting down server %s gracefully", s.ID)
	// First, stop accepting new incoming requests, in order to prevent interrupting a pending response to a peer
	s.grpcServer.GracefulStop()
	// Then, close all outbound client connections
	s.transport.CloseAllClients()
}

func (s *Server) ForceShutdown() {
	log.Printf("Force shutting down server %s", s.ID)
	s.transport.CloseAllClients()
	s.grpcServer.Stop()
}

func NewServer(currentTerm uint64, peers []raft.ServerAddress) *Server {
	var term uint64 = 0
	if currentTerm != 0 {
		term = currentTerm
	}

	// https://go.dev/doc/effective_go#composite_literals
	return &Server{
		serverState: serverState{
			State:       Follower,
			CurrentTerm: term,
		},
		ID:        raft.ServerID(uuid.New().String()),
		transport: transport.NewTransport(peers),
		peers:     peers,
	}
}
