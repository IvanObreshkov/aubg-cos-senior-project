package server

import (
	"aubg-cos-senior-project/internal/raft"
	"aubg-cos-senior-project/internal/raft/proto"
	"context"
	"fmt"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
)

// getElectionTimeoutMs generates a randomly chosen ElectionTimeout in Ms. ElectionTimeout is the allowed period of
// time in Ms for a follower not to receive communications from a Leader, as defined in Section 5.2 from the
// [Raft paper](https://raft.github.io/raft.pdf). If no communications are received by the Follower over this time
// period, it assumes no viable Leader exists or is available and initiates an election to choose a new one.
func getElectionTimeoutMs() time.Duration {
	// The range of 150-300ms is chosen based on the recommendation for the end of Section 9.3 from the Raft paper
	// 300-150 gives the size of the range, and we add 1 to make it inclusive
	// We add 150 to shift the range, as rand.Intn() could return 0
	return time.Duration(rand.Intn(151)+150) * time.Millisecond
}

// serverState is container for different state variables as defined in Figure 2 from the
// [Raft paper](https://raft.github.io/raft.pdf)
// It provides an interface to set/get the variables in a thread safe manner.
type serverState struct {
	// The state of the server as per Section 5.1 from the [Raft paper](https://raft.github.io/raft.pdf). When a server
	// initially starts it is a Follower as per Section 5.2 from the paper.
	state State
	// The latest term server has seen. It is a [logical clock](https://dl.acm.org/doi/pdf/10.1145/359545.359563) used
	// by servers to detect obsolete info, such as stale leaders. It is initialized to 0 on first boot of the cluster,
	// and  increases monotonically, as per Section 5.1 from the [Raft paper](https://raft.github.io/raft.pdf)
	currentTerm uint64
	// The ID of the candidate server that the current server has voted for in the CurrentTerm.
	votedFor ServerID
	// TODO: THis is property for each follower, maybe this should be a different type
	// nextIndex is the index of the next LogEntry the leader will send to a follower
	//nextIndex uint64
	// ElectionTimeout is the current election timeout for the server. It is randomly chosen when the server is created.
	// It should be used with a time.Timer, and the timer should be reset at the beginning of each new election, as per
	// Section 5.2 from the [Raft paper](https://raft.github.io/raft.pdf)
	electionTimeout time.Duration
}

func (s *serverState) getState() State {
	return State(atomic.LoadUint64((*uint64)(&s.state)))
}

func (s *serverState) setState(state State) {
	atomic.StoreUint64((*uint64)(&s.state), uint64(state))
}

func (s *serverState) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&s.currentTerm)
}

func (s *serverState) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&s.currentTerm, term)
}

type Server struct {
	// This makes the Server struct impl the proto.RaftServiceServer interface
	proto.UnimplementedRaftServiceServer

	serverState
	// The ID of the server in the cluster
	ID ServerID
	// The network address of the server
	Address ServerAddress
	// A Log is a collection of LogEntry objects. If State is Leader, this collection is Append Only as per the Leader
	// Append-Only Property in Figure 3 from the [Raft paper](https://raft.github.io/raft.pdf)
	Log raft.LogStorage
	// StateMachine is the state machine of the Server as per Section 2 from the
	// [Raft paper](https://raft.github.io/raft.pdf)
	StateMachine raft.StateMachine
	// Transport is the transport layer used for sending RPC messages
	transport *Transport
	// A list of NetworkAddresses of the Servers in the cluster
	peers []ServerAddress
	// The underlying gRPC server used for receiving RPC messages
	grpcServer *grpc.Server
}

// RequestVote handles the RequestVote RPC call from a peer's client
func (s *Server) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	fmt.Print("HANDLING REQ")
	fmt.Print("CTX\n", ctx)
	fmt.Print("REQ\n", req)
	return nil, nil
}

// AppendEntries handles the AppendEntries RPC call from a peer's client
func (s *Server) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	return nil, nil
}

// BeginElection is called when a server does not receive HeartBeat messages from a Leader node over an ElectionTimeout
// period, as per Section 5.2 from the [Raft paper](https://raft.github.io/raft.pdf)
func (s *Server) BeginElection() error {
	req := &proto.RequestVoteRequest{
		Term:         s.currentTerm,
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
	s.Address = ServerAddress(tcpAddr.String())

	s.grpcServer = grpc.NewServer(grpc.ConnectionTimeout(time.Second * 30))
	proto.RegisterRaftServiceServer(s.grpcServer, s)

	log.Printf("Raft node with ID %s running on %s:%d with peers %v\n", s.ID, tcpAddr.IP, tcpAddr.Port, s.peers)
	// TODO: Add election timeout which will unblock the thread the server is running in and begin election

	// Start gRPC server in a separate goroutine and send values to a buffered channel.
	// We do this in order to have a way of accepting incoming requests while keeping track of the election timeout at
	// the same time.
	serverErrCh := make(chan error, 1)
	go func() {
		// This one blocks as under the hood there is a call to lis.Accept which is a blocking operation.
		// If the server returns an error (and unblocks), the use of the buffered channel will prevent the goroutine
		// from blocking again, as we might not have reached the `<-serverErrCh` yet, which will read the value from
		// the channel, preventing a deadlock.
		// Sending values to an unbuffered channel will block the sender until a receiver reads the value.
		// See: https://gobyexample.com/channels and https://gobyexample.com/channel-buffering
		serverErrCh <- s.grpcServer.Serve(lis)
	}()

	// Start election timeout timer in another goroutine, so we can track it on the background (while waiting for
	// Heartbeats as per Section 5.2 from the [Raft paper](https://raft.github.io/raft.pdf))
	go func() {
		timer := time.NewTimer(s.serverState.electionTimeout)
		defer timer.Stop()

		for {
			select {
			case <-timer.C:
				log.Printf("Election timeout expired for server %s", s.ID)
				s.BeginElection()
				timer.Reset(s.serverState.electionTimeout)
			}
		}
	}()

	// Wait for server to finish or error
	return <-serverErrCh

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

func NewServer(currentTerm uint64, peers []ServerAddress) *Server {
	var term uint64 = 0
	if currentTerm != 0 {
		term = currentTerm
	}

	// https://go.dev/doc/effective_go#composite_literals
	return &Server{
		serverState: serverState{
			state:           Follower,
			currentTerm:     term,
			electionTimeout: getElectionTimeoutMs(),
		},
		ID:        ServerID(uuid.New().String()),
		transport: NewTransport(peers),
		peers:     peers,
	}
}
