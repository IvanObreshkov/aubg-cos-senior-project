package server

import (
	"aubg-cos-senior-project/internal"
	"aubg-cos-senior-project/internal/raft"
	"aubg-cos-senior-project/internal/raft/proto"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
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
	// Protects all fields below
	mu sync.RWMutex

	// The state of the server as per Section 5.1 from the [Raft paper](https://raft.github.io/raft.pdf). When a server
	// initially starts it is a Follower as per Section 5.2 from the paper.
	state State
	// The latest term server has seen. It is a [logical clock](https://dl.acm.org/doi/pdf/10.1145/359545.359563) used
	// by servers to detect obsolete info, such as stale leaders. It is initialized to 0 on first boot of the cluster,
	// and  increases monotonically, as per Section 5.1 from the [Raft paper](https://raft.github.io/raft.pdf)
	currentTerm uint64
	// The ID of the Candidate Server that the current Server has voted for in the currentTerm. It could be null at the
	// beginning of a new term, as no votes are issued.
	votedFor *ServerID
	// TODO: THis is property for each follower, maybe this should be a different type
	// nextIndex is the index of the next LogEntry the leader will send to a follower
	//nextIndex uint64

	// ElectionTimeout is the current election timeout for the server. It is randomly chosen when the server is created.
	// It should be used with a time.Timer, and the timer should be reset at the beginning of each new election and
	// when the server receives an AppendEntries RPC, as per Section 5.2 from the
	// [Raft paper](https://raft.github.io/raft.pdf). It only makes sense when Server is Follower or Candidate.
	electionTimeout time.Duration
}

func (s *serverState) getState() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

func (s *serverState) setState(state State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
}

func (s *serverState) getCurrentTerm() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentTerm
}

func (s *serverState) setCurrentTerm(term uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm = term
}

func (s *serverState) getElectionTimeout() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.electionTimeout
}

func (s *serverState) setElectionTimeout(timeout time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.electionTimeout = timeout
}

func (s *serverState) getVotedFor() *ServerID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.votedFor
}

func (s *serverState) setVotedFor(id *ServerID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.votedFor = id
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
	// The timer for the serverState.electionTimeout as defined in Section 5.2 from the
	// [Raft paper](https://raft.github.io/raft.pdf)
	electionTimeoutTimer *time.Timer
	// stateManager is the orchestrator of the server
	stateManager *StateManager
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
	// Get the currentTerm of the server handling the request
	currTerm := s.getCurrentTerm()

	// If a server receives a request with a stale term number, it rejects the request. (Section 5.1)
	if req.Term < currTerm {
		return &proto.AppendEntriesResponse{
			Term:    currTerm,
			Success: false,
		}, nil
	}

	// if one server’s current term is smaller than the other’s (Section 5.1)
	if req.Term > currTerm {
		// 1. then it updates its current term to the larger value
		s.setCurrentTerm(req.Term)
		// 2. If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state
		s.setState(Follower)
		// Clear vote for new term. This is an explicit rule from Figure 2 for RequestVote and is required by the
		// Election Safety Property. Since we are now in a new, higher term, we must reset the vote to 'null' to be
		// eligible to vote for a candidate in this new term. (RequestVote logic) An old 'votedFor' value is only valid
		// for the old 'currentTerm'.
		s.setVotedFor(nil)
	}

	// Reset election timeout since we received communication from a leader
	s.electionTimeoutTimer.Reset(s.getElectionTimeout())

	// For heartbeat (empty entries), return success with the current term
	if len(req.Entries) == 0 {
		return &proto.AppendEntriesResponse{
			Term:    s.getCurrentTerm(),
			Success: true,
		}, nil
	}

	// #TDDO: Handle actual log entries here...
	return &proto.AppendEntriesResponse{
		Term:    s.getCurrentTerm(),
		Success: false,
	}, nil
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

	// Create the gRPC server
	s.grpcServer = grpc.NewServer(grpc.ConnectionTimeout(time.Second * 30))
	proto.RegisterRaftServiceServer(s.grpcServer, s)

	// Start the ElectionTimeout timer
	s.electionTimeoutTimer = time.NewTimer(s.serverState.electionTimeout)

	log.Printf("Raft node with ID %s running on %s:%d with peers %v\n", s.ID, tcpAddr.IP, tcpAddr.Port, s.peers)

	// Track ElectionTimeout on the background (while waiting for Heartbeats as per Section 5.2 from the
	// [Raft paper](https://raft.github.io/raft.pdf))
	go TrackElectionTimeoutJob(ServerCtx{
		ID:    s.ID,
		Addr:  s.Address,
		State: s.getState(),
	}, s.electionTimeoutTimer, s.stateManager.pubSub)

	// This one blocks as under the hood there is a call to lis.Accept which is a blocking operation.
	return s.grpcServer.Serve(lis)
}

func (s *Server) GracefulShutdown() {
	log.Printf("Shutting down server %s gracefully", s.ID)
	// First, stop accepting new incoming requests, in order to prevent interrupting a pending response to a peer
	s.grpcServer.GracefulStop()
	// Then, close all outbound client connections
	s.transport.CloseAllClients()
	// Send a signal to all listeners that the server is shutting down
	s.stateManager.pubSub.Publish(internal.NewEvent(ServerShutDown, struct{}{}))
}

func (s *Server) ForceShutdown() {
	log.Printf("Force shutting down server %s", s.ID)
	s.transport.CloseAllClients()
	s.grpcServer.Stop()
	// Send a signal to all listeners that the server is shutting down
	s.stateManager.pubSub.Publish(internal.NewEvent(ServerShutDown, struct{}{}))
}

func NewServer(currentTerm uint64, peers []ServerAddress, manager *StateManager) *Server {
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
		ID:           ServerID(uuid.New().String()),
		transport:    NewTransport(peers),
		peers:        peers,
		stateManager: manager,
	}
}
