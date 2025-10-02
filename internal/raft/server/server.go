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

type Server struct {
	// This makes the Server struct impl the proto.RaftServiceServer interface
	proto.UnimplementedRaftServiceServer

	serverState
	// The ID of the server in the cluster
	ID ServerID
	// The network address of the server
	Address ServerAddress
	// A Log is a collection of proto.LogEntry objects. If State is Leader, this collection is Append Only as per the Leader
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
	// pubSub is used to send events about the state of the server to subscribed listeners
	pubSub *internal.PubSub
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
	// If a server receives a request with a stale term number, it rejects the request. (Section 5.1)
	if req.Term < s.getCurrentTerm() {
		return &proto.AppendEntriesResponse{
			Term:    s.getCurrentTerm(),
			Success: false,
		}, nil
	}

	// if one server’s current term is smaller than the other’s (Section 5.1)
	if req.Term > s.getCurrentTerm() {
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

	// TODO: Handle actual log entries here...
	return &proto.AppendEntriesResponse{
		Term:    s.getCurrentTerm(),
		Success: false,
	}, nil
}

// BeginElection is called when a server does not receive HeartBeat messages from a Leader node over an ElectionTimeout
// period, as per Section 5.2 from the [Raft paper](https://raft.github.io/raft.pdf)
func (s *Server) BeginElection() {
	// Start on a clean state
	s.setGrantedVotesTotal(0)

	// 1. Increment the currentTerm of the Server
	s.incrementCurrentTerm()

	// 2. Transition to a Candidate state
	s.setState(Candidate)

	// 3. The server Votes for itself
	s.setVotedFor(&s.ID)

	// 4. Send a RequestVote RPC to all its peers in the cluster
	req := &proto.RequestVoteRequest{
		Term:         s.getCurrentTerm(),
		CandidateId:  string(s.ID),
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	log.Printf("Server %v Initiated a new Election", s.ID)

	// TODO: Servers retry RPCs if they do not receive a response in a timely manner, and they issue RPCs in parallel
	//  for best performance.
	reqCtx := context.Background()
	SetServerCurrTerm(reqCtx, s.getCurrentTerm())
	SetServerID(reqCtx, s.ID)
	SetServerAddr(reqCtx, s.Address)

	resp, _ := s.transport.RequestVote(reqCtx, "localhost:50052", req)
	// TODO: Handle err

	// If one server’s current term is smaller than the other’s (Section 5.1)
	if resp.Term > s.getCurrentTerm() {
		s.setCurrentTerm(resp.Term)
		s.setState(Follower)
		s.setVotedFor(nil)
		return
	}

	// Count the vote if granted
	if resp.VoteGranted {
		s.incrementGrantedVotesTotal()
		// Signal Orchestrator that a vote was received
		s.pubSub.Publish(internal.NewEvent(VoteReceived, struct{}{}))
	}

	// If we reach here, we didn't win the election. The ElectionTimeout will eventually expire and a new election
	// will be triggered as per option c) from Section 5.2 in the Raft paper
	log.Printf("Candidate %v neither won, nor lost the election", s.ID)
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
	go TrackElectionTimeoutJob(serverCtx{
		ID:    s.ID,
		Addr:  s.Address,
		State: s.getState(),
	}, s.electionTimeoutTimer, s.pubSub)

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
	s.pubSub.Publish(internal.NewEvent(ServerShutDown, struct{}{}))
}

func (s *Server) ForceShutdown() {
	log.Printf("Force shutting down server %s", s.ID)
	s.transport.CloseAllClients()
	s.grpcServer.Stop()
	// Send a signal to all listeners that the server is shutting down
	s.pubSub.Publish(internal.NewEvent(ServerShutDown, struct{}{}))
}

func NewServer(currentTerm uint64, peers []ServerAddress, pubSub *internal.PubSub) *Server {
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
		pubSub:    pubSub,
	}
}
