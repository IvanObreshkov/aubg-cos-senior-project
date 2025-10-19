package server

import (
	"aubg-cos-senior-project/internal/pubsub"
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
	// TODO: (Updated on stable storage before responding to RPCs)
	Log raft.LogStorage
	// StateMachine is the state machine of the Server as per Section 2 from the
	// [Raft paper](https://raft.github.io/raft.pdf)
	StateMachine raft.StateMachine
	// Transport is the transport layer used for sending RPC messages
	transport *Transport
	// A list of ServerIDs of the other Servers in the cluster
	peers []ServerID
	// The underlying gRPC server used for receiving RPC messages
	grpcServer *grpc.Server
	// The timer for the serverState.electionTimeout as defined in Section 5.2 from the
	// [Raft paper](https://raft.github.io/raft.pdf)
	electionTimeoutTimer *time.Timer
	// pubSub is used to send events about the state of the server to subscribed listeners
	pubSub *pubsub.PubSubClient
}

// quorumSize calculates the number of nodes required to achieve a quorum (majority), required to agree on a decision
func (s *Server) quorumSize() int {
	voters := len(s.peers) + 1 // + self
	return voters/2 + 1
}

// SetPeers updates the list of peer server IDs and reinitializes the transport with the new peer list
func (s *Server) SetPeers(peerIDs []ServerID) {
	s.peers = peerIDs
	s.transport = NewTransport(peerIDs)
}

// RequestVote handles the RequestVote RPC call from a peer's client
func (s *Server) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Reply false if term < currentTerm
	if req.Term < s.currentTerm {
		return &proto.RequestVoteResponse{
			Term:        s.currentTerm,
			VoteGranted: false,
		}, nil
	}

	// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower
	if req.Term > s.currentTerm {
		s.currentTerm = req.Term
		s.state = Follower
		s.votedFor = nil
		s.votersThisTerm = nil
	}

	// Grant vote if: (votedFor is null or candidateId) AND candidate's log is at least as up-to-date
	voteGranted := (s.votedFor == nil || *s.votedFor == ServerID(req.CandidateId))

	if voteGranted {
		s.votedFor = (*ServerID)(&req.CandidateId)
		// Reset election timeout when granting a vote
		s.electionTimeoutTimer.Reset(s.electionTimeout)
	}

	return &proto.RequestVoteResponse{
		Term:        s.currentTerm,
		VoteGranted: voteGranted,
	}, nil
}

// AppendEntries handles the AppendEntries RPC call from a peer's client
func (s *Server) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	// Acquire a lock for the whole duration of this method. This creates a Transaction like behaviour, as we need to
	// update multiple values at the same time, and they have to stay consistent with each other. This guarantees no
	// other goroutine can modify the Server state.
	s.mu.Lock()
	defer s.mu.Unlock()

	// If a server receives a request with a stale term number, it rejects the request. (Section 5.1)
	if req.Term < s.currentTerm {
		return &proto.AppendEntriesResponse{
			Term:    s.currentTerm,
			Success: false,
		}, nil
	}

	// if one server’s current term is smaller than the other’s (Section 5.1)
	if req.Term > s.currentTerm {
		// 1. then it updates its current term to the larger value
		s.currentTerm = req.Term
		// 2. If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state
		s.state = Follower
		// Clear vote for new term. This is an explicit rule from Figure 2 for RequestVote and is required by the
		// Election Safety Property. Since we are now in a new, higher term, we must reset the vote to 'null' to be
		// eligible to vote for a candidate in this new term. (RequestVote logic) An old 'votedFor' value is only valid
		// for the old 'currentTerm'.
		s.votedFor = nil
		// Clear voters map when reverting to Follower
		s.votersThisTerm = nil
	}

	// Reset election timeout since we received communication from a leader
	s.electionTimeoutTimer.Reset(s.electionTimeout)

	// For heartbeat (empty entries), return success with the current term
	if len(req.Entries) == 0 {
		return &proto.AppendEntriesResponse{
			Term:    s.currentTerm,
			Success: true,
		}, nil
	}

	// TODO: Handle actual log entries here...
	return &proto.AppendEntriesResponse{
		Term:    s.currentTerm,
		Success: false,
	}, nil
}

// BeginElection is called when a server does not receive HeartBeat messages from a Leader node over an ElectionTimeout
// period, as per Section 5.2 from the [Raft paper](https://raft.github.io/raft.pdf)
func (s *Server) BeginElection() {
	// We put a Lock as all updates must be atomic, to avoid race conditions
	s.mu.Lock()

	// TOCTOU Protection: Only start election if we're a Follower or Candidate
	// This prevents starting an election after AppendEntries made us a Follower
	// in a higher term between the Orchestrator's check and this call.
	if s.state != Follower && s.state != Candidate {
		s.mu.Unlock()
		return
	}

	// Start on a clean state
	s.votersThisTerm = make(map[ServerID]struct{}, len(s.peers)+1)

	// 1. Increment the currentTerm of the Server
	s.currentTerm++

	// 2. Transition to a Candidate state
	s.state = Candidate

	// 3. The server Votes for itself exactly once
	s.votedFor = &s.ID
	s.votersThisTerm[s.ID] = struct{}{}

	// Snapshot the term and peers for outbound RPCs to avoid race conditions. (In case any other thread changes these
	// after we have released the lock)
	termForReq := s.currentTerm
	// s.peers... unpacks the slice
	peers := append([]ServerID(nil), s.peers...)

	// Release the lock before making any RPC calls, as these are blocking, and we don't want to stall other incoming
	// requests which also could change these state vars (e.g. AppendEntries)
	s.mu.Unlock()

	// 4. Send a RequestVote RPC in parallel to all its peers in the cluster
	req := &proto.RequestVoteRequest{
		Term:        termForReq,
		CandidateId: string(s.ID),
		// TODO: Add these later
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	log.Printf("Server %v Initiated a new Election", s.ID)

	for _, peer := range peers {
		// We do this in order to have parallelism but also waiting for a resp is a blocking operation, so we do this
		// in another thread.
		go func() {
			reqCtx := context.Background()
			SetServerCurrTerm(reqCtx, termForReq)
			SetServerID(reqCtx, s.ID)
			SetServerAddr(reqCtx, s.Address)

			resp, err := s.transport.RequestVote(reqCtx, peer, req)
			if err != nil {
				// Transport layer already logged the failure, just return
				return
			}

			// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
			// as per Figure 2 from the Raft paper
			if resp.Term > termForReq {
				// Lock as we are updating multiple values at once, to ensure atomicity
				s.mu.Lock()

				if resp.Term > s.currentTerm {
					s.currentTerm = resp.Term
					s.state = Follower
					s.votedFor = nil
					// Clear voters map when reverting to Follower
					s.votersThisTerm = nil
				}

				s.mu.Unlock()
				return
			}

			// if resp.Term < termForReq: it's a stale response, so ignore it

			// Count granted votes only for THIS election's term snapshot
			if resp.VoteGranted && resp.Term == termForReq {
				pubsub.Publish(s.pubSub, pubsub.NewEvent(VoteGranted, VoteGrantedPayload{
					From: peer,
					Term: termForReq,
				}))
			}
		}()
	}
}

// OnVoteGranted is called when we have received a vote from a server for a given term
func (s *Server) OnVoteGranted(from ServerID, term uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check for duplicate vote
	if _, dup := s.votersThisTerm[from]; dup {
		log.Printf("Detected a duplicate vote from Server %v for term %v. Discarding duplicate vote.", from, term)
		return
	}

	// Record the vote
	s.votersThisTerm[from] = struct{}{}
	votes := len(s.votersThisTerm)

	// Check if we've reached quorum
	if votes >= s.quorumSize() {
		pubsub.Publish(s.pubSub, pubsub.NewEvent(ElectionWon, term))
	}
}

// OnElectionWon is called when the server has won an election and should transition to Leader state
func (s *Server) OnElectionWon(term uint64) {
	s.mu.Lock()

	// TOCTOU Protection: Double-check we're still a candidate in the same term.
	// This ensures the Election Safety Property (Section 5.2, Figure 3) which states:
	// "at most one leader can be elected in a given term."
	//
	// The election may have been won for term T, but between the event being queued and now,
	// another goroutine (e.g., AppendEntries) could have:
	// 1. Discovered a higher term and reverted us to Follower
	// 2. Made us lose the election
	//
	// Without this check, we'd incorrectly become Leader with a stale term, allowing multiple
	// leaders in different terms to coexist, violating the Election Safety Property.
	if s.state != Candidate || s.currentTerm != term {
		s.mu.Unlock()
		return
	}

	// Transition to Leader state
	s.state = Leader

	// Clear the voters map as it's only relevant for Candidate state
	s.votersThisTerm = nil

	// Stop the election timeout timer as leaders don't need it.
	// We save the timer reference and clear the field while holding the lock,
	// but stop it outside the lock to avoid blocking while holding s.mu.
	// This prevents potential issues with timer channel operations.
	var timerToStop *time.Timer
	if s.electionTimeoutTimer != nil {
		timerToStop = s.electionTimeoutTimer
		s.electionTimeoutTimer = nil
	}

	s.mu.Unlock()

	// Stop timer outside the lock to minimize lock contention.
	// As of Go 1.23+, timer.Stop() is safe to call without draining the channel.
	if timerToStop != nil {
		timerToStop.Stop()
	}

	log.Printf("Server %s became Leader for term %d", s.ID, term)

	// Start sending heartbeats to maintain leadership
	s.SendHeartbeats()
}

// SendHeartbeats sends Heartbeats (empty AppendEntries RPCs) in parallel to all peers in the cluster.
// Each RPC already runs in its own goroutine.
func (s *Server) SendHeartbeats() {
	s.mu.RLock()
	term := s.currentTerm
	peerIDs := append([]ServerID(nil), s.peers...)
	s.mu.RUnlock()

	req := &proto.AppendEntriesRequest{
		Term:         term,
		LeaderId:     string(s.ID),
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}
	for _, peerID := range peerIDs {
		go func() {
			if _, err := s.transport.AppendEntries(context.Background(), peerID, req); err != nil {
				log.Printf("AppendEntries (heartbeat) -> %s failed: %v", peerID, err)
			}
		}()
	}
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
	pubsub.Publish(s.pubSub, pubsub.NewEvent(ServerShutDown, struct{}{}))
}

func (s *Server) ForceShutdown() {
	log.Printf("Force shutting down server %s", s.ID)
	s.transport.CloseAllClients()
	s.grpcServer.Stop()
	// Send a signal to all listeners that the server is shutting down
	pubsub.Publish(s.pubSub, pubsub.NewEvent(ServerShutDown, struct{}{}))
}

func NewServer(currentTerm uint64, addr ServerAddress, peers []ServerID, pubSub *pubsub.PubSubClient) *Server {
	var term uint64 = 0
	if currentTerm != 0 {
		term = currentTerm
	}

	// Generate a unique ID for this server
	serverID := ServerID(uuid.New().String())

	// Register this server with the resolver
	RegisterResolverPeer(serverID, addr)

	// https://go.dev/doc/effective_go#composite_literals
	return &Server{
		serverState: serverState{
			state:           Follower,
			currentTerm:     term,
			electionTimeout: getElectionTimeoutMs(),
		},
		ID:        serverID,
		Address:   addr,
		transport: NewTransport(peers),
		peers:     peers,
		pubSub:    pubSub,
	}
}
