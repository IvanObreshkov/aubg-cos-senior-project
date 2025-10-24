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
	// The timer for sending periodic heartbeats when the server is a Leader
	// Section 5.2: "Leaders sends periodic heartbeats (AppendEntries RPCs that carry no log entries)
	// to all followers in order to maintain its authority"
	heartbeatTimer *time.Timer
	// pubSub is used to send events about the state of the server to subscribed listeners
	pubSub *pubsub.PubSubClient
}

// quorumSize calculates the number of nodes required to achieve a quorum (majority), required to agree on a decision
func (s *Server) quorumSize() int {
	voters := len(s.peers) + 1 // + self
	return voters/2 + 1
}

// getLastLogIndexAndTerm returns the index and term of the last entry in the log.
// Returns (0, 0) if the log is empty, as per Section 5.4.1 from the [Raft paper](https://raft.github.io/raft.pdf)
func (s *Server) getLastLogIndexAndTerm() (uint64, uint64) {
	// TODO: Implement this once LogStorage interface is complete
	// For now, return (0, 0) indicating an empty log
	return 0, 0
}

// isLogUpToDate checks if the candidate's log is at least as up-to-date as the receiver's log.
// Section 5.4.1: "Raft determines which of two logs is more up-to-date by comparing the index and term of the
// last entries in the logs. If the logs have last entries with different terms, then the log with the later term
// is more up-to-date. If the logs end with the same term, then whichever log is longer is more up-to-date."
func (s *Server) isLogUpToDate(candidateLastLogIndex, candidateLastLogTerm uint64) bool {
	lastLogIndex, lastLogTerm := s.getLastLogIndexAndTerm()

	// If candidate's last log term is greater, their log is more up-to-date
	if candidateLastLogTerm > lastLogTerm {
		return true
	}

	// If terms are equal, the longer log is more up-to-date
	if candidateLastLogTerm == lastLogTerm {
		return candidateLastLogIndex >= lastLogIndex
	}

	// Candidate's log term is less than ours, so it's not up-to-date
	return false
}

// SetPeers updates the list of peer server IDs and reinitializes the transport with the new peer list
func (s *Server) SetPeers(peerIDs []ServerID) {
	s.peers = peerIDs
	s.transport = NewTransport(peerIDs)
}

// GetPubSub returns the server's PubSub client instance
func (s *Server) GetPubSub() *pubsub.PubSubClient {
	return s.pubSub
}

// RequestVote handles the RequestVote RPC call from a peer's client
func (s *Server) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[SERVER-%s] [TERM-%d] [RPC] Received RequestVote from %s (candidateTerm=%d, lastLogIndex=%d, lastLogTerm=%d)",
		s.ID, s.currentTerm, req.CandidateId, req.Term, req.LastLogIndex, req.LastLogTerm)

	// Reject self-votes - this happens due to gRPC resolver routing
	if ServerID(req.CandidateId) == s.ID {
		log.Printf("[SERVER-%s] [TERM-%d] [RPC] Rejecting self-vote (routing bug)",
			s.ID, s.currentTerm)
		return &proto.RequestVoteResponse{
			Term:        s.currentTerm,
			VoteGranted: false,
		}, nil
	}

	// Reply false if term < currentTerm
	if req.Term < s.currentTerm {
		log.Printf("[SERVER-%s] [TERM-%d] [RPC] Rejecting vote to %s (stale term: %d < %d)",
			s.ID, s.currentTerm, req.CandidateId, req.Term, s.currentTerm)
		return &proto.RequestVoteResponse{
			Term:        s.currentTerm,
			VoteGranted: false,
		}, nil
	}

	// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower
	if req.Term > s.currentTerm {
		oldTerm := s.currentTerm
		s.currentTerm = req.Term
		log.Printf("[SERVER-%s] [TERM-%d→%d] Discovered higher term in RequestVote, updating term and reverting to Follower",
			s.ID, oldTerm, s.currentTerm)
		s.state = Follower
		s.votedFor = nil
		s.votersThisTerm = nil
	}

	// Grant vote if: (votedFor is null or candidateId) AND candidate's log is at least as up-to-date
	// Section 5.4.1: The RequestVote RPC implements this restriction: the RPC includes information about the
	// candidate's log, and the voter denies its vote if its own log is more up-to-date than that of the candidate.
	voteGranted := (s.votedFor == nil || *s.votedFor == ServerID(req.CandidateId)) &&
		s.isLogUpToDate(req.LastLogIndex, req.LastLogTerm)

	if voteGranted {
		s.votedFor = (*ServerID)(&req.CandidateId)
		log.Printf("[SERVER-%s] [TERM-%d] [RPC] Granting vote to %s (log is up-to-date, resetting election timeout)",
			s.ID, s.currentTerm, req.CandidateId)
		// Reset election timeout when granting a vote (if timer exists)
		if s.electionTimeoutTimer != nil {
			s.electionTimeoutTimer.Reset(s.electionTimeout)
		}
	} else {
		if s.votedFor != nil && *s.votedFor != ServerID(req.CandidateId) {
			log.Printf("[SERVER-%s] [TERM-%d] [RPC] Rejecting vote to %s (already voted for %s)",
				s.ID, s.currentTerm, req.CandidateId, *s.votedFor)
		} else {
			log.Printf("[SERVER-%s] [TERM-%d] [RPC] Rejecting vote to %s (candidate's log is not up-to-date)",
				s.ID, s.currentTerm, req.CandidateId)
		}
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

	isHeartbeat := len(req.Entries) == 0
	if isHeartbeat {
		log.Printf("[SERVER-%s] [TERM-%d] [RPC] Received Heartbeat from Leader %s (leaderTerm=%d, leaderCommit=%d)",
			s.ID, s.currentTerm, req.LeaderId, req.Term, req.LeaderCommit)
	} else {
		log.Printf("[SERVER-%s] [TERM-%d] [RPC] Received AppendEntries from Leader %s (leaderTerm=%d, entries=%d, prevLogIndex=%d, prevLogTerm=%d)",
			s.ID, s.currentTerm, req.LeaderId, req.Term, len(req.Entries), req.PrevLogIndex, req.PrevLogTerm)
	}

	// If a server receives a request with a stale term number, it rejects the request. (Section 5.1)
	if req.Term < s.currentTerm {
		log.Printf("[SERVER-%s] [TERM-%d] [RPC] Rejecting AppendEntries from %s (stale term: %d < %d)",
			s.ID, s.currentTerm, req.LeaderId, req.Term, s.currentTerm)
		return &proto.AppendEntriesResponse{
			Term:    s.currentTerm,
			Success: false,
		}, nil
	}

	// if one server's current term is smaller than the other's (Section 5.1)
	if req.Term > s.currentTerm {
		// 1. then it updates its current term to the larger value
		oldTerm := s.currentTerm
		oldState := s.state
		s.currentTerm = req.Term
		// 2. If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state
		wasLeader := s.state == Leader
		s.state = Follower
		log.Printf("[SERVER-%s] [TERM-%d→%d] Discovered higher term in AppendEntries, reverting %s → Follower",
			s.ID, oldTerm, s.currentTerm, oldState)
		// Clear vote for new term. This is an explicit rule from Figure 2 for RequestVote and is required by the
		// Election Safety Property. Since we are now in a new, higher term, we must reset the vote to 'null' to be
		// eligible to vote for a candidate in this new term. (RequestVote logic) An old 'votedFor' value is only valid
		// for the old 'currentTerm'.
		s.votedFor = nil
		// Clear voters map when reverting to Follower
		s.votersThisTerm = nil
		// Clear leader-specific state
		s.nextIndex = nil
		s.matchIndex = nil

		// If we were a leader, stop sending heartbeats
		if wasLeader {
			log.Printf("[SERVER-%s] [TERM-%d] Stopping heartbeats (stepped down from Leader)",
				s.ID, s.currentTerm)
			// Stop heartbeat timer outside the main lock to avoid deadlock
			s.mu.Unlock()
			s.StopHeartbeats()
			s.mu.Lock()
		}
	}

	// Reset election timeout since we received communication from a leader
	// Only reset if we have a timer (Leaders don't have election timeout timers)
	if s.electionTimeoutTimer != nil {
		log.Printf("[SERVER-%s] [TERM-%d] Resetting election timeout (received communication from Leader %s)",
			s.ID, s.currentTerm, req.LeaderId)
		s.electionTimeoutTimer.Reset(s.electionTimeout)
	}

	// For heartbeat (empty entries), return success with the current term
	if len(req.Entries) == 0 {
		log.Printf("[SERVER-%s] [TERM-%d] [RPC] Accepted heartbeat from Leader %s",
			s.ID, s.currentTerm, req.LeaderId)
		return &proto.AppendEntriesResponse{
			Term:    s.currentTerm,
			Success: true,
		}, nil
	}

	// TODO: Handle actual log entries here...
	log.Printf("[SERVER-%s] [TERM-%d] [RPC] Log replication not yet implemented, rejecting AppendEntries",
		s.ID, s.currentTerm)
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

	// Check if server has started yet (timer is initialized in StartServer)
	if s.electionTimeoutTimer == nil {
		log.Printf("[SERVER-%s] Server not started yet, cannot begin election", s.ID)
		s.mu.Unlock()
		return
	}

	// TOCTOU Protection: Only start election if we're a Follower or Candidate
	// This prevents starting an election after AppendEntries made us a Follower
	// in a higher term between the Orchestrator's check and this call.
	if s.state != Follower && s.state != Candidate {
		s.mu.Unlock()
		return
	}

	// Detect split vote: if we're still a Candidate, the previous election failed
	if s.state == Candidate {
		log.Printf("[SERVER-%s] [TERM-%d] ⚠️  SPLIT VOTE detected - previous election failed, starting new election",
			s.ID, s.currentTerm)
	}

	// Start on a clean state
	s.votersThisTerm = make(map[ServerID]struct{}, len(s.peers)+1)

	// 1. Increment the currentTerm of the Server
	oldTerm := s.currentTerm
	s.currentTerm++

	// 2. Transition to a Candidate state
	s.state = Candidate
	log.Printf("[SERVER-%v] [TERM-%d→%d] Transitioning %v → Candidate, starting election",
		s.ID, oldTerm, s.currentTerm, Follower)

	// 3. The server Votes for itself exactly once
	s.votedFor = &s.ID
	s.votersThisTerm[s.ID] = struct{}{}
	log.Printf("[SERVER-%s] [TERM-%d] Voting for self (1/%d votes, need %d for quorum)",
		s.ID, s.currentTerm, len(s.peers)+1, s.quorumSize())

	// Reset election timeout with a NEW random value to prevent split votes
	// Section 5.2: "Raft uses randomized election timeouts to ensure that split votes are rare
	// and that they are resolved quickly"
	oldTimeout := s.electionTimeout
	s.electionTimeout = getElectionTimeoutMs()
	s.electionTimeoutTimer.Reset(s.electionTimeout)
	log.Printf("[SERVER-%s] [TERM-%d] Reset election timeout %v→%v (prevents synchronized elections)",
		s.ID, s.currentTerm, oldTimeout, s.electionTimeout)

	// Snapshot the term and peers for outbound RPCs to avoid race conditions. (In case any other thread changes these
	// after we have released the lock)
	termForReq := s.currentTerm
	// s.peers... unpacks the slice
	peers := append([]ServerID(nil), s.peers...)
	// Get last log index and term for the RequestVote RPC
	lastLogIndex, lastLogTerm := s.getLastLogIndexAndTerm()

	// Release the lock before making any RPC calls, as these are blocking, and we don't want to stall other incoming
	// requests which also could change these state vars (e.g. AppendEntries)
	s.mu.Unlock()

	// 4. Send a RequestVote RPC in parallel to all its peers in the cluster
	req := &proto.RequestVoteRequest{
		Term:         termForReq,
		CandidateId:  string(s.ID),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	log.Printf("[SERVER-%s] [TERM-%d] Sending RequestVote to %d peers (lastLogIndex=%d, lastLogTerm=%d)",
		s.ID, termForReq, len(peers), lastLogIndex, lastLogTerm)

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
					oldTerm := s.currentTerm
					oldState := s.state
					s.currentTerm = resp.Term
					wasLeader := s.state == Leader
					s.state = Follower
					s.votedFor = nil
					// Clear voters map when reverting to Follower
					s.votersThisTerm = nil
					// Clear leader-specific state
					s.nextIndex = nil
					s.matchIndex = nil

					log.Printf("[SERVER-%s] [TERM-%d→%d] Discovered higher term in RequestVote response from %s, reverting %s → Follower",
						s.ID, oldTerm, s.currentTerm, peer, oldState)

					s.mu.Unlock()

					// If we were a leader, stop sending heartbeats
					if wasLeader {
						s.StopHeartbeats()
					}
					return
				}

				s.mu.Unlock()
				return
			}

			// if resp.Term < termForReq: it's a stale response, so ignore it
			if resp.Term < termForReq {
				log.Printf("[SERVER-%s] [TERM-%d] Ignoring stale RequestVote response from %s (responseTerm=%d < currentTerm=%d)",
					s.ID, termForReq, peer, resp.Term, termForReq)
				return
			}

			// Count granted votes only for THIS election's term snapshot
			if resp.VoteGranted && resp.Term == termForReq {
				log.Printf("[SERVER-%s] [TERM-%d] Received vote from %s",
					s.ID, termForReq, peer)
				pubsub.Publish(s.pubSub, pubsub.NewEvent(VoteGranted, VoteGrantedPayload{
					From: peer,
					Term: termForReq,
				}))
			} else if !resp.VoteGranted {
				log.Printf("[SERVER-%s] [TERM-%d] Vote denied by %s",
					s.ID, termForReq, peer)
			}
		}()
	}
}

// OnVoteGranted is called when we have received a vote from a server for a given term
func (s *Server) OnVoteGranted(from ServerID, term uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Ignore votes if we're not a Candidate anymore (already Leader/Follower)
	if s.state != Candidate {
		log.Printf("[SERVER-%s] [TERM-%d] Ignoring vote from %s (state=%s, not Candidate)",
			s.ID, s.currentTerm, from, s.state)
		return
	}

	// Ignore votes from old terms
	if term != s.currentTerm {
		log.Printf("[SERVER-%s] [TERM-%d] Ignoring vote from %s for old term %d",
			s.ID, s.currentTerm, from, term)
		return
	}

	// Check for duplicate vote
	if _, dup := s.votersThisTerm[from]; dup {
		log.Printf("[SERVER-%s] [TERM-%d] Duplicate vote from %s, discarding",
			s.ID, term, from)
		return
	}

	// Record the vote
	s.votersThisTerm[from] = struct{}{}
	votes := len(s.votersThisTerm)
	quorum := s.quorumSize()
	totalVoters := len(s.peers) + 1

	log.Printf("[SERVER-%s] [TERM-%d] Vote recorded from %s (%d/%d votes, need %d for quorum)",
		s.ID, term, from, votes, totalVoters, quorum)

	// Check if we've reached quorum
	if votes >= quorum {
		log.Printf("[SERVER-%s] [TERM-%d] Quorum reached! (%d/%d votes)",
			s.ID, term, votes, quorum)
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
		log.Printf("[SERVER-%s] [TERM-%d] Election win event for term %d ignored (state=%s, currentTerm=%d)",
			s.ID, s.currentTerm, term, s.state, s.currentTerm)
		s.mu.Unlock()
		return
	}

	log.Printf("[SERVER-%s] [TERM-%d] Transitioning Candidate → Leader, won election!",
		s.ID, term)

	// Transition to Leader state
	s.state = Leader

	// Clear the voters map as it's only relevant for Candidate state
	s.votersThisTerm = nil

	// Initialize leader-specific volatile state as per Figure 2:
	// "for each server, index of the next log entry to send to that server
	// (initialized to leader last log index + 1)"
	lastLogIndex, _ := s.getLastLogIndexAndTerm()
	s.nextIndex = make(map[ServerID]uint64)
	s.matchIndex = make(map[ServerID]uint64)
	for _, peerID := range s.peers {
		s.nextIndex[peerID] = lastLogIndex + 1
		s.matchIndex[peerID] = 0
	}
	log.Printf("[SERVER-%s] [TERM-%d] Initialized leader state: nextIndex[*]=%d, matchIndex[*]=0 for %d peers",
		s.ID, term, lastLogIndex+1, len(s.peers))

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
	s.StartHeartbeats()
}

// StartHeartbeats starts a background job that periodically sends heartbeats to all peers.
// Section 5.2: "The leader sends periodic heartbeats (AppendEntries RPCs that carry no log entries)
// to all followers in order to maintain its authority"
func (s *Server) StartHeartbeats() {
	// The heartbeat interval should be significantly less than the election timeout
	// to prevent followers from starting elections. The paper recommends sending
	// heartbeats much more frequently than the election timeout (typically 10x).
	// With election timeout of 150-300ms, we use 50ms for heartbeats.
	const heartbeatInterval = 50 * time.Millisecond

	s.mu.Lock()
	currentTerm := s.currentTerm
	s.heartbeatTimer = time.NewTimer(heartbeatInterval)
	s.mu.Unlock()

	log.Printf("[SERVER-%s] [TERM-%d] Starting periodic heartbeats (interval=%v)",
		s.ID, currentTerm, heartbeatInterval)

	go func() {
		for {
			s.mu.RLock()
			timer := s.heartbeatTimer
			s.mu.RUnlock()

			if timer == nil {
				// Timer was stopped, exit the goroutine
				return
			}

			<-timer.C

			// Check if we're still the leader before sending heartbeats
			s.mu.RLock()
			isLeader := s.state == Leader
			timer = s.heartbeatTimer
			s.mu.RUnlock()

			if !isLeader || timer == nil {
				// No longer leader or timer stopped, exit
				return
			}

			// Send heartbeats to all peers
			s.SendHeartbeats()

			// Reset timer for next heartbeat
			timer.Reset(heartbeatInterval)
		}
	}()
}

// StopHeartbeats stops the periodic heartbeat timer. Should be called when stepping down from Leader.
func (s *Server) StopHeartbeats() {
	s.mu.Lock()
	currentTerm := s.currentTerm
	if s.heartbeatTimer != nil {
		s.heartbeatTimer.Stop()
		s.heartbeatTimer = nil
		log.Printf("[SERVER-%s] [TERM-%d] Stopped periodic heartbeats",
			s.ID, currentTerm)
	}
	s.mu.Unlock()
}

// SendHeartbeats sends Heartbeats (empty AppendEntries RPCs) in parallel to all peers in the cluster.
// Each RPC already runs in its own goroutine.
func (s *Server) SendHeartbeats() {
	s.mu.RLock()
	term := s.currentTerm
	peerIDs := append([]ServerID(nil), s.peers...)
	commitIndex := s.commitIndex
	s.mu.RUnlock()

	req := &proto.AppendEntriesRequest{
		Term:         term,
		LeaderId:     string(s.ID),
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: commitIndex,
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

	// Note: Election timer will be started later by StartElectionTimer()
	// after orchestrators are ready to receive events

	log.Printf("[SERVER-%s] [TERM-%d] Raft node running on %s with %d peers %v (electionTimeout=%v)",
		s.ID, s.currentTerm, s.Address, len(s.peers), s.peers, s.serverState.electionTimeout)

	// Start gRPC server in a goroutine so this method doesn't block
	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			log.Printf("[SERVER-%s] gRPC server stopped: %v", s.ID, err)
		}
	}()

	return nil
}

// StartElectionTimer initializes and starts the election timeout timer
// This should be called AFTER orchestrators are ready to receive events
func (s *Server) StartElectionTimer() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Add random startup jitter (0-150ms) to prevent all servers from timing out simultaneously
	// This is critical for initial leader election in clusters where all servers start together
	jitter := time.Duration(rand.Intn(150)) * time.Millisecond
	initialTimeout := s.serverState.electionTimeout + jitter

	// Initialize the timer with jittered timeout
	s.electionTimeoutTimer = time.NewTimer(initialTimeout)

	// Track ElectionTimeout on the background
	ctx := serverCtx{
		ID:    s.ID,
		Addr:  s.Address,
		State: s.state,
		Term:  s.currentTerm,
	}
	go TrackElectionTimeoutJob(ctx, s.electionTimeoutTimer, s.pubSub)

	log.Printf("[SERVER-%s] [TERM-%d] Started election timer with jitter (base=%v, actual=%v)",
		s.ID, s.currentTerm, s.serverState.electionTimeout, initialTimeout)
}

func (s *Server) GracefulShutdown() {
	s.mu.RLock()
	serverID := s.ID
	currentTerm := s.currentTerm
	currentState := s.state
	s.mu.RUnlock()

	log.Printf("[SERVER-%s] [TERM-%d] Shutting down server gracefully (state=%s)", serverID, currentTerm, currentState)

	// First, stop accepting new incoming requests, in order to prevent interrupting a pending response to a peer
	s.grpcServer.GracefulStop()
	log.Printf("[SERVER-%s] Stopped gRPC server", serverID)

	// Then, close all outbound client connections
	s.transport.CloseAllClients()
	log.Printf("[SERVER-%s] Closed all transport connections", serverID)

	// Send a signal to all listeners that the server is shutting down
	pubsub.Publish(s.pubSub, pubsub.NewEvent(ServerShutDown, struct{}{}))
	log.Printf("[SERVER-%s] Shutdown complete", serverID)
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

	electionTimeout := getElectionTimeoutMs()

	log.Printf("[SERVER-%s] Initializing new server at %s (term=%d, electionTimeout=%v, state=Follower)",
		serverID, addr, term, electionTimeout)

	// https://go.dev/doc/effective_go#composite_literals
	return &Server{
		serverState: serverState{
			state:           Follower,
			currentTerm:     term,
			electionTimeout: electionTimeout,
		},
		ID:        serverID,
		Address:   addr,
		transport: NewTransport(peers),
		peers:     peers,
		pubSub:    pubSub,
	}
}
