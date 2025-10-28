package server

import (
	"aubg-cos-senior-project/internal/pubsub"
	"aubg-cos-senior-project/internal/raft/proto"
	"aubg-cos-senior-project/internal/raft/state_machine"
	"aubg-cos-senior-project/internal/raft/storage"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
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
	Log storage.LogStorage
	// StateMachine is the state machine of the Server as per Section 2 from the
	// [Raft paper](https://raft.github.io/raft.pdf)
	StateMachine state_machine.StateMachine
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

// getLatestConfig returns the latest configuration (may be uncommitted)
func (s *Server) getLatestConfig() *proto.Configuration {
	return s.latestConfig
}

// getCommittedConfig returns the committed configuration
func (s *Server) getCommittedConfig() *proto.Configuration {
	return s.committedConfig
}

// quorumSize calculates the number of nodes required to achieve a quorum (majority), required to agree on a decision
// Section 6: During joint consensus, need majority in BOTH old and new configurations
func (s *Server) quorumSize() int {
	// Check if we're in joint consensus mode
	latestConfig := s.getLatestConfig()
	if latestConfig != nil && latestConfig.IsJoint {
		// During joint consensus, we need quorum in both configurations
		// This method returns the quorum for the new configuration
		// The caller must also check the old configuration separately
		return s.quorumSizeForConfig(latestConfig)
	}

	// Use committed configuration if available
	committedConfig := s.getCommittedConfig()
	if committedConfig != nil {
		return s.quorumSizeForConfig(committedConfig)
	}

	// Fallback to peers-based calculation
	voters := len(s.peers) + 1 // + self
	return voters/2 + 1
}

// getLastLogIndexAndTerm returns the index and term of the last entry in the log.
// Returns (0, 0) if the log is empty, as per Section 5.4.1 from the [Raft paper](https://raft.github.io/raft.pdf)
func (s *Server) getLastLogIndexAndTerm() (uint64, uint64) {
	lastIndex, err := s.Log.GetLastIndex()
	if err != nil {
		log.Printf("[SERVER-%s] Error getting last log index: %v", s.ID, err)
		return 0, 0
	}

	if lastIndex == 0 {
		return 0, 0
	}

	lastTerm, err := s.Log.GetLastTerm()
	if err != nil {
		log.Printf("[SERVER-%s] Error getting last log term: %v", s.ID, err)
		return 0, 0
	}

	return lastIndex, lastTerm
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

// SetPeersWithAddresses updates the list of peers with both IDs and addresses,
// and properly initializes the configuration for the cluster
func (s *Server) SetPeersWithAddresses(peers map[ServerID]ServerAddress) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Extract peer IDs for the transport
	peerIDs := make([]ServerID, 0, len(peers))
	for id := range peers {
		peerIDs = append(peerIDs, id)
	}
	s.peers = peerIDs
	s.transport = NewTransport(peerIDs)

	// Initialize configuration with ALL servers in the cluster
	servers := []*proto.ServerConfig{
		{Id: string(s.ID), Address: string(s.Address)}, // Self
	}
	for id, addr := range peers {
		servers = append(servers, &proto.ServerConfig{
			Id:      string(id),
			Address: string(addr),
		})
	}

	initialConfig := &proto.Configuration{
		Servers: servers,
		IsJoint: false,
	}

	s.latestConfig = initialConfig
	s.committedConfig = initialConfig

	log.Printf("[SERVER-%s] Initialized configuration with %d servers (self + %d peers)",
		s.ID, len(servers), len(peers))
}

// GetPubSub returns the server's PubSub client instance
func (s *Server) GetPubSub() *pubsub.PubSubClient {
	return s.pubSub
}

// persistCurrentTerm persists the current term to stable storage
// Section 5.2: "Updated on stable storage before responding to RPCs"
func (s *Server) persistCurrentTerm(term uint64) error {
	if err := s.Log.SetCurrentTerm(term); err != nil {
		log.Printf("[SERVER-%s] Failed to persist current term %d: %v", s.ID, term, err)
		return err
	}
	return nil
}

// persistVotedFor persists votedFor to stable storage
// Section 5.2: "Updated on stable storage before responding to RPCs"
func (s *Server) persistVotedFor(candidateID *ServerID) error {
	var idStr *string
	if candidateID != nil {
		str := string(*candidateID)
		idStr = &str
	}
	if err := s.Log.SetVotedFor(idStr); err != nil {
		log.Printf("[SERVER-%s] Failed to persist votedFor: %v", s.ID, err)
		return err
	}
	return nil
}

// RequestVote handles the RequestVote RPC call from a peer's client
func (s *Server) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[SERVER-%s] [TERM-%d] [RPC] Received RequestVote from %s (candidateTerm=%d, lastLogIndex=%d, lastLogTerm=%d)",
		s.ID, s.currentTerm, req.CandidateId, req.Term, req.LastLogIndex, req.LastLogTerm)

	// Reply false if term < currentTerm  (Section 5.1)
	if req.Term < s.currentTerm {
		log.Printf("[SERVER-%s] [TERM-%d] [RPC] Rejecting vote to %s (stale term: %d < %d)",
			s.ID, s.currentTerm, req.CandidateId, req.Term, s.currentTerm)
		return &proto.RequestVoteResponse{
			Term:        s.currentTerm,
			VoteGranted: false,
		}, nil
	}

	// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower  (Section 5.1)
	if req.Term > s.currentTerm {
		oldTerm := s.currentTerm
		s.currentTerm = req.Term
		log.Printf("[SERVER-%s] [TERM-%d→%d] Discovered higher term in RequestVote, updating term and reverting to Follower", s.ID, oldTerm, s.currentTerm)
		s.state = Follower
		s.votedFor = nil
		s.votersThisTerm = nil

		// Persist the new term and reset votedFor
		if err := s.Log.SetCurrentTerm(s.currentTerm); err != nil {
			log.Printf("[SERVER-%s] Error persisting current term: %v", s.ID, err)
		}

		if err := s.Log.SetVotedFor(nil); err != nil {
			log.Printf("[SERVER-%s] Error persisting votedFor: %v", s.ID, err)
		}
	}

	// Leaders never grant votes to other candidates in the same term
	// A leader has already won the election for this term, so it should not vote for anyone else
	if s.state == Leader && s.currentTerm == req.Term {
		log.Printf("[SERVER-%s] [TERM-%d] [RPC] Rejecting vote to %s (already Leader in this term)",
			s.ID, s.currentTerm, req.CandidateId)
		return &proto.RequestVoteResponse{
			Term:        s.currentTerm,
			VoteGranted: false,
		}, nil
	}

	// Candidates never grant votes to other candidates in the same term
	// A candidate has already voted for itself (Section 5.2: "Each server will vote for at most one candidate in a given term")
	// This prevents the split-brain scenario where two candidates vote for each other
	if s.state == Candidate && s.currentTerm == req.Term {
		log.Printf("[SERVER-%s] [TERM-%d] [RPC] Rejecting vote to %s (already Candidate in this term, voted for self)",
			s.ID, s.currentTerm, req.CandidateId)
		return &proto.RequestVoteResponse{
			Term:        s.currentTerm,
			VoteGranted: false,
		}, nil
	}

	// Section 6 optimization: "servers disregard RequestVote RPCs when they believe a current leader exists"
	// If we've heard from a valid leader recently (within minimum election timeout), reject the vote.
	// This prevents disruptions from servers that have been partitioned and are now rejoining.
	if !s.lastLeaderContact.IsZero() {
		minElectionTimeout := 150 * time.Millisecond // Minimum from our 150-300ms range
		timeSinceLastContact := time.Since(s.lastLeaderContact)
		if timeSinceLastContact < minElectionTimeout {
			log.Printf("[SERVER-%s] [TERM-%d] [RPC] Rejecting vote to %s (heard from leader %v ago, within minimum election timeout)",
				s.ID, s.currentTerm, req.CandidateId, timeSinceLastContact)

			return &proto.RequestVoteResponse{
				Term:        s.currentTerm,
				VoteGranted: false,
			}, nil
		}
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

		// Persist votedFor to storage (Section 5.2: "Updated on stable storage before responding to RPCs")
		candidateIDStr := string(*s.votedFor)
		if err := s.Log.SetVotedFor(&candidateIDStr); err != nil {
			log.Printf("[SERVER-%s] Error persisting votedFor: %v", s.ID, err)
		}

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

	// Only log AppendEntries that contain actual entries (not empty heartbeats)
	if len(req.Entries) > 0 {
		log.Printf("[SERVER-%s] [TERM-%d] [RPC] Received AppendEntries from Leader %s (leaderTerm=%d, entries=%d, prevLogIndex=%d, prevLogTerm=%d, leaderCommit=%d)",
			s.ID, s.currentTerm, req.LeaderId, req.Term, len(req.Entries), req.PrevLogIndex, req.PrevLogTerm, req.LeaderCommit)
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

		// Persist currentTerm and reset votedFor
		if err := s.Log.SetCurrentTerm(s.currentTerm); err != nil {
			log.Printf("[SERVER-%s] Error persisting current term: %v", s.ID, err)
		}
		if err := s.Log.SetVotedFor(nil); err != nil {
			log.Printf("[SERVER-%s] Error persisting votedFor: %v", s.ID, err)
		}

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

	// Section 5.2: If a Candidate receives AppendEntries from a leader with term >= currentTerm,
	// it recognizes the leader as legitimate and reverts to Follower
	if s.state == Candidate && req.Term == s.currentTerm {
		log.Printf("[SERVER-%s] [TERM-%d] Candidate received AppendEntries from Leader, reverting to Follower",
			s.ID, s.currentTerm)
		s.state = Follower
		// Clear candidate-specific state
		s.votersThisTerm = nil
	}

	// Reset election timeout since we received communication from a leader
	// If this is a newly joined server that hasn't started its election timer yet, start it now
	if s.electionTimeoutTimer != nil {
		s.electionTimeoutTimer.Reset(s.electionTimeout)
	} else if s.state == Follower {
		// This is a new server joining the cluster - start its election timer now.
		// We do this AFTER receiving the first AppendEntries to avoid split-brain scenarios
		// and ensure the new server is properly synchronized before participating in elections.
		//
		// Why wait for the first AppendEntries?
		// =====================================
		// 1. Discovery & Synchronization:
		//    - Learn the current term and who the leader is
		//    - Begin catching up on the log (receiving missing entries)
		//    - Understand the current cluster state before making decisions
		//
		// 2. Avoid Premature Elections:
		//    If we started the election timer immediately upon server startup, the new
		//    server could time out and start an election before receiving any communication
		//    from the leader. This would:
		//    - Disrupt the stable leader with unnecessary elections
		//    - Cause split votes since the new server's log is behind
		//    - Create a split-brain scenario where the new server competes for leadership
		//      while being completely out of sync with the cluster
		//
		// 3. Safe Integration:
		//    By waiting for the first AppendEntries, we ensure:
		//    - The new server acknowledges the current leader's authority
		//    - The server begins log replication and catches up on committed entries
		//    - The server is ready to participate in consensus with up-to-date information
		//    - The cluster remains stable during the membership change
		//
		// This approach implements Raft's safe server addition protocol (Section 6 of the paper),
		// where new servers must catch up before being able to fully participate in the cluster.
		log.Printf("[SERVER-%s] [TERM-%d] Starting election timer after receiving first AppendEntries from leader",
			s.ID, s.currentTerm)
		s.mu.Unlock()
		s.StartElectionTimer()
		s.mu.Lock()
	}

	// Update last leader contact time for Section 6 optimization
	s.lastLeaderContact = time.Now()

	// Handle commit index updates even for heartbeats (empty entries)
	// This is critical for propagating commit index to followers
	// Section 5.3: "If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)"
	if len(req.Entries) == 0 {
		// This is a heartbeat - still need to update commitIndex if leaderCommit is higher
		if req.LeaderCommit > s.commitIndex {
			lastLogIndex, err := s.Log.GetLastIndex()
			if err != nil {
				log.Printf("[SERVER-%s] Error getting last log index: %v", s.ID, err)
			} else {
				oldCommitIndex := s.commitIndex
				// Set commitIndex to min(leaderCommit, lastLogIndex)
				if req.LeaderCommit < lastLogIndex {
					s.commitIndex = req.LeaderCommit
				} else {
					s.commitIndex = lastLogIndex
				}

				// Only log if commitIndex actually changed
				if s.commitIndex != oldCommitIndex {
					log.Printf("[SERVER-%s] [TERM-%d] Updated commitIndex from %d to %d via heartbeat (lastLogIndex=%d, leaderCommit=%d)",
						s.ID, s.currentTerm, oldCommitIndex, s.commitIndex, lastLogIndex, req.LeaderCommit)

					// Release lock before applying entries to avoid blocking
					s.mu.Unlock()
					// Apply newly committed entries to state machine
					s.applyCommittedEntries()
					// Re-acquire lock for the defer unlock
					s.mu.Lock()
				}
			}
		}

		return &proto.AppendEntriesResponse{
			Term:    s.currentTerm,
			Success: true,
		}, nil
	}

	// Handle log replication (Section 5.3)

	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if req.PrevLogIndex > 0 {
		prevEntry, err := s.Log.GetEntry(req.PrevLogIndex)
		if err != nil {
			// Entry doesn't exist at prevLogIndex
			log.Printf("[SERVER-%s] [TERM-%d] [RPC] Rejecting AppendEntries: no entry at prevLogIndex=%d",
				s.ID, s.currentTerm, req.PrevLogIndex)
			return &proto.AppendEntriesResponse{
				Term:    s.currentTerm,
				Success: false,
			}, nil
		}

		if prevEntry.Term != req.PrevLogTerm {
			// Term mismatch at prevLogIndex
			log.Printf("[SERVER-%s] [TERM-%d] [RPC] Rejecting AppendEntries: term mismatch at prevLogIndex=%d (expected=%d, got=%d)",
				s.ID, s.currentTerm, req.PrevLogIndex, req.PrevLogTerm, prevEntry.Term)
			return &proto.AppendEntriesResponse{
				Term:    s.currentTerm,
				Success: false,
			}, nil
		}
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (Section 5.3)
	for _, newEntry := range req.Entries {
		existingEntry, err := s.Log.GetEntry(newEntry.Index)
		if err == nil && existingEntry.Term != newEntry.Term {
			// Conflict found - delete this entry and all following entries
			log.Printf("[SERVER-%s] [TERM-%d] [RPC] Log conflict at index %d (existing term=%d, new term=%d), truncating log",
				s.ID, s.currentTerm, newEntry.Index, existingEntry.Term, newEntry.Term)
			if err := s.Log.DeleteEntriesFrom(newEntry.Index); err != nil {
				log.Printf("[SERVER-%s] Error deleting conflicting log entries: %v", s.ID, err)
				return &proto.AppendEntriesResponse{
					Term:    s.currentTerm,
					Success: false,
				}, nil
			}
			break
		}
	}

	// Append any new entries not already in the log
	var entriesToAppend []*proto.LogEntry
	for _, protoEntry := range req.Entries {
		// Check if entry already exists
		_, err := s.Log.GetEntry(protoEntry.Index)
		if err != nil {
			// Entry doesn't exist, add it to the list
			entriesToAppend = append(entriesToAppend, protoEntry)
		}
	}

	if len(entriesToAppend) > 0 {
		if err := s.Log.AppendEntries(entriesToAppend); err != nil {
			log.Printf("[SERVER-%s] Error appending log entries: %v", s.ID, err)
			return &proto.AppendEntriesResponse{
				Term:    s.currentTerm,
				Success: false,
			}, nil
		}
		log.Printf("[SERVER-%s] [TERM-%d] [RPC] Appended %d new entries to log",
			s.ID, s.currentTerm, len(entriesToAppend))
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if req.LeaderCommit > s.commitIndex {
		// Find the index of the last entry in our log
		// This could be from the new entries we just appended, or from entries we already had
		lastLogIndex, err := s.Log.GetLastIndex()
		if err != nil {
			log.Printf("[SERVER-%s] Error getting last log index: %v", s.ID, err)
		} else {
			oldCommitIndex := s.commitIndex
			// Set commitIndex to min(leaderCommit, lastLogIndex)
			if req.LeaderCommit < lastLogIndex {
				s.commitIndex = req.LeaderCommit
			} else {
				s.commitIndex = lastLogIndex
			}
			log.Printf("[SERVER-%s] [TERM-%d] Updated commitIndex from %d to %d (lastLogIndex=%d, leaderCommit=%d)",
				s.ID, s.currentTerm, oldCommitIndex, s.commitIndex, lastLogIndex, req.LeaderCommit)

			// Release lock before applying entries to avoid blocking
			// applyCommittedEntries() needs to acquire the lock itself
			s.mu.Unlock()
			// Apply newly committed entries to state machine
			// This must be called synchronously to ensure entries are applied before we return
			s.applyCommittedEntries()
			// Re-acquire lock for the defer unlock
			s.mu.Lock()
		}
	}

	log.Printf("[SERVER-%s] [TERM-%d] [RPC] Successfully replicated %d entries from Leader %s",
		s.ID, s.currentTerm, len(req.Entries), req.LeaderId)

	return &proto.AppendEntriesResponse{
		Term:    s.currentTerm,
		Success: true,
	}, nil
}

// ClientCommand handles client requests to submit commands to the Raft cluster
// Section 5.3: "If command received from client: append entry to local log, respond after entry applied to state machine"
func (s *Server) ClientCommand(ctx context.Context, req *proto.ClientCommandRequest) (*proto.ClientCommandResponse, error) {
	s.mu.Lock()

	// Check if we're the leader
	if s.state != Leader {
		// Not the leader - try to redirect to leader if known
		leaderID := ""
		// In a production system, we'd track the leader ID from AppendEntries RPCs
		// For now, we just return empty leader ID

		log.Printf("[SERVER-%s] [TERM-%d] [RPC] ClientCommand rejected - not the leader",
			s.ID, s.currentTerm)
		s.mu.Unlock()
		return &proto.ClientCommandResponse{
			Success:  false,
			Index:    0,
			LeaderId: leaderID,
		}, nil
	}

	// Get the next log index
	lastIndex, err := s.Log.GetLastIndex()
	if err != nil {
		s.mu.Unlock()
		log.Printf("[SERVER-%s] Error getting last log index: %v", s.ID, err)
		return &proto.ClientCommandResponse{
			Success:  false,
			Index:    0,
			LeaderId: string(s.ID),
		}, fmt.Errorf("failed to get last log index: %w", err)
	}

	nextIndex := lastIndex + 1
	currentTerm := s.currentTerm

	// Create the new log entry
	entry := &proto.LogEntry{
		Index:   nextIndex,
		Term:    currentTerm,
		Command: req.Command,
		Type:    proto.LogEntryType_LOG_COMMAND,
	}

	// Append to local log
	if err := s.Log.AppendEntry(entry); err != nil {
		s.mu.Unlock()
		log.Printf("[SERVER-%s] Error appending entry to log: %v", s.ID, err)
		return &proto.ClientCommandResponse{
			Success:  false,
			Index:    0,
			LeaderId: string(s.ID),
		}, fmt.Errorf("failed to append entry to log: %w", err)
	}

	log.Printf("[SERVER-%s] [TERM-%d] [CLIENT] Appended command to log at index %d: %s",
		s.ID, currentTerm, nextIndex, string(req.Command))

	s.mu.Unlock()

	// Replicate to followers immediately
	go s.replicateToFollowers()

	// For simplicity in this demo, we return success immediately
	// In a production system, you'd wait for the entry to be committed before returning
	return &proto.ClientCommandResponse{
		Success:  true,
		Index:    nextIndex,
		LeaderId: string(s.ID),
	}, nil
}

// GetServerState handles requests to query the current state of the server
// This is useful for debugging and demo purposes
func (s *Server) GetServerState(ctx context.Context, req *proto.GetServerStateRequest) (*proto.GetServerStateResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lastIndex, _ := s.Log.GetLastIndex()

	return &proto.GetServerStateResponse{
		ServerId:        string(s.ID),
		CurrentTerm:     s.currentTerm,
		State:           s.state.String(),
		LastLogIndex:    lastIndex,
		CommitIndex:     s.commitIndex,
		LastApplied:     s.lastApplied,
		LeaderId:        "", // We don't track leader ID in follower state
		LogEntriesCount: lastIndex,
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
	oldState := s.state
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
		s.ID, oldTerm, s.currentTerm, oldState)

	// 3. The server Votes for itself exactly once
	s.votedFor = &s.ID
	s.votersThisTerm[s.ID] = struct{}{}
	log.Printf("[SERVER-%s] [TERM-%d] Voting for self (1/%d votes, need %d for quorum)",
		s.ID, s.currentTerm, len(s.peers)+1, s.quorumSize())

	// Persist currentTerm and votedFor to storage (Section 5.2: "Updated on stable storage before responding to RPCs")
	if err := s.Log.SetCurrentTerm(s.currentTerm); err != nil {
		log.Printf("[SERVER-%s] Error persisting current term: %v", s.ID, err)
	}

	votedForStr := string(s.ID)
	if err := s.Log.SetVotedFor(&votedForStr); err != nil {
		log.Printf("[SERVER-%s] Error persisting votedFor: %v", s.ID, err)
	}

	// Reset election timeout with a NEW random value to prevent split votes
	// Section 5.2: "Raft uses randomized election timeouts to ensure that split votes are rare
	// and that they are resolved quickly"
	s.electionTimeout = getElectionTimeoutMs()
	s.electionTimeoutTimer.Reset(s.electionTimeout)

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
		// Defensive check: never send RequestVote to ourselves
		// This prevents the bug where a server votes for itself twice (once locally, once via RPC)
		if peer == s.ID {
			log.Printf("[SERVER-%s] [TERM-%d] WARNING: Self (%s) found in peers list, skipping RequestVote to self",
				s.ID, termForReq, peer)
			continue
		}

		// We do this in order to have parallelism but also waiting for a resp is a blocking operation, so we do this
		// in another thread.
		go func(peerID ServerID) {
			reqCtx := context.Background()
			SetServerCurrTerm(reqCtx, termForReq)
			SetServerID(reqCtx, s.ID)
			SetServerAddr(reqCtx, s.Address)

			resp, err := s.transport.RequestVote(reqCtx, peerID, req)
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
						s.ID, oldTerm, s.currentTerm, peerID, oldState)

					// Persist term change
					if err := s.Log.SetCurrentTerm(s.currentTerm); err != nil {
						log.Printf("[SERVER-%s] Error persisting current term: %v", s.ID, err)
					}
					if err := s.Log.SetVotedFor(nil); err != nil {
						log.Printf("[SERVER-%s] Error persisting votedFor: %v", s.ID, err)
					}

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
					s.ID, termForReq, peerID, resp.Term, termForReq)
				return
			}

			// Count granted votes only for THIS election's term snapshot
			if resp.VoteGranted && resp.Term == termForReq {
				log.Printf("[SERVER-%s] [TERM-%d] Received vote from %s",
					s.ID, termForReq, peerID)
				pubsub.Publish(s.pubSub, pubsub.NewEvent(VoteGranted, VoteGrantedPayload{
					From: peerID,
					Term: termForReq,
				}))
			} else if !resp.VoteGranted {
				log.Printf("[SERVER-%s] [TERM-%d] Vote denied by %s",
					s.ID, termForReq, peerID)
			}
		}(peer)
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
	// Need to capture nextIndex for each peer to set proper prevLogIndex/prevLogTerm
	nextIndexMap := make(map[ServerID]uint64)
	for _, peerID := range peerIDs {
		nextIndexMap[peerID] = s.nextIndex[peerID]
	}
	s.mu.RUnlock()

	// Reduced logging - only log heartbeats when commitIndex changes or on errors

	for _, peerID := range peerIDs {
		// Defensive check: Skip sending heartbeat to ourselves
		if peerID == s.ID {
			log.Printf("[SERVER-%s] WARNING: Skipping heartbeat to self (peers list may be corrupted)", s.ID)
			continue
		}

		nextIndex := nextIndexMap[peerID]
		prevLogIndex := nextIndex - 1
		prevLogTerm := uint64(0)

		// Get the term of the previous log entry
		if prevLogIndex > 0 {
			entry, err := s.Log.GetEntry(prevLogIndex)
			if err != nil {
				log.Printf("[SERVER-%s] Error getting entry at index %d for heartbeat to %s: %v",
					s.ID, prevLogIndex, peerID, err)
				continue
			}
			prevLogTerm = entry.Term
		}

		req := &proto.AppendEntriesRequest{
			Term:         term,
			LeaderId:     string(s.ID),
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      nil, // Empty for heartbeat
			LeaderCommit: commitIndex,
		}

		go func(id ServerID, request *proto.AppendEntriesRequest) {
			_, err := s.transport.AppendEntries(context.Background(), id, request)
			if err != nil {
				log.Printf("[SERVER-%s] Heartbeat to %s failed: %v", s.ID, id, err)
			}
		}(peerID, req)
	}
}

// SubmitCommand is called by clients to submit a command to the Raft cluster.
// Section 5.3: "Clients send all of their requests to the leader... If a client contacts a follower,
// the follower redirects it to the leader."
// Returns an error if this server is not the leader or if the command cannot be appended to the log.
func (s *Server) SubmitCommand(command []byte) error {
	s.mu.Lock()

	// Check if we're the leader
	if s.state != Leader {
		s.mu.Unlock()
		return fmt.Errorf("not the leader")
	}

	// Get the next index for this entry
	lastIndex, err := s.Log.GetLastIndex()
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to get last index: %w", err)
	}
	nextIndex := lastIndex + 1

	// Create a new log entry with the command
	entry := &proto.LogEntry{
		Index:   nextIndex,
		Term:    s.currentTerm,
		Command: command,
	}

	// Append the entry to our log
	err = s.Log.AppendEntry(entry)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to append entry to log: %w", err)
	}

	log.Printf("[SERVER-%s] [TERM-%d] Appended command to log at index %d",
		s.ID, s.currentTerm, nextIndex)

	s.mu.Unlock()

	// Replicate to followers immediately (don't wait for heartbeat interval)
	// This ensures faster replication of client commands
	go s.ReplicateToFollowers()

	return nil
}

// ReplicateToFollowers replicates log entries to all followers in parallel.
// This is similar to SendHeartbeats but includes actual log entries.
func (s *Server) ReplicateToFollowers() {
	s.mu.RLock()

	// Check if we're still the leader
	if s.state != Leader {
		s.mu.RUnlock()
		return
	}

	term := s.currentTerm
	peerIDs := append([]ServerID(nil), s.peers...)
	commitIndex := s.commitIndex
	s.mu.RUnlock()

	// Send AppendEntries to each follower in parallel
	for _, peerID := range peerIDs {
		go s.sendAppendEntriesToPeer(peerID, term, commitIndex)
	}
}

// sendAppendEntriesToPeer sends AppendEntries RPC to a specific peer, handling log replication.
// This implements the leader's log replication logic from Section 5.3.
func (s *Server) sendAppendEntriesToPeer(peerID ServerID, term uint64, commitIndex uint64) {
	s.mu.RLock()

	// Get the next index to send to this peer
	nextIndex, exists := s.nextIndex[peerID]
	if !exists {
		s.mu.RUnlock()
		return
	}

	// Get the previous log entry's index and term
	prevLogIndex := nextIndex - 1
	prevLogTerm := uint64(0)

	if prevLogIndex > 0 {
		entry, err := s.Log.GetEntry(prevLogIndex)
		if err != nil {
			s.mu.RUnlock()
			log.Printf("[SERVER-%s] Error getting entry at index %d: %v", s.ID, prevLogIndex, err)
			return
		}
		prevLogTerm = entry.Term
	}

	// Get entries to send (from nextIndex onwards)
	lastIndex, err := s.Log.GetLastIndex()
	if err != nil {
		s.mu.RUnlock()
		log.Printf("[SERVER-%s] Error getting last index: %v", s.ID, err)
		return
	}

	var entries []*proto.LogEntry
	if lastIndex >= nextIndex {
		storageEntries, err := s.Log.GetEntries(nextIndex, lastIndex)
		if err != nil {
			s.mu.RUnlock()
			log.Printf("[SERVER-%s] Error getting entries %d-%d: %v", s.ID, nextIndex, lastIndex, err)
			return
		}

		// Convert storage.LogEntry to proto.LogEntry
		// IMPORTANT: Must copy ALL fields, not just Term and Command
		for _, e := range storageEntries {
			entries = append(entries, &proto.LogEntry{
				Index:         e.Index,
				Term:          e.Term,
				Type:          e.Type,
				Command:       e.Command,
				Configuration: e.Configuration,
			})
		}
	}

	s.mu.RUnlock()

	// Build the AppendEntries request
	req := &proto.AppendEntriesRequest{
		Term:         term,
		LeaderId:     string(s.ID),
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}

	// Send the RPC
	resp, err := s.transport.AppendEntries(context.Background(), peerID, req)
	if err != nil {
		// Check if this is a "peer not found" error (removed from cluster)
		// In this case, silently return - it's expected behavior during membership changes
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "removed from cluster") {
			return
		}
		// For other errors, log them
		log.Printf("[TRANSPORT] AppendEntries to %s failed: %v", peerID, err)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if we're still the leader and in the same term
	if s.state != Leader || s.currentTerm != term {
		return
	}

	// Handle response
	if resp.Term > s.currentTerm {
		// Discovered a higher term, step down
		log.Printf("[SERVER-%s] [TERM-%d→%d] Discovered higher term in AppendEntries response from %s, stepping down",
			s.ID, s.currentTerm, resp.Term, peerID)
		s.currentTerm = resp.Term
		s.state = Follower
		s.votedFor = nil
		s.votersThisTerm = nil
		s.nextIndex = nil
		s.matchIndex = nil

		// Persist the term change
		if err := s.Log.SetCurrentTerm(s.currentTerm); err != nil {
			log.Printf("[SERVER-%s] Error persisting current term: %v", s.ID, err)
		}
		if err := s.Log.SetVotedFor(nil); err != nil {
			log.Printf("[SERVER-%s] Error persisting votedFor: %v", s.ID, err)
		}

		s.StopHeartbeats()
		return
	}

	if resp.Success {
		// Successfully replicated entries
		// Update nextIndex and matchIndex for this follower
		if len(entries) > 0 {
			lastEntryIndex := prevLogIndex + uint64(len(entries))
			s.nextIndex[peerID] = lastEntryIndex + 1
			s.matchIndex[peerID] = lastEntryIndex

			log.Printf("[SERVER-%s] [TERM-%d] Successfully replicated %d entries to %s (matchIndex=%d)",
				s.ID, term, len(entries), peerID, lastEntryIndex)

			// Check if we can advance commitIndex
			s.updateCommitIndex()
		}
	} else {
		// Log inconsistency, decrement nextIndex and retry
		if s.nextIndex[peerID] > 1 {
			s.nextIndex[peerID]--
			log.Printf("[SERVER-%s] [TERM-%d] Log inconsistency with %s, decremented nextIndex to %d",
				s.ID, term, peerID, s.nextIndex[peerID])

			// Retry immediately
			go s.sendAppendEntriesToPeer(peerID, term, commitIndex)
		}
	}
}

// updateCommitIndex updates the leader's commitIndex based on matchIndex of followers.
// Section 5.3 and 5.4: "If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
// and log[N].term == currentTerm: set commitIndex = N"
// Section 6: During joint consensus, need majority in BOTH old and new configurations
// Must be called with s.mu held.
func (s *Server) updateCommitIndex() {
	// Build a sorted list of match indices (including our own last log index)
	lastIndex, err := s.Log.GetLastIndex()
	if err != nil {
		log.Printf("[SERVER-%s] Error getting last index: %v", s.ID, err)
		return
	}

	// Check if we're in joint consensus
	latestConfig := s.getLatestConfig()
	isJoint := latestConfig != nil && latestConfig.IsJoint

	// Try each index from highest down to current commitIndex + 1
	for n := lastIndex; n > s.commitIndex; n-- {
		// Check that the entry's term matches currentTerm (safety check from Section 5.4.2)
		entry, err := s.Log.GetEntry(n)
		if err != nil || entry.Term != s.currentTerm {
			continue
		}

		// Count how many servers have replicated this entry
		replicatedServers := make(map[ServerID]bool)
		replicatedServers[s.ID] = true // Leader has it

		for peerID, matchIndex := range s.matchIndex {
			if matchIndex >= n {
				replicatedServers[peerID] = true
			}
		}

		// Check if we have quorum
		hasQuorum := false
		if isJoint {
			// Joint consensus: need quorum in BOTH old and new configurations
			hasQuorum = s.hasJointQuorum(latestConfig, replicatedServers)
		} else {
			// Normal case: need simple majority
			committedConfig := s.getCommittedConfig()
			if committedConfig != nil {
				hasQuorum = s.isQuorumInConfig(committedConfig, replicatedServers)
			} else {
				// Fallback: use peer count
				count := len(replicatedServers)
				quorum := (len(s.peers)+1)/2 + 1
				hasQuorum = count >= quorum
			}
		}

		if hasQuorum {
			// Found the highest N that can be committed
			oldCommitIndex := s.commitIndex
			s.commitIndex = n

			// Calculate how many new entries were committed
			numNewCommits := n - oldCommitIndex
			if numNewCommits == 1 {
				log.Printf("[SERVER-%s] [TERM-%d] ✓ Entry %d is now COMMITTED (majority replicated)",
					s.ID, s.currentTerm, n)
			} else {
				log.Printf("[SERVER-%s] [TERM-%d] ✓ Entries %d-%d are now COMMITTED (majority replicated)",
					s.ID, s.currentTerm, oldCommitIndex+1, n)
			}

			// IMPORTANT: Release the lock before sending heartbeats to avoid blocking
			// We need to release the lock because SendHeartbeats() will try to acquire a read lock
			s.mu.Unlock()

			// Immediately send heartbeats to propagate the new commitIndex to followers
			// This ensures followers update their commitIndex without waiting for the next periodic heartbeat
			log.Printf("[SERVER-%s] [TERM-%d] Sending immediate heartbeats to propagate commitIndex=%d",
				s.ID, s.currentTerm, s.commitIndex)
			s.SendHeartbeats()

			// Apply newly committed entries to the state machine
			// This is called after heartbeats so followers can also start applying
			// It will acquire its own lock as needed
			s.applyCommittedEntries()

			// Re-acquire the lock before returning (caller expects lock to be held)
			s.mu.Lock()
			return
		}
	}
}

// applyCommittedEntries applies all committed but not yet applied log entries to the state machine
// This implements the rule: "If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine"
func (s *Server) applyCommittedEntries() {
	s.mu.Lock()

	// Check if there are entries to apply
	if s.commitIndex <= s.lastApplied {
		s.mu.Unlock()
		return
	}

	startIndex := s.lastApplied + 1
	endIndex := s.commitIndex
	s.mu.Unlock()

	// Fetch entries to apply (outside the lock to avoid blocking)
	entries, err := s.Log.GetEntries(startIndex, endIndex)
	if err != nil {
		log.Printf("[SERVER-%s] Error fetching committed entries [%d,%d]: %v",
			s.ID, startIndex, endIndex, err)
		return
	}

	// Apply each entry to the state machine
	for _, entry := range entries {
		// Check if this is a configuration entry (Section 6)
		if entry.Type == proto.LogEntryType_LOG_CONFIGURATION {
			// Apply configuration change
			if err := s.applyConfigurationEntry(entry); err != nil {
				log.Printf("[SERVER-%s] Error applying configuration entry %d: %v",
					s.ID, entry.Index, err)
			}
		} else if s.StateMachine != nil {
			// Regular command - apply to state machine
			s.StateMachine.Apply([]proto.LogEntry{*entry})
		}

		// Update lastApplied
		s.mu.Lock()
		s.lastApplied = entry.Index
		s.mu.Unlock()

		log.Printf("[SERVER-%s] Applied log entry %d to state machine (type=%v)",
			s.ID, entry.Index, entry.Type)
	}
}

// ReplicateCommand is called by clients to replicate a command. Only Leaders can accept commands.
// Section 5.3: "If command received from client: append entry to local log, respond after entry applied to state machine"
func (s *Server) ReplicateCommand(command []byte) error {
	s.mu.Lock()

	// Only leaders can accept commands
	if s.state != Leader {
		s.mu.Unlock()
		return fmt.Errorf("not a leader")
	}

	// Get the next log index
	lastIndex, err := s.Log.GetLastIndex()
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to get last log index: %w", err)
	}

	nextIndex := lastIndex + 1
	currentTerm := s.currentTerm

	// Create the new log entry
	entry := &proto.LogEntry{
		Index:   nextIndex,
		Term:    currentTerm,
		Command: command,
	}

	// Append to local log
	if err := s.Log.AppendEntry(entry); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to append entry to log: %w", err)
	}

	log.Printf("[SERVER-%s] [TERM-%d] Appended command to log at index %d",
		s.ID, currentTerm, nextIndex)

	s.mu.Unlock()

	// Replicate to followers
	s.replicateToFollowers()

	return nil
}

// replicateToFollowers sends AppendEntries RPCs to all followers to replicate log entries
func (s *Server) replicateToFollowers() {
	s.mu.RLock()
	if s.state != Leader {
		s.mu.RUnlock()
		return
	}

	currentTerm := s.currentTerm
	peerIDs := append([]ServerID(nil), s.peers...)
	commitIndex := s.commitIndex
	s.mu.RUnlock()

	for _, peerID := range peerIDs {
		go s.replicateToPeer(peerID, currentTerm, commitIndex)
	}
}

// replicateToPeer sends AppendEntries RPC to a specific peer
func (s *Server) replicateToPeer(peerID ServerID, term uint64, leaderCommit uint64) {
	s.mu.RLock()
	nextIndex := s.nextIndex[peerID]
	s.mu.RUnlock()

	// Get the previous log entry (the one right before nextIndex)
	var prevLogIndex uint64
	var prevLogTerm uint64

	if nextIndex > 1 {
		prevLogIndex = nextIndex - 1
		prevEntry, err := s.Log.GetEntry(prevLogIndex)
		if err != nil {
			log.Printf("[SERVER-%s] Error getting log entry %d for peer %s: %v",
				s.ID, prevLogIndex, peerID, err)
			return
		}
		prevLogTerm = prevEntry.Term
	}

	// Get entries to send (from nextIndex onwards)
	entries, err := s.Log.GetEntriesFrom(nextIndex)
	if err != nil {
		log.Printf("[SERVER-%s] Error getting log entries from %d: %v",
			s.ID, nextIndex, err)
		return
	}

	// Convert to proto entries
	var protoEntries []*proto.LogEntry
	for _, entry := range entries {
		protoEntries = append(protoEntries, &proto.LogEntry{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: entry.Command,
		})
	}

	// Send AppendEntries RPC
	req := &proto.AppendEntriesRequest{
		Term:         term,
		LeaderId:     string(s.ID),
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      protoEntries,
		LeaderCommit: leaderCommit,
	}

	resp, err := s.transport.AppendEntries(context.Background(), peerID, req)
	if err != nil {
		// Transport already logged the error
		return
	}

	// Handle response
	s.handleAppendEntriesResponse(peerID, req, resp)
}

// handleAppendEntriesResponse processes the response from an AppendEntries RPC
func (s *Server) handleAppendEntriesResponse(peerID ServerID, req *proto.AppendEntriesRequest, resp *proto.AppendEntriesResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If we're no longer a leader or term has changed, ignore the response
	if s.state != Leader || s.currentTerm != req.Term {
		return
	}

	// If response contains a higher term, step down
	if resp.Term > s.currentTerm {
		log.Printf("[SERVER-%s] [TERM-%d→%d] Discovered higher term in AppendEntries response, stepping down",
			s.ID, s.currentTerm, resp.Term)
		s.currentTerm = resp.Term
		s.state = Follower
		s.votedFor = nil
		s.votersThisTerm = nil
		s.nextIndex = nil
		s.matchIndex = nil

		// Persist the term change
		if err := s.Log.SetCurrentTerm(s.currentTerm); err != nil {
			log.Printf("[SERVER-%s] Error persisting current term: %v", s.ID, err)
		}
		if err := s.Log.SetVotedFor(nil); err != nil {
			log.Printf("[SERVER-%s] Error persisting votedFor: %v", s.ID, err)
		}

		s.mu.Unlock()
		s.StopHeartbeats()
		s.mu.Lock()
		return
	}

	if resp.Success {
		// Update nextIndex and matchIndex for this peer
		if len(req.Entries) > 0 {
			lastEntryIndex := req.Entries[len(req.Entries)-1].Index
			s.nextIndex[peerID] = lastEntryIndex + 1
			s.matchIndex[peerID] = lastEntryIndex

			// Shorten peer ID for readability
			shortPeerID := string(peerID)
			if len(shortPeerID) > 12 {
				shortPeerID = shortPeerID[:12] + "..."
			}

			log.Printf("[SERVER-%s] [TERM-%d] ✓ Follower %s acknowledged replication up to index %d",
				s.ID, s.currentTerm, shortPeerID, lastEntryIndex)

			// Check if we can advance commitIndex
			s.updateCommitIndex()
		}
	} else {
		// Replication failed - decrement nextIndex and retry
		if s.nextIndex[peerID] > 1 {
			s.nextIndex[peerID]--
			log.Printf("[SERVER-%s] [TERM-%d] AppendEntries to %s failed, decrementing nextIndex to %d",
				s.ID, s.currentTerm, peerID, s.nextIndex[peerID])

			// Retry replication in background
			go s.replicateToPeer(peerID, s.currentTerm, s.commitIndex)
		}
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
	baseTimeout := s.serverState.electionTimeout
	jitter := time.Duration(rand.Intn(150)) * time.Millisecond
	initialTimeout := baseTimeout + jitter

	// Store the jittered timeout as the current election timeout
	// This ensures consistency throughout the first election cycle
	s.electionTimeout = initialTimeout

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
		s.ID, s.currentTerm, baseTimeout, initialTimeout)
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

	// Close the database connection
	if err := s.Log.Close(); err != nil {
		log.Printf("[SERVER-%s] Error closing database: %v", serverID, err)
	} else {
		log.Printf("[SERVER-%s] Closed database connection", serverID)
	}

	// Send a signal to all listeners that the server is shutting down
	pubsub.Publish(s.pubSub, pubsub.NewEvent(ServerShutDown, struct{}{}))
	log.Printf("[SERVER-%s] Shutdown complete", serverID)
}

func (s *Server) ForceShutdown() {
	log.Printf("Force shutting down server %s", s.ID)
	s.transport.CloseAllClients()
	s.grpcServer.Stop()

	// Close the database connection
	if err := s.Log.Close(); err != nil {
		log.Printf("[SERVER-%s] Error closing database during force shutdown: %v", s.ID, err)
	}

	// Send a signal to all listeners that the server is shutting down
	pubsub.Publish(s.pubSub, pubsub.NewEvent(ServerShutDown, struct{}{}))
}

func NewServer(currentTerm uint64, addr ServerAddress, peers []ServerID, pubSub *pubsub.PubSubClient) *Server {
	// Generate a unique ID for this server
	serverID := ServerID(uuid.New().String())

	// Register this server with the resolver
	RegisterResolverPeer(serverID, addr)

	electionTimeout := getElectionTimeoutMs()

	// Initialize BBolt storage
	// Use server ID in the path to ensure each server has its own database
	storagePath := fmt.Sprintf("./data/raft-%s.db", serverID)
	store, err := storage.NewBboltStorage(storagePath)
	if err != nil {
		log.Fatalf("[SERVER-%s] Failed to initialize storage: %v", serverID, err)
	}

	// Load persisted state from storage
	persistedTerm, err := store.GetCurrentTerm()
	if err != nil {
		log.Fatalf("[SERVER-%s] Failed to load current term: %v", serverID, err)
	}

	// Use persisted term if it's higher than the provided one
	term := currentTerm
	if persistedTerm > term {
		term = persistedTerm
	}

	// Load votedFor from storage
	persistedVotedFor, err := store.GetVotedFor()
	if err != nil {
		log.Fatalf("[SERVER-%s] Failed to load votedFor: %v", serverID, err)
	}

	var votedFor *ServerID
	if persistedVotedFor != nil {
		id := ServerID(*persistedVotedFor)
		votedFor = &id
	}

	log.Printf("[SERVER-%s] Initializing new server at %s (term=%d, electionTimeout=%v, state=Follower)",
		serverID, addr, term, electionTimeout)

	// https://go.dev/doc/effective_go#composite_literals
	server := &Server{
		serverState: serverState{
			state:           Follower,
			currentTerm:     term,
			votedFor:        votedFor,
			electionTimeout: electionTimeout,
		},
		ID:           serverID,
		Address:      addr,
		Log:          store,
		StateMachine: state_machine.NewKVStateMachine(string(serverID)),
		transport:    NewTransport(peers),
		peers:        peers,
		pubSub:       pubSub,
	}

	// Initialize configuration (Section 6)
	server.initializeConfiguration()

	return server
}
