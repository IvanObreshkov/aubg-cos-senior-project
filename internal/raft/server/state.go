package server

import (
	"aubg-cos-senior-project/internal/raft/proto"
	"sync"
	"time"
)

// serverState is container for different state variables as defined in Figure 2 from the
// [Raft paper](https://raft.github.io/raft.pdf)
// Thread Safety:
//   - Provides atomic getter/setter methods for accessing individual fields
//   - For operations requiring multiple field updates at the same time (transactionally), callers must
//     acquire s.mu directly to maintain consistency across related state changes
type serverState struct {
	// Protects all fields below
	mu sync.RWMutex

	// The state of the server as per Section 5.1 from the [Raft paper](https://raft.github.io/raft.pdf). When a server
	// initially starts it is a Follower as per Section 5.2 from the paper.
	state State
	// The latest term server has seen. It is a [logical clock](https://dl.acm.org/doi/pdf/10.1145/359545.359563) used
	// by servers to detect obsolete info, such as stale leaders. It is initialized to 0 on first boot of the cluster,
	// and increases monotonically, as per Section 5.1 from the [Raft paper](https://raft.github.io/raft.pdf)
	// Updated on stable storage before responding to RPCs (Section 5.2)
	currentTerm uint64
	// The ID of the Candidate Server that the current Server has voted for in the currentTerm. It could be null at the
	// beginning of a new term, as no votes are issued. This should be set to null when the Term changes, as votes in
	// Raft are per Term.
	// Updated on stable storage before responding to RPCs (Section 5.2)
	votedFor *ServerID

	// ElectionTimeout is the current election timeout for the server. It is randomly chosen when the server is created.
	// It should be used with a time.Timer, and the timer should be reset at the beginning of each new election and
	// when the server receives an AppendEntries RPC, as per Section 5.2 from the
	// [Raft paper](https://raft.github.io/raft.pdf). It only makes sense when Server is Follower or Candidate.
	electionTimeout time.Duration
	// votersThisTerm acts as a "set" for per-term vote dedupe.
	// As defined in Section 5.2: "Each server will vote for at most one candidate in a given term, on a
	// first-come-first-served basis."
	// We still deduplicate on the Candidate side because the same voter can respond to multiple duplicated RequestVote
	// requests for the same term (client retries, duplicate deliveries, slow/late responses, etc.)
	// Count each voter at most once per term.
	votersThisTerm map[ServerID]struct{}

	// commitIndex is the index of highest log entry known to be committed (initialized to 0, increases monotonically)
	// as per Figure 2 from the [Raft paper](https://raft.github.io/raft.pdf)
	commitIndex uint64
	// lastApplied is the index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	// as per Figure 2 from the [Raft paper](https://raft.github.io/raft.pdf)
	lastApplied uint64

	// Leader-only volatile state (reinitialized after election)
	// nextIndex is for each server, index of the next log entry to send to that server
	// (initialized to leader last log index + 1) as per Figure 2 from the [Raft paper](https://raft.github.io/raft.pdf)
	nextIndex map[ServerID]uint64
	// matchIndex is for each server, index of highest log entry known to be replicated on server
	// (initialized to 0, increases monotonically) as per Figure 2 from the [Raft paper](https://raft.github.io/raft.pdf)
	matchIndex map[ServerID]uint64

	// lastLeaderContact tracks the last time we received communication from a valid leader.
	// Used for Section 6 optimization: servers disregard RequestVote RPCs when they believe
	// a current leader exists (within minimum election timeout of last leader contact).
	lastLeaderContact time.Time

	// Configuration state (Section 6: Cluster membership changes)
	// committedConfig is the latest committed configuration (C_old or C_new)
	// This is the configuration used once joint consensus is complete
	committedConfig *proto.Configuration
	// latestConfig is the latest configuration seen (may be uncommitted)
	// During joint consensus, this is C_old,new
	latestConfig *proto.Configuration
	// configChangeInProgress tracks whether a configuration change is currently being processed
	// Only one configuration change can be in progress at a time (Section 6)
	configChangeInProgress bool
	// configChangeIndex is the log index of the configuration change being applied
	configChangeIndex uint64
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

func (s *serverState) incrementCurrentTerm() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm++
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

// recordVote adds a voter to the current term's voter set and returns true if this is a new vote.
// Returns false if the voter has already voted this term (duplicate).
func (s *serverState) recordVote(voterID ServerID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already voted
	if _, exists := s.votersThisTerm[voterID]; exists {
		return false // duplicate vote
	}

	// Record the vote
	s.votersThisTerm[voterID] = struct{}{}
	return true // new vote
}

// getVoteCount returns the number of votes received this term
func (s *serverState) getVoteCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.votersThisTerm)
}

// initVotersForTerm initializes the voters map for a new election term
func (s *serverState) initVotersForTerm(capacity int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.votersThisTerm = make(map[ServerID]struct{}, capacity)
}

// clearVoters clears the voters map
func (s *serverState) clearVoters() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.votersThisTerm = nil
}

// getLastLeaderContact returns the time of last contact with a valid leader
func (s *serverState) getLastLeaderContact() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastLeaderContact
}

// setLastLeaderContact updates the time of last contact with a valid leader
func (s *serverState) setLastLeaderContact(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastLeaderContact = t
}

// getCommittedConfig returns the latest committed configuration
func (s *serverState) getCommittedConfig() *proto.Configuration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.committedConfig
}

// setCommittedConfig updates the committed configuration
func (s *serverState) setCommittedConfig(config *proto.Configuration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.committedConfig = config
}

// getLatestConfig returns the latest configuration (may be uncommitted)
func (s *serverState) getLatestConfig() *proto.Configuration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.latestConfig
}

// setLatestConfig updates the latest configuration
func (s *serverState) setLatestConfig(config *proto.Configuration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latestConfig = config
}

// isConfigChangeInProgress returns whether a configuration change is in progress
func (s *serverState) isConfigChangeInProgress() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.configChangeInProgress
}

// setConfigChangeInProgress updates the configuration change in progress flag
func (s *serverState) setConfigChangeInProgress(inProgress bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configChangeInProgress = inProgress
}

// getConfigChangeIndex returns the log index of the configuration change being applied
func (s *serverState) getConfigChangeIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.configChangeIndex
}

// setConfigChangeIndex updates the log index of the configuration change being applied
func (s *serverState) setConfigChangeIndex(index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configChangeIndex = index
}
