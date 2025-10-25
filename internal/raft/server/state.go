package server

import (
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
