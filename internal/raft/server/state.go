package server

import (
	"sync"
	"time"
)

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
	// grantedVotesTotal represents the total number of votes received during an Election, when the server is Candidate
	// It should be reset at the beginning of each new Election.
	grantedVotesTotal uint64
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
func (s *serverState) getGrantedVotesTotal() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.grantedVotesTotal
}

func (s *serverState) setGrantedVotesTotal(votes uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grantedVotesTotal = votes
}

func (s *serverState) incrementGrantedVotesTotal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grantedVotesTotal++
}
