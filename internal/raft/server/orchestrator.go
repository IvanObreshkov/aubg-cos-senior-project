package server

import (
	"aubg-cos-senior-project/internal"
)

// Orchestrator orchestrates and monitors the behavior of a Server, handling elections, state transitions, and
// coordination between different server components.
type Orchestrator struct {
	// A channel where a signal is sent once the ElectionTimeout of a server expires. This channel is buffered.
	electionTimeoutExpiredChan chan *internal.Event
	// A channel where a shutdown signal is received. It could be used to notify all background threads that they need
	// to finish work. It also signals that the Orchestrator running in a goroutine should exit. This channel is
	// buffered.
	shutDownChan chan *internal.Event
	// A channel where a signal is sent when a new vote is received, assuming the server is in Candidate state
	voteReceivedChan chan *internal.Event

	pubSub *internal.PubSub
	// The server that is orchestrated.
	server *Server
}

// Run Runs the Orchestrator for a given Server. It should be executed as a goroutine.
func (s *Orchestrator) Run() {
	for {
		select {
		case <-s.electionTimeoutExpiredChan:
			// Only start an election if we are a Follower or Candidate.
			// This prevents a Leader from mistakenly starting an election.
			if s.server.getState() == Follower || s.server.getState() == Candidate {
				s.server.BeginElection()
			}
		case <-s.shutDownChan:
			return
		}

	}
}

func NewOrchestrator(pubSub *internal.PubSub, server *Server) *Orchestrator {
	manager := &Orchestrator{
		electionTimeoutExpiredChan: make(chan *internal.Event, 1),
		shutDownChan:               make(chan *internal.Event, 1),
		voteReceivedChan:           make(chan *internal.Event, len(server.peers)+1),
		pubSub:                     pubSub,
		server:                     server,
	}

	pubSub.Subscribe(ServerShutDown, manager.shutDownChan, internal.SubscriptionOptions{IsBlocking: false})
	pubSub.Subscribe(ElectionTimeoutExpired, manager.electionTimeoutExpiredChan, internal.SubscriptionOptions{IsBlocking: false})
	pubSub.Subscribe(VoteReceived, manager.voteReceivedChan, internal.SubscriptionOptions{IsBlocking: false})

	return manager
}
