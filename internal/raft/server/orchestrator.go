package server

import (
	"aubg-cos-senior-project/internal/pubsub"
	"log"
	"time"
)

// Orchestrator orchestrates and monitors the behavior of a Server, handling elections, state transitions, and
// coordination between different server components.
type Orchestrator struct {
	electionTimeoutExpiredChan <-chan *pubsub.Event[time.Time]
	shutDownChan               <-chan *pubsub.Event[struct{}]
	voteReceivedChan           <-chan *pubsub.Event[VoteGrantedPayload]
	electionWonChan            <-chan *pubsub.Event[uint64]

	pubSub *pubsub.PubSubClient
	server *Server
}

// Run Runs the Orchestrator for a given Server. It should be executed as a goroutine.
func (s *Orchestrator) Run() {
	for {
		select {
		case <-s.electionTimeoutExpiredChan:
			// Log that election timeout expired - the server state is checked in BeginElection
			log.Printf("[ORCHESTRATOR] Election timeout expired, triggering election")
			s.server.BeginElection()

		case ev := <-s.voteReceivedChan:
			s.server.OnVoteGranted(ev.Payload.From, ev.Payload.Term)

		case ev := <-s.electionWonChan:
			s.server.OnElectionWon(ev.Payload)

		case <-s.shutDownChan:
			log.Printf("[ORCHESTRATOR] Shutdown signal received, stopping orchestrator")
			return
		}
	}
}

func NewOrchestrator(pubSub *pubsub.PubSubClient, server *Server) *Orchestrator {
	orchestrator := &Orchestrator{
		pubSub: pubSub,
		server: server,
	}

	// Create channels and subscribe to events
	shutDownChan := make(chan *pubsub.Event[struct{}], 1)
	electionTimeoutExpiredChan := make(chan *pubsub.Event[time.Time], 1)
	voteReceivedChan := make(chan *pubsub.Event[VoteGrantedPayload], len(server.peers)+1)
	electionWonChan := make(chan *pubsub.Event[uint64], 1)

	orchestrator.shutDownChan = shutDownChan
	orchestrator.electionTimeoutExpiredChan = electionTimeoutExpiredChan
	orchestrator.voteReceivedChan = voteReceivedChan
	orchestrator.electionWonChan = electionWonChan

	pubsub.Subscribe(pubSub, ServerShutDown, shutDownChan, pubsub.SubscriptionOptions{IsBlocking: false})
	pubsub.Subscribe(pubSub, ElectionTimeoutExpired, electionTimeoutExpiredChan, pubsub.SubscriptionOptions{IsBlocking: false})
	pubsub.Subscribe(pubSub, VoteGranted, voteReceivedChan, pubsub.SubscriptionOptions{IsBlocking: false})
	pubsub.Subscribe(pubSub, ElectionWon, electionWonChan, pubsub.SubscriptionOptions{IsBlocking: false})

	return orchestrator
}
