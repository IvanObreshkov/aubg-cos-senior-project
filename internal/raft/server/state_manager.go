package server

import (
	"aubg-cos-senior-project/internal"
)

// StateManager StageManager manages and monitors the state of a Server. It acts as an orchestrator.
type StateManager struct {
	// A channel where a signal is sent once the ElectionTimeout of a server expires. This channel is buffered.
	electionTimeoutExpiredChan chan *internal.Event
	// A channel where a shutdown signal is received. It could be used to notify all background threads that they need
	// to finish work. This channel is buffered.
	shutDownChan chan *internal.Event
	pubSub       *internal.PubSub
}

func NewStateManager(pubSub *internal.PubSub) *StateManager {
	manager := &StateManager{
		electionTimeoutExpiredChan: make(chan *internal.Event, 1),
		shutDownChan:               make(chan *internal.Event, 1),
		pubSub:                     pubSub,
	}

	pubSub.Subscribe(ServerShutDown, manager.shutDownChan, internal.SubscriptionOptions{IsBlocking: false})
	pubSub.Subscribe(ElectionTimeoutExpired, manager.electionTimeoutExpiredChan, internal.SubscriptionOptions{IsBlocking: false})

	return manager
}
