package internal

import (
	"log"
	"sync"
	"sync/atomic"
)

// EventType is the type of event subscribers are listening for
type EventType int

// SubscriptionOptions configures the behavior of a subscription.
type SubscriptionOptions struct {
	// If true, the broker will block to deliver an event to this subscriber's channel if it's full or unbuffered.
	// This guarantees delivery but can impact the performance of the entire event bus.
	// This should generally be set to false.
	IsBlocking bool
}

// SubscriberChan defines the channel through which subscribers receive Event.Payload
type SubscriberChan chan any

// SubscriberID is a unique identifier for a single subscription instance.
// It is returned upon subscribing and is required to unsubscribe.
type SubscriberID uint64

// Subscriber holds the channel and configuration for a single subscriber.
type Subscriber struct {
	Chan       SubscriberChan
	Options    SubscriptionOptions
	NumDropped uint64 // For monitoring, atomically updated.
}

// nextSubscriberID is used to provide a unique ID for each subscriber.
var nextSubscriberID uint64

// Event is the message structure that is published via PubSub.
type Event struct {
	Type    EventType
	Payload any
}

// PubSub implements the publish-subscribe pattern and is designed to be thread-safe. It could be used for various
// event based flows across the project.
type PubSub struct {
	mu sync.RWMutex
	// Used to wait for the run() goroutine to finish
	wg sync.WaitGroup

	// registry maps an EventType to a list of Subscribers listing for it
	registry map[EventType]map[SubscriberID]*Subscriber

	// publishChan is where all events of a given type are published, along with any payload for that event.
	//
	// It uses a buffered channel to decouple the sender (Publish) from the receiver (run).
	// Without a buffer, a call to Publish() would block until the run() goroutine is ready to read the event from the
	// chan. This creates a bottleneck, as the run() loop is often busy broadcasting a previous event.
	// The buffer solves this by acting as a queue, allowing Publish() to return immediately.
	//
	// It also allows in-flight events to be drained during a GracefulShutdown.
	publishChan chan Event

	// shuttingDown is used to atomically track if a shutdown process has begun.
	shuttingDown atomic.Bool
}

// Subscribe registers a subscriber to listen for a specific EventType.
// Following the IoC principle, the caller is responsible for creating and providing the channel.
// This gives the caller full control over the channel's buffer size, tailoring it to their specific needs.
// The returned SubscriberID is used to unsubscribe.
func (p *PubSub) Subscribe(eventType EventType, channel SubscriberChan, opts SubscriptionOptions) SubscriberID {
	// Only one goroutine at a time can register for a given EventType
	// https://go.dev/blog/maps#concurrency
	p.mu.Lock()
	defer p.mu.Unlock()

	id := SubscriberID(atomic.AddUint64(&nextSubscriberID, 1))
	sub := &Subscriber{
		Chan:    channel, // Use the channel provided by the caller
		Options: opts,
	}

	// Initialize the inner map if this is the first registration for this EventType
	if _, ok := p.registry[eventType]; !ok {
		p.registry[eventType] = make(map[SubscriberID]*Subscriber)
	}

	// Register the subscriber
	p.registry[eventType][id] = sub
	return id
}

// Unsubscribe removes a subscriber for a given event type.
func (p *PubSub) Unsubscribe(eventType EventType, id SubscriberID) {
	// Only one goroutine at a time can unsubscribe for a given EventType
	// https://go.dev/blog/maps#concurrency
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if there are any subscribers for this event type
	if subscribers, ok := p.registry[eventType]; ok {
		// Check if the specific subscriber ID exists
		if subscriber, ok := subscribers[id]; ok {
			// Remove the subscriber from the map
			delete(subscribers, id)

			// Close the channel to signal the subscriber goroutine to stop listening
			close(subscriber.Chan)

			// If no subscribers are left for this event type, delete the eventType
			if len(subscribers) == 0 {
				delete(p.registry, eventType)
			}
			log.Printf("[PubSub] Unsubscribed subscriber %d from event type %v.", id, eventType)
		}
	}
}

// Publish sends an event to the PubSub system for broadcast.
func (p *PubSub) Publish(event Event) {
	// The RLock here prevents a critical race condition.
	// THE RACE: Without a lock, a shutdown could occur between the check and the send:
	// 1. Goroutine A calls Publish() and sees `p.shuttingDown` is false.
	// 2. The scheduler pauses Goroutine A.
	// 3. Goroutine B calls GracefulShutdown(), acquires its own lock, and closes `p.publishChan`.
	// 4. Goroutine A resumes and attempts `p.publishChan <- event`, causing a panic because the channel is now closed.
	//
	// THE FIX: The RWMutex guarantees that a goroutine holding this RLock cannot send on a closed channel. Here's why:
	// For the channel to be closed, a shutdown method must acquire a full write Lock(). A write Lock() cannot be
	// acquired while ANY RLock() is being held. Therefore, as long as this goroutine holds the RLock, we are
	// guaranteed that the shutdown process is blocked and cannot close the channel under our feet
	// See: https://en.wikipedia.org/wiki/Time-of-check_to_time-of-use
	p.mu.RLock()
	defer p.mu.RUnlock()

	// If PubSub is shutting down, drop the message gracefully.
	if p.shuttingDown.Load() {
		log.Printf("[PubSub] Warning: Dropping published event %v because PubSub is shutting down.", event.Type)
		return
	}

	// This send is now safe. The channel cannot be closed while we hold the RLock.
	p.publishChan <- event
}

// ForceShutdown immediately stops accepting new publishes and closes the channel.
// It is non-blocking (it returns immediately).
func (p *PubSub) ForceShutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if already shutting down to make the call idempotent.
	if p.shuttingDown.Load() {
		return
	}

	p.shuttingDown.Store(true)
	log.Println("[PubSub] ForceStop: Closing publish channel immediately.")

	// This triggers the run() loop to drain the buffer and terminate.
	close(p.publishChan)

	// We do NOT wait here because it's a "Force" stop.
}

// GracefulShutdown ensures all buffered messages are processed and waits for the
// run() goroutine to exit cleanly. It is blocking.
func (p *PubSub) GracefulShutdown() {
	p.mu.Lock()
	// Check if already shutting down to make the call idempotent.
	if p.shuttingDown.Load() {
		p.mu.Unlock() // Unlock before waiting
		p.wg.Wait()   // Wait for it to finish if another goroutine initiated shutdown
		return
	}

	// 1. Set the flag to true to immediately reject new publishes.
	p.shuttingDown.Store(true)
	log.Println("[PubSub] GracefulStop: Rejected new publishes. Initiating buffer drain.")

	// 2. Close the channel. This signals the p.run() loop to start draining the buffer.
	close(p.publishChan)
	p.mu.Unlock() // IMPORTANT: Unlock before waiting to avoid deadlock!

	// 3. SYNCHRONIZATION: Wait for the run() goroutine to finish processing the buffer and exit.
	p.wg.Wait()

	log.Println("[PubSub] GracefulStop: Broker fully drained and terminated.")
}

// run is the central goroutine that performs the broadcasting logic.
func (p *PubSub) run() {
	// Signal the WaitGroup when this function exits.
	defer p.wg.Done()

	for event := range p.publishChan {
		// Acquire read lock to safely read the registry while broadcasting
		p.mu.RLock()

		// Get the set of subscribers for this event type
		if subscribers, ok := p.registry[event.Type]; ok {
			log.Printf("[PubSub]: Broadcasting %v event to %d listeners.\n", event.Type, len(subscribers))

			// FAN-OUT: Iterate over all registered channels for this specific type.
			for id, sub := range subscribers {
				// Check the subscriber's policy on how to handle a full channel.
				if sub.Options.IsBlocking {
					// Perform a blocking send. This guarantees delivery but can stall the broker
					// if the subscriber is slow to read from its channel.
					sub.Chan <- event.Payload
				} else {
					// Perform a non-blocking send.
					select {
					case sub.Chan <- event.Payload:
						// Success
					default:
						// For non-blocking subscribers, drop the message if their channel is full.
						// This protects the PubSub system from being stalled by a single slow subscriber.
						atomic.AddUint64(&sub.NumDropped, 1)
						log.Printf("[PubSub] Dropped event %v for subscriber %d (channel blocked). Total dropped: %d\n",
							event.Type, id, atomic.LoadUint64(&sub.NumDropped))
					}
				}
			}
		}

		p.mu.RUnlock()
	}
}

func NewPubSub() *PubSub {
	p := &PubSub{
		registry:    make(map[EventType]map[SubscriberID]*Subscriber),
		publishChan: make(chan Event, 100),
	}
	p.shuttingDown.Store(false)

	// Add the run() goroutine to the WaitGroup
	p.wg.Add(1)

	go p.run()

	return p
}
