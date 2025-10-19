package pubsub

import (
	"log"
	"sync"
	"sync/atomic"
)

// EventType is the type of event subscribers are listening for. This is a base type.
type EventType int

// SubscriptionOptions configures the behavior of a subscription.
type SubscriptionOptions struct {
	// If true, the broker will block to deliver an event to this subscriber's channel if it's full or unbuffered.
	// This guarantees delivery but can impact the performance of the entire event bus.
	// This should generally be set to false.
	IsBlocking bool
}

// SubscriberID is a unique identifier for a single subscription instance.
// It is returned upon subscribing and is required to unsubscribe.
type SubscriberID uint64

// nextSubscriberID is used to provide a unique ID for each subscriber.
var nextSubscriberID uint64

// Event is a generic event with compile-time type safety for payloads.
// Each instantiation creates a distinct concrete type (e.g., Event[string] != Event[int]).
type Event[T any] struct {
	Type    EventType
	Payload T
}

func NewEvent[T any](eventType EventType, payload T) *Event[T] {
	return &Event[T]{
		Type:    eventType,
		Payload: payload,
	}
}

// subscriber holds the channel and configuration for a single subscriber (internal, type-erased).
//
// TYPE ERASURE PATTERN:
// The challenge: We need to store channels of different generic types (chan *Event[VoteGrantedPayload],
// chan *Event[struct{}], chan *Event[time.Time], etc.) in a single homogeneous map registry.
// Go doesn't allow heterogeneous types in the same map (each Event[T] instantiation is a distinct type).
//
// The solution: Instead of storing the typed channels directly, we store FUNCTIONS that know how to
// operate on those channels. All functions have the same signature (homogeneous), but each captures
// a different typed channel via closures. This is type erasure - we erase the specific type at the
// storage level but preserve it in the closure's captured environment.
//
// Benefits:
// - Single registry map for all subscriber types (efficient)
// - Type safety at the API level (callers get Event[T])
// - Type assertion happens once (at Subscribe time, not at every receive)
type subscriber struct {
	// sendFunc is a closure that captures a specific typed channel (chan *Event[T]).
	// It performs type assertion from 'any' to T, constructs Event[T], and sends to the captured channel.
	// Returns true if sent successfully, false if channel is full (non-blocking mode) or type mismatch.
	sendFunc func(eventType EventType, payload any) bool

	// closeFunc is a closure that captures the same typed channel and closes it when unsubscribing.
	closeFunc func()

	Options    SubscriptionOptions
	NumDropped uint64 // For monitoring, atomically updated.
}

// PubSubClient implements the publish-subscribe pattern and is designed to be thread-safe. It could be used for various
// event based flows across the project.
type PubSubClient struct {
	mu sync.RWMutex
	// Used to wait for the run() goroutine to finish
	wg sync.WaitGroup

	// registry maps an EventType to a list of subscribers listening for it
	registry map[EventType]map[SubscriberID]*subscriber

	// publishChan is where all events of a given type are published, along with any payload for that event.
	//
	// It uses a buffered channel to decouple the sender (Publish) from the receiver (run).
	// Without a buffer, a call to Publish() would block until the run() goroutine is ready to read the event from the
	// chan. This creates a bottleneck, as the run() loop is often busy broadcasting a previous event.
	// The buffer solves this by acting as a queue, allowing Publish() to return immediately.
	//
	// It also allows in-flight events to be drained during a GracefulShutdown.
	publishChan chan struct {
		eventType EventType
		payload   any
	}

	// shuttingDown is used to atomically track if a shutdown process has begun.
	shuttingDown atomic.Bool
}

// Subscribe registers a subscriber to listen for a specific EventType with compile-time type safety.
// Following the IoC principle, the caller is responsible for creating and providing the channel.
// This gives the caller full control over the channel's buffer size, tailoring it to their specific needs.
// Returns a SubscriberID for unsubscribing.
//
// TYPE ERASURE IMPLEMENTATION:
// This function creates closures (sendFunc, closeFunc) that capture the typed channel 'ch'.
// These closures are stored in the type-erased 'subscriber' struct, allowing us to store
// channels of different types (Event[VoteGrantedPayload], Event[struct{}], etc.) in a single
// homogeneous registry map. The type parameter T is "erased" at the storage level but preserved
// in the closure's environment.
//
// Why a free function?
// Go does not support methods that declare their own type parameters,
// so `func (c *PubSubClient) Publish[T any](...)` is invalid.
// Because PubSubClient itself isn’t generic, Publish must be a generic
// top-level function that takes the client as its first parameter.
// This mirrors patterns in the standard library (e.g., slices.Sort[T](s)).
func Subscribe[T any](p *PubSubClient, eventType EventType, ch chan *Event[T], opts SubscriptionOptions) SubscriberID {
	// Only one goroutine at a time can register for a given EventType
	// https://go.dev/blog/maps#concurrency
	p.mu.Lock()
	defer p.mu.Unlock()

	id := SubscriberID(atomic.AddUint64(&nextSubscriberID, 1))

	// Create type-erased functions for the internal registry
	sub := &subscriber{
		Options: opts,
		// TYPE ERASURE: This closure captures the typed channel 'ch' (chan *Event[T]).
		// The closure signature is homogeneous (func(EventType, any) bool) allowing storage
		// in a single map, but each instance captures a different typed channel.
		sendFunc: func(evType EventType, payload any) bool {
			// TYPE ASSERTION: Convert from type-erased 'any' back to the concrete type T.
			// This happens once here, not in the caller's code - that's the key benefit!
			typedPayload, ok := payload.(T)
			if !ok {
				log.Printf("[PubSubClient] Warning: Type mismatch for event %v. Expected %T, got %T",
					evType, *new(T), payload)
				return false
			}

			// Construct a strongly-typed Event[T] to send on the captured typed channel
			event := &Event[T]{
				Type:    evType,
				Payload: typedPayload,
			}

			// Check the subscriber's policy on how to handle a full channel.
			if opts.IsBlocking {
				// Perform a blocking send. This guarantees delivery but will stall the broker if the subscriber is
				// slow to read from its channel.
				ch <- event
				return true
			} else {
				select {
				// Perform a non-blocking send.
				case ch <- event:
					return true
				// For non-blocking subscribers, we will drop the message if their channel is full.
				// This protects the PubSub system from being stalled by a single slow subscriber.
				default:
					return false
				}
			}
		},
		// TYPE ERASURE: This closure also captures 'ch' and provides a uniform way to close it
		closeFunc: func() {
			close(ch)
		},
	}

	// Initialize the inner map if this is the first registration for this EventType
	if _, ok := p.registry[eventType]; !ok {
		p.registry[eventType] = make(map[SubscriberID]*subscriber)
	}

	// Register the subscriber
	p.registry[eventType][id] = sub
	return id
}

// Unsubscribe removes a subscriber for a given event type.
func (p *PubSubClient) Unsubscribe(eventType EventType, id SubscriberID) {
	// Only one goroutine at a time can unsubscribe for a given EventType
	// https://go.dev/blog/maps#concurrency
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if there are any subscribers for this event type
	if subscribers, ok := p.registry[eventType]; ok {
		// Check if the specific subscriber ID exists
		if sub, ok := subscribers[id]; ok {
			// Remove the subscriber from the map
			delete(subscribers, id)

			// Close the channel to signal the subscriber goroutine to stop listening
			sub.closeFunc()

			// If no subscribers are left for this event type, delete the eventType
			if len(subscribers) == 0 {
				delete(p.registry, eventType)
			}
			log.Printf("[PubSubClient] Unsubscribed subscriber %d from event type %v.", id, eventType)
		}
	}
}

// Publish broadcasts an event via the PubSubClient.
//
// Why a free function?
// Go does not support methods that declare their own type parameters,
// so `func (c *PubSubClient) Publish[T any](...)` is invalid.
// Because PubSubClient itself isn’t generic, Publish must be a generic
// top-level function that takes the client as its first parameter.
// This mirrors patterns in the standard library (e.g., slices.Sort[T](s)).
func Publish[T any](p *PubSubClient, event *Event[T]) {
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

	// If PubSubClient is shutting down, drop the message gracefully.
	if p.shuttingDown.Load() {
		log.Printf("[PubSubClient] Warning: Dropping published event %v because PubSubClient is shutting down.", event.Type)
		return
	}

	// This send is now safe. The channel cannot be closed while we hold the RLock.
	p.publishChan <- struct {
		eventType EventType
		payload   any
	}{
		eventType: event.Type,
		payload:   event.Payload,
	}
}

// ForceShutdown immediately stops accepting new publishes and closes the channel.
// It is non-blocking (it returns immediately).
func (p *PubSubClient) ForceShutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if already shutting down to make the call idempotent.
	if p.shuttingDown.Load() {
		return
	}

	p.shuttingDown.Store(true)
	log.Println("[PubSubClient] ForceStop: Closing publish channel immediately.")

	// This triggers the run() loop to drain the buffer and terminate.
	close(p.publishChan)

	// We do NOT wait here because it's a "Force" stop.
}

// GracefulShutdown ensures all buffered messages are processed and waits for the
// run() goroutine to exit cleanly. It is blocking.
func (p *PubSubClient) GracefulShutdown() {
	p.mu.Lock()
	// Check if already shutting down to make the call idempotent.
	if p.shuttingDown.Load() {
		p.mu.Unlock() // Unlock before waiting
		p.wg.Wait()   // Wait for it to finish if another goroutine initiated shutdown
		return
	}

	// 1. Set the flag to true to immediately reject new publishes.
	p.shuttingDown.Store(true)
	log.Println("[PubSubClient] GracefulStop: Rejected new publishes. Initiating buffer drain.")

	// 2. Close the channel. This signals the p.run() loop to start draining the buffer.
	close(p.publishChan)
	p.mu.Unlock() // IMPORTANT: Unlock before waiting to avoid deadlock!

	// 3. SYNCHRONIZATION: Wait for the run() goroutine to finish processing the buffer and exit.
	p.wg.Wait()

	log.Println("[PubSubClient] GracefulStop: Broker fully drained and terminated.")
}

// run is the central goroutine that performs the broadcasting logic.
func (p *PubSubClient) run() {
	// Signal the WaitGroup when this function exits.
	defer p.wg.Done()

	for msg := range p.publishChan {
		// Acquire read lock to safely read the registry while broadcasting
		p.mu.RLock()

		// Get the set of subscribers for this event type
		if subscribers, ok := p.registry[msg.eventType]; ok {
			log.Printf("[PubSubClient]: Broadcasting %v event to %d listeners.\n", msg.eventType, len(subscribers))

			// FAN-OUT: Iterate over all type-erased subscribers.
			// Each subscriber's sendFunc closure knows how to convert the type-erased payload
			for id, sub := range subscribers {
				sent := sub.sendFunc(msg.eventType, msg.payload)
				if !sent && !sub.Options.IsBlocking {
					atomic.AddUint64(&sub.NumDropped, 1)
					log.Printf("[PubSubClient] Dropped event %v for subscriber %d (channel blocked). Total dropped: %d\n",
						msg.eventType, id, atomic.LoadUint64(&sub.NumDropped))
				}
			}
		}

		p.mu.RUnlock()
	}
}

func NewPubSub() *PubSubClient {
	p := &PubSubClient{
		registry: make(map[EventType]map[SubscriberID]*subscriber),
		publishChan: make(chan struct {
			eventType EventType
			payload   any
		}, 100),
	}
	p.shuttingDown.Store(false)

	// Add the run() goroutine to the WaitGroup
	p.wg.Add(1)

	go p.run()

	return p
}
