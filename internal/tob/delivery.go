package tob

import (
	"container/heap"
	"sync"
	"time"
)

// DeliveryManager manages ordered delivery of messages
// Paper: "Messages are delivered to the application in sequence number order"
type DeliveryManager struct {
	nextExpectedSeq uint64
	pendingMessages *MessageHeap
	deliveredMsgs   map[string]bool // Track delivered message IDs
	mu              sync.Mutex
	tob             *TOBroadcast
	stopCh          chan struct{}
	wg              sync.WaitGroup
}

// NewDeliveryManager creates a new delivery manager
func NewDeliveryManager(tob *TOBroadcast) *DeliveryManager {
	mh := &MessageHeap{}
	heap.Init(mh)

	return &DeliveryManager{
		nextExpectedSeq: 1,
		pendingMessages: mh,
		deliveredMsgs:   make(map[string]bool),
		tob:             tob,
		stopCh:          make(chan struct{}),
	}
}

// Start begins the delivery manager
func (dm *DeliveryManager) Start() {
	dm.tob.config.Logger.Infof("[Delivery] Starting delivery manager")

	dm.wg.Add(1)
	go dm.runDelivery()
}

// Stop stops the delivery manager
func (dm *DeliveryManager) Stop() {
	close(dm.stopCh)
	dm.wg.Wait()
}

// AddSequencedMessage adds a sequenced message to the delivery queue
// Paper: "When a process receives a message with sequence number s,
// it holds the message until it has delivered all messages with
// sequence numbers less than s"
func (dm *DeliveryManager) AddSequencedMessage(msg *Message) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Check if already delivered
	if dm.deliveredMsgs[msg.MessageID] {
		dm.tob.config.Logger.Debugf("[Delivery] Message %s already delivered, ignoring", msg.MessageID)
		return
	}

	// Add to pending messages heap
	heap.Push(dm.pendingMessages, msg)
	dm.tob.config.Logger.Debugf("[Delivery] Added message %s with seq=%d to pending queue (next expected: %d)",
		msg.MessageID, msg.SequenceNumber, dm.nextExpectedSeq)

	// Try to deliver messages
	dm.deliverPendingMessages()
}

// deliverPendingMessages delivers all messages in sequence order
// Paper: "Messages are delivered in increasing sequence number order"
func (dm *DeliveryManager) deliverPendingMessages() {
	for dm.pendingMessages.Len() > 0 {
		// Peek at the next message
		nextMsg := (*dm.pendingMessages)[0]

		// Check if this is the next expected message
		if nextMsg.SequenceNumber == dm.nextExpectedSeq {
			// Remove from heap
			msg := heap.Pop(dm.pendingMessages).(*Message)

			// Mark as delivered
			dm.deliveredMsgs[msg.MessageID] = true
			dm.nextExpectedSeq++

			// Calculate latency
			latency := time.Since(msg.Timestamp)
			dm.tob.stats.UpdateAverageLatency(latency)
			dm.tob.stats.IncrementMessagesDelivered()

			dm.tob.config.Logger.Infof("[Delivery] Delivering message %s (seq=%d, latency=%v)",
				msg.MessageID, msg.SequenceNumber, latency)

			// Deliver to application (unlock first to avoid deadlock)
			dm.mu.Unlock()
			if dm.tob.deliveryCallback != nil {
				dm.tob.deliveryCallback(msg)
			}
			dm.mu.Lock()
		} else if nextMsg.SequenceNumber > dm.nextExpectedSeq {
			// Gap in sequence - wait for missing messages
			dm.tob.config.Logger.Debugf("[Delivery] Gap detected: have seq=%d, expecting seq=%d",
				nextMsg.SequenceNumber, dm.nextExpectedSeq)
			break
		} else {
			// This message has a sequence number less than expected (duplicate or out of order)
			// This shouldn't happen with correct sequencer, but handle defensively
			dm.tob.config.Logger.Warnf("[Delivery] Received duplicate or out-of-order message seq=%d (expecting %d)",
				nextMsg.SequenceNumber, dm.nextExpectedSeq)
			heap.Pop(dm.pendingMessages)
		}
	}
}

// runDelivery periodically checks for delivery opportunities and cleanups
func (dm *DeliveryManager) runDelivery() {
	defer dm.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			dm.mu.Lock()
			dm.cleanupOldDeliveries()
			dm.mu.Unlock()
		case <-dm.stopCh:
			return
		}
	}
}

// cleanupOldDeliveries removes old delivered message IDs to prevent memory growth
func (dm *DeliveryManager) cleanupOldDeliveries() {
	// Keep only recent delivered messages (e.g., last 10000)
	if len(dm.deliveredMsgs) > 10000 {
		dm.tob.config.Logger.Debugf("[Delivery] Cleaning up old delivered messages")
		// In production, might want to use a time-based approach or LRU cache
		dm.deliveredMsgs = make(map[string]bool)
	}
}

// GetNextExpectedSeq returns the next expected sequence number
func (dm *DeliveryManager) GetNextExpectedSeq() uint64 {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.nextExpectedSeq
}

// GetPendingCount returns the number of pending messages
func (dm *DeliveryManager) GetPendingCount() int {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.pendingMessages.Len()
}

// MessageHeap implements a min-heap of messages by sequence number
// This ensures we can efficiently find the message with the smallest sequence number
type MessageHeap []*Message

func (h MessageHeap) Len() int           { return len(h) }
func (h MessageHeap) Less(i, j int) bool { return h[i].SequenceNumber < h[j].SequenceNumber }
func (h MessageHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MessageHeap) Push(x interface{}) {
	*h = append(*h, x.(*Message))
}

func (h *MessageHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
