package tob

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrNotStarted       = errors.New("TOBroadcast not started")
	ErrSequencerStopped = errors.New("sequencer stopped")
	ErrQueueFull        = errors.New("message queue full")
	ErrNoSequencer      = errors.New("no sequencer available")
	ErrInvalidConfig    = errors.New("invalid configuration")
)

// TOBroadcast implements the Fixed Sequencer Total Order Broadcast protocol
// Paper: "In the Fixed Sequencer algorithm, a centralized process (the sequencer)
// is designated. When a process wants to broadcast a message, it sends the message
// to the sequencer. The sequencer assigns a sequence number to the message and
// multicasts it to all processes. Processes deliver messages in sequence number order."
type TOBroadcast struct {
	config    *Config
	transport Transport
	sequencer *Sequencer
	delivery  *DeliveryManager
	stats     *Statistics

	// State management
	isSequencer      bool
	currentSequencer string
	sequencerAddr    string
	mu               sync.RWMutex

	// Sequencer failure detection
	lastHeartbeat time.Time
	heartbeatMu   sync.RWMutex
	monitorStopCh chan struct{}

	// Callbacks
	deliveryCallback        DeliveryCallback
	sequencerChangeCallback SequencerChangeCallback

	// Lifecycle management
	started bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// New creates a new Total Order Broadcast instance
func New(config *Config) (*TOBroadcast, error) {
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	tob := &TOBroadcast{
		config:           config,
		isSequencer:      config.IsSequencer,
		currentSequencer: config.SequencerID,
		sequencerAddr:    config.SequencerAddr,
		stats:            &Statistics{},
		stopCh:           make(chan struct{}),
		monitorStopCh:    make(chan struct{}),
	}

	// Create transport
	tob.transport = NewUDPTransport(config.BindAddr, config.Logger)

	// Create delivery manager
	tob.delivery = NewDeliveryManager(tob)

	// Create sequencer if this node is the sequencer
	if config.IsSequencer {
		tob.sequencer = NewSequencer(tob)
	}

	// Set message handler
	tob.transport.SetMessageHandler(tob.handleMessage)

	return tob, nil
}

// validateConfig validates the configuration
func validateConfig(config *Config) error {
	if config.NodeID == "" {
		return fmt.Errorf("%w: NodeID is required", ErrInvalidConfig)
	}
	if config.BindAddr == "" {
		return fmt.Errorf("%w: BindAddr is required", ErrInvalidConfig)
	}
	if config.AdvertiseAddr == "" {
		return fmt.Errorf("%w: AdvertiseAddr is required", ErrInvalidConfig)
	}
	if !config.IsSequencer && config.SequencerAddr == "" {
		return fmt.Errorf("%w: SequencerAddr is required for non-sequencer nodes", ErrInvalidConfig)
	}
	if len(config.Nodes) == 0 {
		return fmt.Errorf("%w: Nodes list is required", ErrInvalidConfig)
	}
	return nil
}

// Start starts the Total Order Broadcast protocol
func (tob *TOBroadcast) Start() error {
	tob.mu.Lock()
	defer tob.mu.Unlock()

	if tob.started {
		return nil
	}

	tob.config.Logger.Infof("[TOB] Starting Total Order Broadcast on node %s", tob.config.NodeID)

	// Start transport
	if err := tob.transport.Start(); err != nil {
		return fmt.Errorf("failed to start transport: %w", err)
	}

	// Start delivery manager
	tob.delivery.Start()

	// Start sequencer if this node is the sequencer
	if tob.isSequencer {
		tob.sequencer.Start()
		tob.config.Logger.Infof("[TOB] This node is the SEQUENCER")
	} else {
		tob.config.Logger.Infof("[TOB] Sequencer is at %s", tob.sequencerAddr)
		// Start monitoring sequencer health
		tob.wg.Add(1)
		go tob.monitorSequencer()
	}

	tob.started = true
	tob.config.Logger.Infof("[TOB] Total Order Broadcast started successfully")

	return nil
}

// Stop stops the Total Order Broadcast protocol
func (tob *TOBroadcast) Stop() error {
	tob.mu.Lock()
	if !tob.started {
		tob.mu.Unlock()
		return nil
	}
	tob.started = false
	tob.mu.Unlock()

	tob.config.Logger.Infof("[TOB] Stopping Total Order Broadcast")

	// Stop monitoring
	close(tob.monitorStopCh)

	// Stop sequencer
	if tob.sequencer != nil {
		tob.sequencer.Stop()
	}

	// Stop delivery
	tob.delivery.Stop()

	// Stop transport
	if err := tob.transport.Stop(); err != nil {
		tob.config.Logger.Errorf("[TOB] Error stopping transport: %v", err)
	}

	close(tob.stopCh)
	tob.wg.Wait()

	tob.config.Logger.Infof("[TOB] Total Order Broadcast stopped")
	return nil
}

// Broadcast sends a message using Total Order Broadcast
// Paper: "When a process wants to broadcast a message m, it sends m to the sequencer"
func (tob *TOBroadcast) Broadcast(payload []byte) error {
	if !tob.started {
		return ErrNotStarted
	}

	// Create message
	msg := &Message{
		Type:      DataMsg,
		From:      tob.config.NodeID,
		FromAddr:  tob.config.AdvertiseAddr,
		MessageID: uuid.New().String(),
		Payload:   payload,
		Timestamp: time.Now(),
	}

	tob.config.Logger.Debugf("[TOB] Broadcasting message %s (size: %d bytes)", msg.MessageID, len(payload))
	tob.stats.IncrementMessagesSent()

	// Send to sequencer
	tob.mu.RLock()
	sequencerAddr := tob.sequencerAddr
	isSeq := tob.isSequencer
	tob.mu.RUnlock()

	if isSeq {
		// We are the sequencer, submit directly
		return tob.sequencer.SubmitMessage(msg)
	} else {
		// Send to remote sequencer
		if sequencerAddr == "" {
			return ErrNoSequencer
		}
		return tob.transport.SendMessage(sequencerAddr, msg)
	}
}

// handleMessage handles incoming protocol messages
func (tob *TOBroadcast) handleMessage(msg *Message) {
	switch msg.Type {
	case DataMsg:
		tob.handleDataMessage(msg)
	case SequencedMsg:
		tob.handleSequencedMessage(msg)
	case HeartbeatMsg:
		tob.handleHeartbeat(msg)
	case SequencerElectionMsg:
		tob.handleSequencerElection(msg)
	case SequencerAnnouncementMsg:
		tob.handleSequencerAnnouncement(msg)
	default:
		tob.config.Logger.Warnf("[TOB] Unknown message type: %v", msg.Type)
	}
}

// handleDataMessage handles a data message sent to the sequencer
func (tob *TOBroadcast) handleDataMessage(msg *Message) {
	if !tob.isSequencer {
		tob.config.Logger.Warnf("[TOB] Received data message but not sequencer, ignoring")
		return
	}

	tob.config.Logger.Debugf("[TOB] Sequencer received data message %s from %s", msg.MessageID, msg.From)

	// Submit to sequencer for processing
	if err := tob.sequencer.SubmitMessage(msg); err != nil {
		tob.config.Logger.Errorf("[TOB] Failed to submit message to sequencer: %v", err)
	}
}

// handleSequencedMessage handles a sequenced message from the sequencer
// Paper: "When a process receives a message with sequence number s,
// it holds the message until it has delivered all messages with sequence numbers less than s"
func (tob *TOBroadcast) handleSequencedMessage(msg *Message) {
	tob.config.Logger.Debugf("[TOB] Received sequenced message %s with seq=%d from %s",
		msg.MessageID, msg.SequenceNumber, msg.From)

	// Add to delivery manager
	tob.delivery.AddSequencedMessage(msg)
}

// multicastSequencedMessage multicasts a sequenced message to all nodes
// Paper: "The sequencer multicasts the message along with its sequence number to all processes"
func (tob *TOBroadcast) multicastSequencedMessage(msg *Message) {
	tob.config.Logger.Debugf("[TOB] Multicasting sequenced message %s (seq=%d) to all nodes",
		msg.MessageID, msg.SequenceNumber)

	// Send to all nodes (including self)
	for _, nodeAddr := range tob.config.Nodes {
		if err := tob.transport.SendMessage(nodeAddr, msg); err != nil {
			tob.config.Logger.Errorf("[TOB] Failed to multicast to %s: %v", nodeAddr, err)
		}
	}
}

// handleHeartbeat handles a heartbeat from the sequencer
func (tob *TOBroadcast) handleHeartbeat(msg *Message) {
	tob.heartbeatMu.Lock()
	tob.lastHeartbeat = time.Now()
	tob.heartbeatMu.Unlock()

	tob.config.Logger.Debugf("[TOB] Received heartbeat from sequencer %s", msg.From)
}

// monitorSequencer monitors the sequencer for failures
// Paper: "The sequencer must be monitored; if it fails, a new sequencer must be elected"
func (tob *TOBroadcast) monitorSequencer() {
	defer tob.wg.Done()

	ticker := time.NewTicker(tob.config.HeartbeatInterval)
	defer ticker.Stop()

	// Initialize last heartbeat
	tob.heartbeatMu.Lock()
	tob.lastHeartbeat = time.Now()
	tob.heartbeatMu.Unlock()

	for {
		select {
		case <-ticker.C:
			tob.checkSequencerHealth()
		case <-tob.monitorStopCh:
			return
		}
	}
}

// checkSequencerHealth checks if the sequencer is still alive
func (tob *TOBroadcast) checkSequencerHealth() {
	tob.heartbeatMu.RLock()
	lastHB := tob.lastHeartbeat
	tob.heartbeatMu.RUnlock()

	timeSinceHeartbeat := time.Since(lastHB)

	if timeSinceHeartbeat > tob.config.SequencerTimeout {
		tob.config.Logger.Warnf("[TOB] Sequencer timeout detected (last heartbeat: %v ago)", timeSinceHeartbeat)

		if tob.config.EnableSequencerFailover {
			tob.handleSequencerFailure()
		}
	}
}

// handleSequencerFailure handles sequencer failure and initiates election
// Paper: "When the sequencer fails, a new sequencer must be elected"
func (tob *TOBroadcast) handleSequencerFailure() {
	tob.mu.Lock()
	defer tob.mu.Unlock()

	tob.config.Logger.Warnf("[TOB] Sequencer failure detected, initiating election")

	// Simple election: highest priority (or lowest ID) becomes sequencer
	// In production, could use a more sophisticated election algorithm
	if tob.shouldBecomeSequencer() {
		tob.becomeSequencer()
	}
}

// shouldBecomeSequencer determines if this node should become the sequencer
func (tob *TOBroadcast) shouldBecomeSequencer() bool {
	// Simple heuristic: node with highest priority
	// Could be enhanced with Bully algorithm or other election protocols
	return true // Simplified for this implementation
}

// becomeSequencer promotes this node to be the sequencer
func (tob *TOBroadcast) becomeSequencer() {
	tob.config.Logger.Infof("[TOB] Becoming the new sequencer")

	oldSequencer := tob.currentSequencer

	tob.isSequencer = true
	tob.currentSequencer = tob.config.NodeID
	tob.sequencerAddr = tob.config.AdvertiseAddr

	// Create and start sequencer
	if tob.sequencer == nil {
		tob.sequencer = NewSequencer(tob)
	}
	tob.sequencer.Start()

	// Announce to all nodes
	tob.announceNewSequencer()

	// Notify callback
	if tob.sequencerChangeCallback != nil {
		tob.sequencerChangeCallback(oldSequencer, tob.config.NodeID)
	}

	tob.stats.IncrementSequencerChanges()
}

// announceNewSequencer announces this node as the new sequencer
func (tob *TOBroadcast) announceNewSequencer() {
	msg := &Message{
		Type:              SequencerAnnouncementMsg,
		From:              tob.config.NodeID,
		FromAddr:          tob.config.AdvertiseAddr,
		ProposedSequencer: tob.config.NodeID,
		Timestamp:         time.Now(),
	}

	for _, nodeAddr := range tob.config.Nodes {
		if nodeAddr != tob.config.AdvertiseAddr {
			if err := tob.transport.SendMessage(nodeAddr, msg); err != nil {
				tob.config.Logger.Errorf("[TOB] Failed to announce new sequencer to %s: %v", nodeAddr, err)
			}
		}
	}
}

// handleSequencerElection handles a sequencer election message
func (tob *TOBroadcast) handleSequencerElection(msg *Message) {
	tob.config.Logger.Infof("[TOB] Received sequencer election message from %s", msg.From)
	// Election protocol implementation
}

// handleSequencerAnnouncement handles a sequencer announcement
func (tob *TOBroadcast) handleSequencerAnnouncement(msg *Message) {
	tob.config.Logger.Infof("[TOB] New sequencer announced: %s", msg.ProposedSequencer)

	tob.mu.Lock()
	defer tob.mu.Unlock()

	oldSequencer := tob.currentSequencer
	tob.currentSequencer = msg.ProposedSequencer
	tob.sequencerAddr = msg.FromAddr

	// Reset heartbeat timer
	tob.heartbeatMu.Lock()
	tob.lastHeartbeat = time.Now()
	tob.heartbeatMu.Unlock()

	// Notify callback
	if tob.sequencerChangeCallback != nil {
		tob.sequencerChangeCallback(oldSequencer, msg.ProposedSequencer)
	}

	tob.stats.IncrementSequencerChanges()
}

// SetDeliveryCallback sets the callback for message delivery
func (tob *TOBroadcast) SetDeliveryCallback(callback DeliveryCallback) {
	tob.mu.Lock()
	defer tob.mu.Unlock()
	tob.deliveryCallback = callback
}

// SetSequencerChangeCallback sets the callback for sequencer changes
func (tob *TOBroadcast) SetSequencerChangeCallback(callback SequencerChangeCallback) {
	tob.mu.Lock()
	defer tob.mu.Unlock()
	tob.sequencerChangeCallback = callback
}

// GetStatistics returns protocol statistics
func (tob *TOBroadcast) GetStatistics() *Statistics {
	return tob.stats
}

// IsSequencer returns whether this node is the current sequencer
func (tob *TOBroadcast) IsSequencer() bool {
	tob.mu.RLock()
	defer tob.mu.RUnlock()
	return tob.isSequencer
}

// GetCurrentSequencer returns the ID of the current sequencer
func (tob *TOBroadcast) GetCurrentSequencer() string {
	tob.mu.RLock()
	defer tob.mu.RUnlock()
	return tob.currentSequencer
}

// GetNextExpectedSequence returns the next expected sequence number
func (tob *TOBroadcast) GetNextExpectedSequence() uint64 {
	return tob.delivery.GetNextExpectedSeq()
}

// GetPendingMessageCount returns the number of pending messages
func (tob *TOBroadcast) GetPendingMessageCount() int {
	return tob.delivery.GetPendingCount()
}
