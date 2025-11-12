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
// Paper (Section 4.1): "In the Fixed Sequencer algorithm, a centralized process (the sequencer)
// is designated. When a process wants to broadcast a message, it sends the message
// to the sequencer. The sequencer assigns a sequence number to the message and
// multicasts it to all processes. Processes deliver messages in sequence number order."
//
// Implementation choices:
// - Two-phase message flow (DataMsg → SequencedMsg) for clarity
// - Heartbeat-based failure detection
// - Fixed sequencer without automatic failover
// - Sequence numbers start from 1
type TOBroadcast struct {
	config    *Config
	transport Transport
	sequencer *Sequencer
	delivery  *DeliveryManager
	metrics   *Metrics // Performance metrics

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
	deliveryCallback DeliveryCallback

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
		metrics:          NewMetrics(),
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
// Paper (Section 4.1): "When a process wants to broadcast a message m, it sends m to the sequencer"
func (tob *TOBroadcast) Broadcast(payload []byte) error {
	if !tob.started {
		return ErrNotStarted
	}

	// Check if sequencer is reachable before accepting broadcast
	if !tob.IsSequencer() && !tob.IsSequencerReachable() {
		return fmt.Errorf("%w: sequencer is unreachable (ordering has stopped)", ErrNoSequencer)
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
	tob.metrics.RecordDataMsg()

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
			return fmt.Errorf("%w: sequencer address not configured", ErrNoSequencer)
		}
		err := tob.transport.SendMessage(sequencerAddr, msg)
		if err != nil {
			return fmt.Errorf("failed to send to sequencer: %w", err)
		}
		return nil
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
// Paper (Section 4.1): "When a process receives a message with sequence number s,
// it holds the message until it has delivered all messages with sequence numbers less than s"
func (tob *TOBroadcast) handleSequencedMessage(msg *Message) {
	tob.config.Logger.Debugf("[TOB] Received sequenced message %s with seq=%d from %s",
		msg.MessageID, msg.SequenceNumber, msg.From)

	// Add to delivery manager
	tob.delivery.AddSequencedMessage(msg)
}

// multicastSequencedMessage sends sequenced message to all processes
// Paper (Section 4.1): "The sequencer multicasts the message along with its sequence number to all processes"
// Implementation: Uses multiple unicast sends (standard practice since true IP multicast is rarely available)
// Optimization: Sequencer delivers to itself locally without network overhead
func (tob *TOBroadcast) multicastSequencedMessage(msg *Message) {
	tob.config.Logger.Debugf("[TOB] Sending sequenced message %s (seq=%d) to all processes",
		msg.MessageID, msg.SequenceNumber)

	// Deliver to self locally (optimization: bypass network)
	if tob.isSequencer {
		tob.delivery.AddSequencedMessage(msg)
	}

	// Send to all other nodes
	for _, nodeAddr := range tob.config.Nodes {
		if nodeAddr != tob.config.AdvertiseAddr {
			if err := tob.transport.SendMessage(nodeAddr, msg); err != nil {
				tob.config.Logger.Errorf("[TOB] Failed to send to %s: %v", nodeAddr, err)
			}
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
// The paper's Fixed Sequencer (Section 4.1) does not include failure handling; see Moving Sequencer (Section 4.2).
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
		// Sequencer is fixed for a run. If it fails, ordering stops.
		tob.config.Logger.Errorf("[TOB] Sequencer is unreachable (last heartbeat: %v ago). Ordering has stopped. Manual intervention required.", timeSinceHeartbeat)
	}
}

// SetDeliveryCallback sets the callback for message delivery
func (tob *TOBroadcast) SetDeliveryCallback(callback DeliveryCallback) {
	tob.mu.Lock()
	defer tob.mu.Unlock()
	tob.deliveryCallback = callback
}

// GetMetrics returns performance metrics
func (tob *TOBroadcast) GetMetrics() *Metrics {
	return tob.metrics
}

// SetMetrics sets a shared metrics collector (for benchmarking)
func (tob *TOBroadcast) SetMetrics(metrics *Metrics) {
	tob.mu.Lock()
	defer tob.mu.Unlock()
	tob.metrics = metrics
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

// IsSequencerReachable checks if the sequencer is currently reachable
// Returns false if sequencer timeout has been exceeded
func (tob *TOBroadcast) IsSequencerReachable() bool {
	if tob.IsSequencer() {
		return true // We are the sequencer
	}

	tob.heartbeatMu.RLock()
	lastHB := tob.lastHeartbeat
	tob.heartbeatMu.RUnlock()

	timeSinceHeartbeat := time.Since(lastHB)
	return timeSinceHeartbeat <= tob.config.SequencerTimeout
}

// GetNextExpectedSequence returns the next expected sequence number
func (tob *TOBroadcast) GetNextExpectedSequence() uint64 {
	return tob.delivery.GetNextExpectedSeq()
}

// GetPendingMessageCount returns the number of pending messages
func (tob *TOBroadcast) GetPendingMessageCount() int {
	return tob.delivery.GetPendingCount()
}
