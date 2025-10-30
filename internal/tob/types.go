package tob

import (
	"sync"
	"time"
)

// MessageType identifies the type of Total Order Broadcast message
type MessageType int

const (
	// DataMsg is a message containing application data to be broadcast
	DataMsg MessageType = iota
	// SequencedMsg is a message with an assigned sequence number from the sequencer
	SequencedMsg
	// AckMsg acknowledges receipt of a sequenced message
	AckMsg
	// HeartbeatMsg is sent by sequencer to detect failures
	HeartbeatMsg
	// SequencerElectionMsg is used during sequencer election
	SequencerElectionMsg
	// SequencerAnnouncementMsg announces the new sequencer
	SequencerAnnouncementMsg
)

func (m MessageType) String() string {
	switch m {
	case DataMsg:
		return "Data"
	case SequencedMsg:
		return "Sequenced"
	case AckMsg:
		return "Ack"
	case HeartbeatMsg:
		return "Heartbeat"
	case SequencerElectionMsg:
		return "SequencerElection"
	case SequencerAnnouncementMsg:
		return "SequencerAnnouncement"
	default:
		return "Unknown"
	}
}

// Message represents a Total Order Broadcast message
// Paper: "Fixed Sequencer algorithm: a centralized process (the sequencer)
// assigns a sequence number to each message"
type Message struct {
	Type           MessageType
	From           string    // Sender's ID
	FromAddr       string    // Sender's address
	SequenceNumber uint64    // Sequence number assigned by sequencer
	MessageID      string    // Unique message identifier
	Payload        []byte    // Application data
	Timestamp      time.Time // Message timestamp

	// For sequencer election
	ProposedSequencer string
	ProposedPriority  int
}

// DeliveryStatus represents the state of a message in the delivery pipeline
type DeliveryStatus int

const (
	// Pending means message is waiting for sequence number
	Pending DeliveryStatus = iota
	// Sequenced means message has been assigned a sequence number
	Sequenced
	// Delivered means message has been delivered to application
	Delivered
)

func (d DeliveryStatus) String() string {
	switch d {
	case Pending:
		return "Pending"
	case Sequenced:
		return "Sequenced"
	case Delivered:
		return "Delivered"
	default:
		return "Unknown"
	}
}

// PendingMessage tracks a message awaiting sequencing or delivery
type PendingMessage struct {
	Message     *Message
	Status      DeliveryStatus
	ReceivedAt  time.Time
	SequencedAt time.Time
	DeliveredAt time.Time
	mu          sync.RWMutex
}

// Config holds the Total Order Broadcast configuration
type Config struct {
	// NodeID is the unique identifier for this node
	NodeID string

	// BindAddr is the address this node binds to
	BindAddr string

	// AdvertiseAddr is the address other nodes use to reach this node
	AdvertiseAddr string

	// IsSequencer indicates if this node is the initial sequencer
	// Paper: "Fixed Sequencer: one process is designated as the sequencer"
	IsSequencer bool

	// SequencerAddr is the address of the current sequencer
	SequencerAddr string

	// SequencerID is the ID of the current sequencer
	SequencerID string

	// Nodes is the list of all nodes in the system
	Nodes []string

	// SequencerTimeout is how long to wait before detecting sequencer failure
	SequencerTimeout time.Duration

	// HeartbeatInterval is how often the sequencer sends heartbeats
	HeartbeatInterval time.Duration

	// DeliveryBufferSize is the size of the delivery buffer
	// Paper: "Messages are delivered in sequence number order"
	DeliveryBufferSize int

	// MessageTimeout is how long to wait for message delivery
	MessageTimeout time.Duration

	// EnableSequencerFailover enables automatic sequencer election
	EnableSequencerFailover bool

	// SequencerPriority is used during election (higher = preferred)
	SequencerPriority int

	// Logger for debugging
	Logger Logger
}

// DefaultConfig returns a Config with sensible default values
func DefaultConfig() *Config {
	return &Config{
		SequencerTimeout:        5 * time.Second,
		HeartbeatInterval:       1 * time.Second,
		DeliveryBufferSize:      1000,
		MessageTimeout:          10 * time.Second,
		EnableSequencerFailover: true,
		SequencerPriority:       0,
		Logger:                  &defaultLogger{},
	}
}

// Logger interface for logging
type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// defaultLogger is a no-op logger implementation
type defaultLogger struct{}

func (l *defaultLogger) Debugf(_ string, _ ...interface{}) {}
func (l *defaultLogger) Infof(_ string, _ ...interface{})  {}
func (l *defaultLogger) Warnf(_ string, _ ...interface{})  {}
func (l *defaultLogger) Errorf(_ string, _ ...interface{}) {}

// DeliveryCallback is called when a message is delivered in total order
// Paper: "The algorithm guarantees that all correct processes deliver
// messages in the same order"
type DeliveryCallback func(message *Message)

// SequencerChangeCallback is called when the sequencer changes
type SequencerChangeCallback func(oldSequencer, newSequencer string)

// Statistics tracks protocol metrics
type Statistics struct {
	MessagesSent      uint64
	MessagesDelivered uint64
	MessagesDropped   uint64
	AverageLatency    time.Duration
	SequencerChanges  uint64
	mu                sync.RWMutex
}

// GetMessagesSent returns the number of messages sent
func (s *Statistics) GetMessagesSent() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.MessagesSent
}

// GetMessagesDelivered returns the number of messages delivered
func (s *Statistics) GetMessagesDelivered() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.MessagesDelivered
}

// GetMessagesDropped returns the number of messages dropped
func (s *Statistics) GetMessagesDropped() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.MessagesDropped
}

// GetSequencerChanges returns the number of sequencer changes
func (s *Statistics) GetSequencerChanges() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.SequencerChanges
}

// IncrementMessagesSent increments the messages sent counter
func (s *Statistics) IncrementMessagesSent() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.MessagesSent++
}

// IncrementMessagesDelivered increments the messages delivered counter
func (s *Statistics) IncrementMessagesDelivered() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.MessagesDelivered++
}

// IncrementMessagesDropped increments the messages dropped counter
func (s *Statistics) IncrementMessagesDropped() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.MessagesDropped++
}

// IncrementSequencerChanges increments the sequencer changes counter
func (s *Statistics) IncrementSequencerChanges() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SequencerChanges++
}

// UpdateAverageLatency updates the average latency metric
func (s *Statistics) UpdateAverageLatency(latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Simple moving average
	if s.AverageLatency == 0 {
		s.AverageLatency = latency
	} else {
		s.AverageLatency = (s.AverageLatency + latency) / 2
	}
}
