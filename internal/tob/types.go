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
	// HeartbeatMsg is sent by sequencer for failure detection (implementation extension)
	HeartbeatMsg
)

func (m MessageType) String() string {
	switch m {
	case DataMsg:
		return "Data"
	case SequencedMsg:
		return "Sequenced"
	case HeartbeatMsg:
		return "Heartbeat"
	default:
		return "Unknown"
	}
}

// Message represents a Total Order Broadcast message
// Paper (Section 4.1): "Fixed Sequencer algorithm: a centralized process (the sequencer)
// assigns a sequence number to each message"
type Message struct {
	Type           MessageType
	From           string    // Sender's ID
	FromAddr       string    // Sender's address
	SequenceNumber uint64    // Sequence number assigned by sequencer
	MessageID      string    // Unique message identifier
	Payload        []byte    // Application data
	Timestamp      time.Time // Message timestamp
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
	// Paper (Section 4.1): "Fixed Sequencer: one process is designated as the sequencer"
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
	// Paper (Section 4.1): "Messages are delivered in sequence number order"
	DeliveryBufferSize int

	// MessageTimeout is how long to wait for message delivery
	MessageTimeout time.Duration

	// Logger for debugging
	Logger Logger
}

// DefaultConfig returns a Config with sensible default values
func DefaultConfig() *Config {
	return &Config{
		SequencerTimeout:   5 * time.Second,
		HeartbeatInterval:  1 * time.Second,
		DeliveryBufferSize: 1000,
		MessageTimeout:     10 * time.Second,
		Logger:             &defaultLogger{},
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
// Paper (Section 4.1): "The algorithm guarantees that all correct processes deliver
// messages in the same order"
type DeliveryCallback func(message *Message)
