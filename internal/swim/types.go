package swim

import (
	"sync"
	"time"
)

// MemberStatus represents the state of a member in the SWIM protocol
type MemberStatus int

const (
	// Alive means the member is functioning correctly
	Alive MemberStatus = iota
	// Suspect means the member may have failed (Section 4.2: "suspicion mechanism")
	Suspect
	// Failed means the member has been confirmed as failed
	Failed
	// Left means the member has voluntarily left the cluster
	Left
)

func (s MemberStatus) String() string {
	switch s {
	case Alive:
		return "Alive"
	case Suspect:
		return "Suspect"
	case Failed:
		return "Failed"
	case Left:
		return "Left"
	default:
		return "Unknown"
	}
}

// Member represents a single node in the SWIM cluster
// Section 4: "Member maintains a membership list, where each entry contains... address and a state"
type Member struct {
	ID           string       // Unique identifier for the member
	Address      string       // Network address (host:port)
	Status       MemberStatus // Current status of the member
	Incarnation  uint64       // Incarnation number for refuting suspicions (Section 4.3)
	LocalTime    time.Time    // Local timestamp when status was last updated
	suspectTimer *time.Timer  // Timer for suspect timeout
	mu           sync.RWMutex // Protects member fields
}

// MessageType identifies the type of SWIM protocol message
type MessageType int

const (
	// Ping is sent to probe a member (Section 3: "ping a randomly selected member")
	PingMsg MessageType = iota
	// Ack is the response to a Ping
	AckMsg
	// PingReq is an indirect probe request (Section 3: "ping-req protocol message")
	PingReqMsg
	// IndirectPing is sent by a node receiving PingReq
	IndirectPingMsg
	// IndirectAck is the response to IndirectPing
	IndirectAckMsg
	// Suspect announces suspicion of a member
	SuspectMsg
	// Alive announces a member is alive (Section 4.3: "refutation mechanism")
	AliveMsg
	// Confirm announces confirmed failure
	ConfirmMsg
	// Leave announces voluntary departure
	LeaveMsg
	// Join is used for joining the cluster
	JoinMsg
	// Sync requests full membership state
	SyncMsg
)

func (m MessageType) String() string {
	switch m {
	case PingMsg:
		return "Ping"
	case AckMsg:
		return "Ack"
	case PingReqMsg:
		return "PingReq"
	case IndirectPingMsg:
		return "IndirectPing"
	case IndirectAckMsg:
		return "IndirectAck"
	case SuspectMsg:
		return "Suspect"
	case AliveMsg:
		return "Alive"
	case ConfirmMsg:
		return "Confirm"
	case LeaveMsg:
		return "Leave"
	case JoinMsg:
		return "Join"
	case SyncMsg:
		return "Sync"
	default:
		return "Unknown"
	}
}

// Message represents a SWIM protocol message
type Message struct {
	Type        MessageType
	From        string   // Sender's ID
	FromAddr    string   // Sender's address
	Target      string   // Target member ID (for indirect pings)
	TargetAddr  string   // Target member address
	SeqNo       uint64   // Sequence number for tracking requests
	Incarnation uint64   // Incarnation number (for alive/suspect messages)
	Piggyback   []Update // Piggybacked membership updates
}

// Update represents a membership state update
// Section 4.4: "disseminate information via piggybacking"
type Update struct {
	MemberID    string
	Address     string
	Status      MemberStatus
	Incarnation uint64
	Timestamp   time.Time
}

// Config holds the SWIM protocol configuration parameters
type Config struct {
	// BindAddr is the address this node binds to
	BindAddr string

	// AdvertiseAddr is the address other nodes use to reach this node
	AdvertiseAddr string

	// NodeID is the unique identifier for this node
	NodeID string

	// ProtocolPeriod is the period of the protocol
	// Default: 1 second
	ProtocolPeriod time.Duration

	// ProbeTimeout is the timeout for direct probe
	// Should be less than ProtocolPeriod
	ProbeTimeout time.Duration

	// ProbeInterval is how often to select a random member to probe
	ProbeInterval time.Duration

	// IndirectProbeCount is the number of nodes to ask for indirect probes
	// Paper suggests k members for indirect probing
	IndirectProbeCount int

	// SuspicionTimeout is how long to wait before marking suspect as failed
	SuspicionTimeout time.Duration

	// SuspicionMultiplier adjusts suspicion timeout based on cluster size
	SuspicionMultiplier int

	// MaxGossipPacketSize is the maximum size for piggybacked updates
	MaxGossipPacketSize int

	// NumGossipRetransmissions is the number of times to retransmit an update
	NumGossipRetransmissions int

	// GossipFanout is the number of members to send each gossip to
	GossipFanout int

	// EnableSuspicionMechanism toggles the suspicion mechanism
	EnableSuspicionMechanism bool

	// JoinNodes are the seed nodes to contact when joining
	JoinNodes []string

	// Logger for debugging
	Logger Logger
}

// DefaultConfig returns a Config with sensible default values
func DefaultConfig() *Config {
	return &Config{
		ProtocolPeriod:           1 * time.Second,
		ProbeTimeout:             500 * time.Millisecond,
		ProbeInterval:            1 * time.Second,
		IndirectProbeCount:       3,
		SuspicionTimeout:         5 * time.Second,
		SuspicionMultiplier:      4,
		MaxGossipPacketSize:      1400, // MTU-safe
		NumGossipRetransmissions: 2,
		GossipFanout:             3,
		EnableSuspicionMechanism: true,
		Logger:                   &defaultLogger{},
	}
}

// Logger interface for logging
type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// defaultLogger is a simple logger implementation
type defaultLogger struct{}

func (l *defaultLogger) Debugf(_ string, _ ...interface{}) {}
func (l *defaultLogger) Infof(_ string, _ ...interface{})  {}
func (l *defaultLogger) Warnf(_ string, _ ...interface{})  {}
func (l *defaultLogger) Errorf(_ string, _ ...interface{}) {}

// Event types for pub/sub
type EventType int

const (
	// MemberJoinEvent is fired when a new member joins
	MemberJoinEvent EventType = iota
	// MemberLeaveEvent is fired when a member leaves
	MemberLeaveEvent
	// MemberFailedEvent is fired when a member is confirmed as failed
	MemberFailedEvent
	// MemberUpdateEvent is fired when member status changes
	MemberUpdateEvent
)

// MemberEventPayload carries information about member events
type MemberEventPayload struct {
	Member    *Member
	OldStatus MemberStatus
	NewStatus MemberStatus
}
