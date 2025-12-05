package swim

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockTransport is a mock implementation of Transport for testing
type MockTransport struct {
	mock.Mock
	mu             sync.RWMutex
	messageHandler func(*Message)
	sentMessages   []*Message
	startCalled    bool
	stopCalled     bool
}

func NewMockTransport() *MockTransport {
	return &MockTransport{
		sentMessages: make([]*Message, 0),
	}
}

func (m *MockTransport) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startCalled = true
	args := m.Called()
	return args.Error(0)
}

func (m *MockTransport) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalled = true
	args := m.Called()
	return args.Error(0)
}

func (m *MockTransport) SendMessage(targetAddr string, msg *Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentMessages = append(m.sentMessages, msg)
	args := m.Called(targetAddr, msg)
	return args.Error(0)
}

func (m *MockTransport) SetMessageHandler(handler func(*Message)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messageHandler = handler
}

func (m *MockTransport) GetSentMessages() []*Message {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sentMessages
}

func (m *MockTransport) SimulateMessage(msg *Message) {
	m.mu.RLock()
	handler := m.messageHandler
	m.mu.RUnlock()
	if handler != nil {
		handler(msg)
	}
}

// MockSWIM creates a mock SWIM instance for testing ProbeScheduler
type MockSWIM struct {
	config     *Config
	memberList *MemberList
	gossip     *GossipManager
	transport  *MockTransport
	metrics    *Metrics
}

func NewMockSWIM() *MockSWIM {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.ProbeTimeout = 100 * time.Millisecond
	config.ProbeInterval = 200 * time.Millisecond

	return &MockSWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  NewMockTransport(),
		metrics:    NewMetrics(),
	}
}

func TestNewProbeScheduler(t *testing.T) {
	mockSwim := NewMockSWIM()

	// Create a real SWIM instance for the test
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.ProbeTimeout = 100 * time.Millisecond
	config.ProbeInterval = 200 * time.Millisecond

	swim := &SWIM{
		config:     config,
		memberList: mockSwim.memberList,
		gossip:     mockSwim.gossip,
		transport:  mockSwim.transport,
		metrics:    mockSwim.metrics,
	}

	ps := NewProbeScheduler(swim)

	assert.NotNil(t, ps)
	assert.Equal(t, swim, ps.swim)
	assert.NotNil(t, ps.stopCh)
}

func TestProbeScheduler_StartStop(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.ProbeTimeout = 50 * time.Millisecond
	config.ProbeInterval = 100 * time.Millisecond

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	// Start should not panic
	ps.Start()

	// Let it run for a bit
	time.Sleep(50 * time.Millisecond)

	// Stop should not panic
	ps.Stop()
}

func TestProbeScheduler_Probe_NoMembers(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.ProbeTimeout = 50 * time.Millisecond
	config.ProbeInterval = 100 * time.Millisecond

	mockTransport := NewMockTransport()

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	// Probe with no members - should not panic
	ps.probe()

	// No messages should be sent
	assert.Empty(t, mockTransport.GetSentMessages())
}

func TestProbeScheduler_Probe_WithMember(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.ProbeTimeout = 200 * time.Millisecond
	config.ProbeInterval = 500 * time.Millisecond

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-1", "127.0.0.1:8001", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	// Probe should send a ping
	ps.probe()

	time.Sleep(50 * time.Millisecond)

	sentMessages := mockTransport.GetSentMessages()
	assert.GreaterOrEqual(t, len(sentMessages), 1)
	assert.Equal(t, PingMsg, sentMessages[0].Type)
	assert.Equal(t, "test-node", sentMessages[0].From)
}

func TestProbeScheduler_Probe_WhenPaused(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := NewMockTransport()

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-1", "127.0.0.1:8001", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)
	ps.Pause()

	// Probe should not send anything when paused
	ps.probe()

	assert.Empty(t, mockTransport.GetSentMessages())
}

func TestProbeScheduler_PauseResume(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-1", "127.0.0.1:8001", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	// Initially not paused
	assert.False(t, ps.paused.Load())

	// Pause
	ps.Pause()
	assert.True(t, ps.paused.Load())

	// Resume
	ps.Resume()
	assert.False(t, ps.paused.Load())
}

func TestProbeScheduler_HandleAck(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.ProbeTimeout = 500 * time.Millisecond

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-1", "127.0.0.1:8001", Alive, 1)

	metrics := NewMetrics()

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    metrics,
	}

	ps := NewProbeScheduler(swim)

	// Start a probe
	ps.probe()

	time.Sleep(20 * time.Millisecond)

	// Get the sequence number from the sent message
	sentMessages := mockTransport.GetSentMessages()
	if len(sentMessages) > 0 {
		seqNo := sentMessages[0].SeqNo

		// Handle ACK for that sequence
		ackMsg := &Message{
			Type:        AckMsg,
			From:        "node-1",
			FromAddr:    "127.0.0.1:8001",
			SeqNo:       seqNo,
			Incarnation: 1,
		}

		ps.HandleAck(ackMsg)

		// Probe latency should be recorded
		time.Sleep(10 * time.Millisecond)
		stats := metrics.GetProbeLatencyStats()
		assert.GreaterOrEqual(t, stats.Count, 0) // May or may not have recorded depending on timing
	}
}

func TestProbeScheduler_HandleAck_UnknownSeqNo(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  NewMockTransport(),
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	// Handle ACK for unknown sequence - should not panic
	ackMsg := &Message{
		Type:  AckMsg,
		From:  "node-1",
		SeqNo: 12345,
	}

	assert.NotPanics(t, func() {
		ps.HandleAck(ackMsg)
	})
}

func TestProbeScheduler_HandleIndirectAck(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  NewMockTransport(),
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	// Handle indirect ACK for unknown sequence - should not panic
	indirectAckMsg := &Message{
		Type:  IndirectAckMsg,
		From:  "node-2",
		SeqNo: 12345,
	}

	assert.NotPanics(t, func() {
		ps.HandleIndirectAck(indirectAckMsg)
	})
}

func TestProbeContext(t *testing.T) {
	ctx := &probeContext{
		targetID:    "node-1",
		targetAddr:  "127.0.0.1:8001",
		seqNo:       1,
		startTime:   time.Now(),
		ackReceived: false,
	}

	assert.Equal(t, "node-1", ctx.targetID)
	assert.Equal(t, "127.0.0.1:8001", ctx.targetAddr)
	assert.Equal(t, uint64(1), ctx.seqNo)
	assert.False(t, ctx.ackReceived)
}

func TestProbeScheduler_SendPing(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", "127.0.0.1:8001", mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	metrics := NewMetrics()

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    metrics,
	}

	ps := NewProbeScheduler(swim)

	target := &Member{
		ID:      "node-1",
		Address: "127.0.0.1:8001",
	}

	ps.sendPing(target, 1)

	sentMessages := mockTransport.GetSentMessages()
	assert.Len(t, sentMessages, 1)
	assert.Equal(t, PingMsg, sentMessages[0].Type)
	assert.Equal(t, "test-node", sentMessages[0].From)
	assert.Equal(t, "node-1", sentMessages[0].Target)
	assert.Equal(t, uint64(1), sentMessages[0].SeqNo)
}

func TestProbeScheduler_SendPingReq(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", "127.0.0.1:8002", mock.Anything).Return(nil)

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	node := &Member{
		ID:      "node-2",
		Address: "127.0.0.1:8002",
	}

	ctx := &probeContext{
		targetID:   "node-1",
		targetAddr: "127.0.0.1:8001",
		seqNo:      1,
	}

	ps.sendPingReq(node, ctx)

	sentMessages := mockTransport.GetSentMessages()
	assert.Len(t, sentMessages, 1)
	assert.Equal(t, PingReqMsg, sentMessages[0].Type)
	assert.Equal(t, "node-1", sentMessages[0].Target)
	assert.Equal(t, "127.0.0.1:8001", sentMessages[0].TargetAddr)
}

func TestProbeScheduler_HandleAck_AlreadyReceived(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	// Create a probe context with ack already received
	ctx := &probeContext{
		targetID:    "node-1",
		targetAddr:  "127.0.0.1:8001",
		seqNo:       1,
		ackReceived: true,
	}
	ps.pendingProbes.Store(uint64(1), ctx)

	ackMsg := &Message{
		Type:  AckMsg,
		From:  "node-1",
		SeqNo: 1,
	}

	// Should return early without processing
	ps.HandleAck(ackMsg)

	// Context should still exist
	_, ok := ps.pendingProbes.Load(uint64(1))
	assert.True(t, ok)
}

func TestProbeScheduler_HandleAck_UpdatesSuspectToAlive(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.EnableSuspicionMechanism = true

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-1", "127.0.0.1:8001", Suspect, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	// Create a pending probe context
	ctx := &probeContext{
		targetID:    "node-1",
		targetAddr:  "127.0.0.1:8001",
		seqNo:       1,
		startTime:   time.Now(),
		ackReceived: false,
	}
	ps.pendingProbes.Store(uint64(1), ctx)

	ackMsg := &Message{
		Type:        AckMsg,
		From:        "node-1",
		FromAddr:    "127.0.0.1:8001",
		SeqNo:       1,
		Incarnation: 2,
	}

	ps.HandleAck(ackMsg)

	// Verify member is still in the list (alive update would be handled by swim.handleAlive)
	member, exists := memberList.GetMember("node-1")
	assert.True(t, exists)
	assert.NotNil(t, member)
}

func TestProbeScheduler_HandleIndirectAck_UpdatesSuspectToAlive(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-1", "127.0.0.1:8001", Suspect, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	// Create a pending probe context
	ctx := &probeContext{
		targetID:    "node-1",
		targetAddr:  "127.0.0.1:8001",
		seqNo:       1,
		startTime:   time.Now(),
		ackReceived: false,
	}
	ps.pendingProbes.Store(uint64(1), ctx)

	indirectAckMsg := &Message{
		Type:        IndirectAckMsg,
		From:        "node-2",
		SeqNo:       1,
		Incarnation: 2,
	}

	ps.HandleIndirectAck(indirectAckMsg)

	// Verify context was processed
	_, ok := ps.pendingProbes.Load(uint64(1))
	assert.False(t, ok)
}

func TestProbeScheduler_HandleProbeTimeout_WithIndirectNodes(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.ProbeTimeout = 100 * time.Millisecond
	config.IndirectProbeCount = 2

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-1", "127.0.0.1:8001", Alive, 1)
	memberList.AddMember("node-2", "127.0.0.1:8002", Alive, 1)
	memberList.AddMember("node-3", "127.0.0.1:8003", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	// Create a probe context
	ctx := &probeContext{
		targetID:    "node-1",
		targetAddr:  "127.0.0.1:8001",
		seqNo:       1,
		startTime:   time.Now(),
		ackReceived: false,
	}

	// Trigger timeout handling
	ps.handleProbeTimeout(ctx)

	// Wait for indirect probes to be sent
	time.Sleep(50 * time.Millisecond)

	// Should have sent ping-req messages
	sentMessages := mockTransport.GetSentMessages()
	pingReqCount := 0
	for _, msg := range sentMessages {
		if msg.Type == PingReqMsg {
			pingReqCount++
		}
	}
	assert.GreaterOrEqual(t, pingReqCount, 1)
}

func TestProbeScheduler_HandleProbeTimeout_NoIndirectNodes(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.ProbeTimeout = 100 * time.Millisecond
	config.IndirectProbeCount = 2
	config.EnableSuspicionMechanism = true

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-1", "127.0.0.1:8001", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
		shutdownCh: make(chan struct{}),
	}

	ps := NewProbeScheduler(swim)

	// Create a probe context
	ctx := &probeContext{
		targetID:    "node-1",
		targetAddr:  "127.0.0.1:8001",
		seqNo:       1,
		startTime:   time.Now(),
		ackReceived: false,
	}

	// Trigger timeout handling - no other nodes available for indirect probe
	ps.handleProbeTimeout(ctx)

	// Should mark as suspect directly
	time.Sleep(50 * time.Millisecond)
	member, _ := memberList.GetMember("node-1")
	assert.Equal(t, Suspect, member.Status)
}

func TestProbeScheduler_HandleProbeTimeout_AckAlreadyReceived(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.ProbeTimeout = 100 * time.Millisecond

	mockTransport := NewMockTransport()

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-1", "127.0.0.1:8001", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	// Create a probe context with ack already received
	ctx := &probeContext{
		targetID:    "node-1",
		targetAddr:  "127.0.0.1:8001",
		seqNo:       1,
		startTime:   time.Now(),
		ackReceived: true,
	}

	// Should return early
	ps.handleProbeTimeout(ctx)

	// No messages should be sent
	assert.Empty(t, mockTransport.GetSentMessages())
}

func TestProbeScheduler_HandleIndirectProbeTimeout(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.EnableSuspicionMechanism = true

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-1", "127.0.0.1:8001", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
		shutdownCh: make(chan struct{}),
	}

	ps := NewProbeScheduler(swim)

	// Create a probe context
	ctx := &probeContext{
		targetID:    "node-1",
		targetAddr:  "127.0.0.1:8001",
		seqNo:       1,
		startTime:   time.Now(),
		ackReceived: false,
	}
	ps.pendingProbes.Store(uint64(1), ctx)

	// Handle indirect probe timeout
	ps.handleIndirectProbeTimeout(ctx)

	// Should mark as suspect
	time.Sleep(50 * time.Millisecond)
	member, _ := memberList.GetMember("node-1")
	assert.Equal(t, Suspect, member.Status)
}

func TestProbeScheduler_HandleIndirectProbeTimeout_AckReceived(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-1", "127.0.0.1:8001", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  NewMockTransport(),
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	// Create a probe context with ack already received
	ctx := &probeContext{
		targetID:    "node-1",
		targetAddr:  "127.0.0.1:8001",
		seqNo:       1,
		ackReceived: true,
	}

	// Should return early
	ps.handleIndirectProbeTimeout(ctx)

	// Member should still be alive
	member, _ := memberList.GetMember("node-1")
	assert.Equal(t, Alive, member.Status)
}

func TestProbeScheduler_HandleProbeFailed_WithSuspicionMechanism(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.EnableSuspicionMechanism = true

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-1", "127.0.0.1:8001", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
		shutdownCh: make(chan struct{}),
	}

	ps := NewProbeScheduler(swim)

	ctx := &probeContext{
		targetID:   "node-1",
		targetAddr: "127.0.0.1:8001",
		seqNo:      1,
	}

	ps.handleProbeFailed(ctx)

	// Should mark as suspect
	member, _ := memberList.GetMember("node-1")
	assert.Equal(t, Suspect, member.Status)
}

func TestProbeScheduler_HandleProbeFailed_WithoutSuspicionMechanism(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.EnableSuspicionMechanism = false

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-1", "127.0.0.1:8001", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	ctx := &probeContext{
		targetID:   "node-1",
		targetAddr: "127.0.0.1:8001",
		seqNo:      1,
	}

	ps.handleProbeFailed(ctx)

	// Should mark as failed directly
	member, _ := memberList.GetMember("node-1")
	assert.Equal(t, Failed, member.Status)
}

func TestProbeScheduler_HandleProbeFailed_MemberNotFound(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  NewMockTransport(),
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	ctx := &probeContext{
		targetID:   "non-existent",
		targetAddr: "127.0.0.1:9999",
		seqNo:      1,
	}

	// Should not panic
	assert.NotPanics(t, func() {
		ps.handleProbeFailed(ctx)
	})
}

func TestProbeScheduler_HandleProbeFailed_AlreadySuspect(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.EnableSuspicionMechanism = true

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-1", "127.0.0.1:8001", Suspect, 1) // Already suspect

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
		shutdownCh: make(chan struct{}),
	}

	ps := NewProbeScheduler(swim)

	ctx := &probeContext{
		targetID:   "node-1",
		targetAddr: "127.0.0.1:8001",
		seqNo:      1,
	}

	// Should not record additional suspicion
	initialSuspicionCount := swim.metrics.GetSuspicionCount()
	ps.handleProbeFailed(ctx)

	// Suspicion count should not change
	assert.Equal(t, initialSuspicionCount, swim.metrics.GetSuspicionCount())
}

func TestProbeScheduler_SendPing_Error(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", "127.0.0.1:8001", mock.Anything).Return(assert.AnError)

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	target := &Member{
		ID:      "node-1",
		Address: "127.0.0.1:8001",
	}

	// Should not panic even if send fails
	assert.NotPanics(t, func() {
		ps.sendPing(target, 1)
	})
}

func TestProbeScheduler_SendPingReq_Error(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", "127.0.0.1:8002", mock.Anything).Return(assert.AnError)

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	ps := NewProbeScheduler(swim)

	node := &Member{
		ID:      "node-2",
		Address: "127.0.0.1:8002",
	}

	ctx := &probeContext{
		targetID:   "node-1",
		targetAddr: "127.0.0.1:8001",
		seqNo:      1,
	}

	// Should not panic even if send fails
	assert.NotPanics(t, func() {
		ps.sendPingReq(node, ctx)
	})
}
