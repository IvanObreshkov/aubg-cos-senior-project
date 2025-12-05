package swim

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// transportMock is used for testing swim.go
type transportMock struct {
	mock.Mock
	mu             sync.RWMutex
	messageHandler func(*Message)
	sentMessages   []*Message
	blocked        bool
}

func newTransportMock() *transportMock {
	return &transportMock{
		sentMessages: make([]*Message, 0),
	}
}

func (t *transportMock) Start() error {
	args := t.Called()
	return args.Error(0)
}

func (t *transportMock) Stop() error {
	args := t.Called()
	return args.Error(0)
}

func (t *transportMock) SendMessage(targetAddr string, msg *Message) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sentMessages = append(t.sentMessages, msg)
	args := t.Called(targetAddr, msg)
	return args.Error(0)
}

func (t *transportMock) SetMessageHandler(handler func(*Message)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.messageHandler = handler
}

func (t *transportMock) getSentMessages() []*Message {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([]*Message, len(t.sentMessages))
	copy(result, t.sentMessages)
	return result
}

func (t *transportMock) simulateMessage(msg *Message) {
	t.mu.RLock()
	handler := t.messageHandler
	t.mu.RUnlock()
	if handler != nil {
		handler(msg)
	}
}

func TestValidateConfig(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		config := &Config{
			NodeID:        "node-1",
			BindAddr:      "127.0.0.1:8000",
			AdvertiseAddr: "127.0.0.1:8000",
			ProbeTimeout:  100 * time.Millisecond,
			ProbeInterval: 200 * time.Millisecond,
		}
		err := validateConfig(config)
		assert.NoError(t, err)
	})

	t.Run("missing NodeID", func(t *testing.T) {
		config := &Config{
			BindAddr:      "127.0.0.1:8000",
			AdvertiseAddr: "127.0.0.1:8000",
		}
		err := validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "NodeID")
	})

	t.Run("missing BindAddr", func(t *testing.T) {
		config := &Config{
			NodeID:        "node-1",
			AdvertiseAddr: "127.0.0.1:8000",
		}
		err := validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "BindAddr")
	})

	t.Run("missing AdvertiseAddr", func(t *testing.T) {
		config := &Config{
			NodeID:   "node-1",
			BindAddr: "127.0.0.1:8000",
		}
		err := validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "AdvertiseAddr")
	})

	t.Run("ProbeTimeout >= ProbeInterval", func(t *testing.T) {
		config := &Config{
			NodeID:        "node-1",
			BindAddr:      "127.0.0.1:8000",
			AdvertiseAddr: "127.0.0.1:8000",
			ProbeTimeout:  200 * time.Millisecond,
			ProbeInterval: 100 * time.Millisecond,
		}
		err := validateConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ProbeTimeout")
	})
}

func TestNew(t *testing.T) {
	t.Run("with nil config uses defaults", func(t *testing.T) {
		// This will fail because nil config needs defaults but no NodeID
		swim, err := New(nil)
		assert.Error(t, err)
		assert.Nil(t, swim)
	})

	t.Run("with valid config", func(t *testing.T) {
		config := DefaultConfig()
		config.NodeID = "node-1"
		config.BindAddr = "127.0.0.1:18000"
		config.AdvertiseAddr = "127.0.0.1:18000"

		swim, err := New(config)
		assert.NoError(t, err)
		assert.NotNil(t, swim)
		assert.Equal(t, "node-1", swim.config.NodeID)
		assert.NotNil(t, swim.memberList)
		assert.NotNil(t, swim.gossip)
		assert.NotNil(t, swim.transport)
		assert.NotNil(t, swim.probe)
		assert.NotNil(t, swim.metrics)
	})

	t.Run("with invalid config returns error", func(t *testing.T) {
		config := &Config{
			NodeID: "", // Invalid
		}
		swim, err := New(config)
		assert.Error(t, err)
		assert.Nil(t, swim)
	})
}

func TestSWIM_GetMembers(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := newTransportMock()

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
		shutdownCh: make(chan struct{}),
	}

	swim.memberList.AddMember("node-2", "127.0.0.1:8001", Alive, 1)
	swim.memberList.AddMember("node-3", "127.0.0.1:8002", Suspect, 1)

	members := swim.GetMembers()
	assert.Len(t, members, 2)
}

func TestSWIM_GetAliveMembers(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  newTransportMock(),
		metrics:    NewMetrics(),
	}

	swim.memberList.AddMember("node-2", "127.0.0.1:8001", Alive, 1)
	swim.memberList.AddMember("node-3", "127.0.0.1:8002", Failed, 1)

	aliveMembers := swim.GetAliveMembers()
	assert.Len(t, aliveMembers, 1)
}

func TestSWIM_NumMembers(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
	}

	assert.Equal(t, 0, swim.NumMembers())

	swim.memberList.AddMember("node-2", "127.0.0.1:8001", Alive, 1)
	assert.Equal(t, 1, swim.NumMembers())
}

func TestSWIM_LocalNode(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
	}

	local := swim.LocalNode()
	assert.NotNil(t, local)
	assert.Equal(t, "node-1", local.ID)
	assert.Equal(t, "127.0.0.1:8000", local.Address)
}

func TestSWIM_Callbacks(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
	}

	var joinCalled, leaveCalled, failedCalled, updateCalled bool

	swim.OnMemberJoin(func(m *Member) {
		joinCalled = true
	})

	swim.OnMemberLeave(func(m *Member) {
		leaveCalled = true
	})

	swim.OnMemberFailed(func(m *Member) {
		failedCalled = true
	})

	swim.OnMemberUpdate(func(m *Member) {
		updateCalled = true
	})

	// Verify callbacks are set
	assert.NotNil(t, swim.onMemberJoin)
	assert.NotNil(t, swim.onMemberLeave)
	assert.NotNil(t, swim.onMemberFailed)
	assert.NotNil(t, swim.onMemberUpdate)

	// Call them to verify
	if swim.onMemberJoin != nil {
		swim.onMemberJoin(nil)
	}
	if swim.onMemberLeave != nil {
		swim.onMemberLeave(nil)
	}
	if swim.onMemberFailed != nil {
		swim.onMemberFailed(nil)
	}
	if swim.onMemberUpdate != nil {
		swim.onMemberUpdate(nil)
	}

	assert.True(t, joinCalled)
	assert.True(t, leaveCalled)
	assert.True(t, failedCalled)
	assert.True(t, updateCalled)
}

func TestSWIM_SetGetMetrics(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		metrics:    NewMetrics(),
	}

	originalMetrics := swim.GetMetrics()
	assert.NotNil(t, originalMetrics)

	newMetrics := NewMetrics()
	swim.SetMetrics(newMetrics)

	assert.Equal(t, newMetrics, swim.GetMetrics())
}

func TestSWIM_HandlePing(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := newTransportMock()
	mockTransport.On("SendMessage", "127.0.0.1:8001", mock.Anything).Return(nil)

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	pingMsg := &Message{
		Type:        PingMsg,
		From:        "node-2",
		FromAddr:    "127.0.0.1:8001",
		SeqNo:       1,
		Incarnation: 1,
	}

	swim.handlePing(pingMsg)

	sentMessages := mockTransport.getSentMessages()
	assert.Len(t, sentMessages, 1)
	assert.Equal(t, AckMsg, sentMessages[0].Type)
	assert.Equal(t, "node-1", sentMessages[0].From)
	assert.Equal(t, uint64(1), sentMessages[0].SeqNo)

	// Verify member was added
	member, exists := swim.memberList.GetMember("node-2")
	assert.True(t, exists)
	assert.Equal(t, "127.0.0.1:8001", member.Address)
}

func TestSWIM_HandlePingReq(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.ProbeTimeout = 100 * time.Millisecond

	mockTransport := newTransportMock()
	mockTransport.On("SendMessage", "127.0.0.1:8002", mock.Anything).Return(nil)

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	pingReqMsg := &Message{
		Type:       PingReqMsg,
		From:       "node-2",
		FromAddr:   "127.0.0.1:8001",
		Target:     "node-3",
		TargetAddr: "127.0.0.1:8002",
		SeqNo:      1,
	}

	swim.handlePingReq(pingReqMsg)

	sentMessages := mockTransport.getSentMessages()
	assert.Len(t, sentMessages, 1)
	assert.Equal(t, IndirectPingMsg, sentMessages[0].Type)
	assert.Equal(t, "node-3", sentMessages[0].Target)
}

func TestSWIM_HandleIndirectPing(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := newTransportMock()
	mockTransport.On("SendMessage", "127.0.0.1:8001", mock.Anything).Return(nil)

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	indirectPingMsg := &Message{
		Type:        IndirectPingMsg,
		From:        "node-2",
		FromAddr:    "127.0.0.1:8001",
		SeqNo:       1,
		Incarnation: 1,
	}

	swim.handleIndirectPing(indirectPingMsg)

	sentMessages := mockTransport.getSentMessages()
	assert.Len(t, sentMessages, 1)
	assert.Equal(t, IndirectAckMsg, sentMessages[0].Type)
}

func TestSWIM_HandleJoinMsg(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := newTransportMock()
	mockTransport.On("SendMessage", "127.0.0.1:8001", mock.Anything).Return(nil)

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	var joinCallbackCalled bool
	swim.OnMemberJoin(func(m *Member) {
		joinCallbackCalled = true
	})

	joinMsg := &Message{
		Type:        JoinMsg,
		From:        "node-2",
		FromAddr:    "127.0.0.1:8001",
		SeqNo:       1,
		Incarnation: 1,
	}

	swim.handleJoinMsg(joinMsg)

	// Verify member was added
	member, exists := swim.memberList.GetMember("node-2")
	assert.True(t, exists)
	assert.Equal(t, Alive, member.Status)

	// Verify callback was called
	assert.True(t, joinCallbackCalled)

	// Verify ACK was sent
	sentMessages := mockTransport.getSentMessages()
	assert.Len(t, sentMessages, 1)
	assert.Equal(t, AckMsg, sentMessages[0].Type)
}

func TestSWIM_HandleSyncMsg(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := newTransportMock()
	mockTransport.On("SendMessage", "127.0.0.1:8001", mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-3", "127.0.0.1:8002", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	syncMsg := &Message{
		Type:     SyncMsg,
		From:     "node-2",
		FromAddr: "127.0.0.1:8001",
	}

	swim.handleSyncMsg(syncMsg)

	sentMessages := mockTransport.getSentMessages()
	assert.Len(t, sentMessages, 1)
	assert.Equal(t, AckMsg, sentMessages[0].Type)
	assert.NotEmpty(t, sentMessages[0].Piggyback)
}

func TestSWIM_HandleLeaveMsg(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := newTransportMock()

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-2", "127.0.0.1:8001", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	var leaveCallbackCalled bool
	swim.OnMemberLeave(func(m *Member) {
		leaveCallbackCalled = true
	})

	leaveMsg := &Message{
		Type:        LeaveMsg,
		From:        "node-2",
		FromAddr:    "127.0.0.1:8001",
		Incarnation: 2,
	}

	swim.handleLeaveMsg(leaveMsg)

	// Verify status was updated
	member, _ := swim.memberList.GetMember("node-2")
	assert.Equal(t, Left, member.Status)

	// Verify callback was called
	assert.True(t, leaveCallbackCalled)
}

func TestSWIM_HandleSuspectMsg(t *testing.T) {
	t.Run("suspicion about self triggers refutation", func(t *testing.T) {
		config := DefaultConfig()
		config.NodeID = "node-1"
		config.BindAddr = "127.0.0.1:8000"
		config.AdvertiseAddr = "127.0.0.1:8000"

		mockTransport := newTransportMock()
		mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

		swim := &SWIM{
			config:     config,
			memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
			gossip:     NewGossipManager(2, 1400),
			transport:  mockTransport,
			metrics:    NewMetrics(),
		}

		suspectMsg := &Message{
			Type:        SuspectMsg,
			From:        "node-2",
			Target:      "node-1", // Suspicion about self
			Incarnation: 1,
		}

		swim.handleSuspectMsg(suspectMsg)

		// Incarnation should have been incremented
		assert.Greater(t, swim.memberList.LocalMember().Incarnation, uint64(0))
	})

	t.Run("suspicion about other member updates status", func(t *testing.T) {
		config := DefaultConfig()
		config.NodeID = "node-1"
		config.BindAddr = "127.0.0.1:8000"
		config.AdvertiseAddr = "127.0.0.1:8000"
		config.EnableSuspicionMechanism = true

		mockTransport := newTransportMock()
		mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

		memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
		memberList.AddMember("node-2", "127.0.0.1:8001", Alive, 1)

		swim := &SWIM{
			config:     config,
			memberList: memberList,
			gossip:     NewGossipManager(2, 1400),
			transport:  mockTransport,
			metrics:    NewMetrics(),
		}

		suspectMsg := &Message{
			Type:        SuspectMsg,
			From:        "node-3",
			Target:      "node-2",
			Incarnation: 2,
		}

		swim.handleSuspectMsg(suspectMsg)

		member, _ := memberList.GetMember("node-2")
		assert.Equal(t, Suspect, member.Status)
	})
}

func TestSWIM_HandleAliveMsg(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := newTransportMock()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-2", "127.0.0.1:8001", Suspect, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	aliveMsg := &Message{
		Type:        AliveMsg,
		From:        "node-2",
		Incarnation: 2,
	}

	swim.handleAliveMsg(aliveMsg)

	member, _ := memberList.GetMember("node-2")
	assert.Equal(t, Alive, member.Status)
	assert.Equal(t, uint64(2), member.Incarnation)
}

func TestSWIM_HandleConfirmMsg(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := newTransportMock()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-2", "127.0.0.1:8001", Suspect, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	var failedCallbackCalled bool
	swim.OnMemberFailed(func(m *Member) {
		failedCallbackCalled = true
	})

	confirmMsg := &Message{
		Type:   ConfirmMsg,
		From:   "node-3",
		Target: "node-2",
	}

	swim.handleConfirmMsg(confirmMsg)

	member, _ := memberList.GetMember("node-2")
	assert.Equal(t, Failed, member.Status)
	assert.True(t, failedCallbackCalled)
}

func TestSWIM_HandleMessage(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.ProbeTimeout = 100 * time.Millisecond
	config.ProbeInterval = 500 * time.Millisecond

	mockTransport := newTransportMock()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	metrics := NewMetrics()

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    metrics,
		shutdownCh: make(chan struct{}),
	}
	swim.probe = NewProbeScheduler(swim)

	// Test that handleMessage records incoming message
	msg := &Message{
		Type:     PingMsg,
		From:     "node-2",
		FromAddr: "127.0.0.1:8001",
	}

	swim.handleMessage(msg)

	assert.Equal(t, uint64(1), metrics.GetTotalMessagesIn())
}

func TestSWIM_HandleMessage_WithPiggyback(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.ProbeTimeout = 100 * time.Millisecond
	config.ProbeInterval = 500 * time.Millisecond

	mockTransport := newTransportMock()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	metrics := NewMetrics()

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    metrics,
		shutdownCh: make(chan struct{}),
	}
	swim.probe = NewProbeScheduler(swim)

	// Message with piggybacked updates
	msg := &Message{
		Type:     PingMsg,
		From:     "node-2",
		FromAddr: "127.0.0.1:8001",
		Piggyback: []Update{
			{MemberID: "node-3", Address: "127.0.0.1:8002", Status: Alive, Incarnation: 1},
		},
	}

	swim.handleMessage(msg)

	// Verify piggybacked member was added
	member, exists := memberList.GetMember("node-3")
	assert.True(t, exists)
	assert.Equal(t, "127.0.0.1:8002", member.Address)
}

func TestSWIM_HandleMessage_UnknownType(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.ProbeTimeout = 100 * time.Millisecond
	config.ProbeInterval = 500 * time.Millisecond

	mockTransport := newTransportMock()

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
		shutdownCh: make(chan struct{}),
	}
	swim.probe = NewProbeScheduler(swim)

	msg := &Message{
		Type: MessageType(99), // Unknown type
	}

	// Should not panic
	assert.NotPanics(t, func() {
		swim.handleMessage(msg)
	})
}

func TestSWIM_CalculateSuspicionTimeout(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.SuspicionTimeout = 1 * time.Second
	config.SuspicionMultiplier = 4
	config.ProtocolPeriod = 1 * time.Second

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
	}

	t.Run("small cluster uses minimum timeout", func(t *testing.T) {
		timeout := swim.calculateSuspicionTimeout(1)
		assert.Equal(t, config.SuspicionTimeout, timeout)
	})

	t.Run("larger cluster scales timeout", func(t *testing.T) {
		timeout := swim.calculateSuspicionTimeout(100)
		assert.GreaterOrEqual(t, timeout, config.SuspicionTimeout)
	})
}

func TestSWIM_RefuteSuspicion(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := newTransportMock()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-2", "127.0.0.1:8001", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	originalIncarnation := memberList.LocalMember().Incarnation

	swim.refuteSuspicion()

	// Incarnation should be incremented
	assert.Greater(t, memberList.LocalMember().Incarnation, originalIncarnation)

	// Alive message should be sent to members
	sentMessages := mockTransport.getSentMessages()
	assert.GreaterOrEqual(t, len(sentMessages), 1)

	foundAlive := false
	for _, msg := range sentMessages {
		if msg.Type == AliveMsg {
			foundAlive = true
			break
		}
	}
	assert.True(t, foundAlive)
}

func TestSWIM_HandleSuspicion(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.EnableSuspicionMechanism = true
	config.SuspicionTimeout = 100 * time.Millisecond

	mockTransport := newTransportMock()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-2", "127.0.0.1:8001", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
		shutdownCh: make(chan struct{}),
	}

	member, _ := memberList.GetMember("node-2")
	swim.handleSuspicion(member)

	// Member should be marked as suspect
	updatedMember, _ := memberList.GetMember("node-2")
	assert.Equal(t, Suspect, updatedMember.Status)

	// Suspicion should be recorded
	assert.Equal(t, uint64(1), swim.metrics.GetSuspicionCount())
}

func TestSWIM_HandleSuspicion_NilMember(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  newTransportMock(),
		metrics:    NewMetrics(),
	}

	// Should not panic
	assert.NotPanics(t, func() {
		swim.handleSuspicion(nil)
	})
}

func TestSWIM_HandleFailure(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := newTransportMock()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-2", "127.0.0.1:8001", Suspect, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	var failedCallbackCalled bool
	swim.OnMemberFailed(func(m *Member) {
		failedCallbackCalled = true
	})

	member, _ := memberList.GetMember("node-2")
	swim.handleFailure(member)

	// Member should be marked as failed
	updatedMember, _ := memberList.GetMember("node-2")
	assert.Equal(t, Failed, updatedMember.Status)

	// Callback should be called
	assert.True(t, failedCallbackCalled)
}

func TestSWIM_HandleFailure_NilMember(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  newTransportMock(),
		metrics:    NewMetrics(),
	}

	// Should not panic
	assert.NotPanics(t, func() {
		swim.handleFailure(nil)
	})
}

func TestSWIM_HandleAlive(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	mockTransport := newTransportMock()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-2", "127.0.0.1:8001", Suspect, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	swim.handleAlive("node-2", 2)

	// Member should be marked as alive
	member, _ := memberList.GetMember("node-2")
	assert.Equal(t, Alive, member.Status)
	assert.Equal(t, uint64(2), member.Incarnation)

	// Refuted suspicion should be recorded
	assert.Equal(t, uint64(1), swim.metrics.refutedSuspicionCount.Load())
}

func TestSWIM_HandleAlive_NonExistentMember(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  newTransportMock(),
		metrics:    NewMetrics(),
	}

	// Should not panic
	assert.NotPanics(t, func() {
		swim.handleAlive("non-existent", 1)
	})
}

func TestSWIM_CancelSuspectTimer(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
	}

	// Create a timer and store it
	timer := time.NewTimer(1 * time.Hour)
	swim.suspectTimers.Store("node-2", timer)

	// Cancel it
	swim.cancelSuspectTimer("node-2")

	// Timer should be removed
	_, exists := swim.suspectTimers.Load("node-2")
	assert.False(t, exists)
}

func TestSWIM_CancelSuspectTimer_NonExistent(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
	}

	// Should not panic
	assert.NotPanics(t, func() {
		swim.cancelSuspectTimer("non-existent")
	})
}

func TestSWIM_GetMember(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-2", "127.0.0.1:8001", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
	}

	member := swim.getMember("node-2")
	assert.NotNil(t, member)
	assert.Equal(t, "node-2", member.ID)

	nonExistent := swim.getMember("non-existent")
	assert.Nil(t, nonExistent)
}

func TestSWIM_PauseResumeProbes(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.ProbeTimeout = 100 * time.Millisecond
	config.ProbeInterval = 500 * time.Millisecond

	mockTransport := newTransportMock()

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}
	swim.probe = NewProbeScheduler(swim)

	// Should not panic with nil probe
	swim.PauseProbes()
	swim.ResumeProbes()

	assert.False(t, swim.probe.paused.Load())
}

func TestSWIM_HandleMembershipUpdate(t *testing.T) {
	t.Run("handles update about self being suspected", func(t *testing.T) {
		config := DefaultConfig()
		config.NodeID = "node-1"
		config.BindAddr = "127.0.0.1:8000"
		config.AdvertiseAddr = "127.0.0.1:8000"

		mockTransport := newTransportMock()
		mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

		swim := &SWIM{
			config:     config,
			memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
			gossip:     NewGossipManager(2, 1400),
			transport:  mockTransport,
			metrics:    NewMetrics(),
		}

		originalIncarnation := swim.memberList.LocalMember().Incarnation

		update := Update{
			MemberID: "node-1", // Self
			Status:   Suspect,
		}

		swim.handleMembershipUpdate(update, Alive)

		// Should have refuted
		assert.Greater(t, swim.memberList.LocalMember().Incarnation, originalIncarnation)
	})

	t.Run("handles refuted suspicion for other member", func(t *testing.T) {
		config := DefaultConfig()
		config.NodeID = "node-1"
		config.BindAddr = "127.0.0.1:8000"
		config.AdvertiseAddr = "127.0.0.1:8000"

		mockTransport := newTransportMock()
		mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

		memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
		memberList.AddMember("node-2", "127.0.0.1:8001", Alive, 1)

		swim := &SWIM{
			config:     config,
			memberList: memberList,
			gossip:     NewGossipManager(2, 1400),
			transport:  mockTransport,
			metrics:    NewMetrics(),
		}

		update := Update{
			MemberID:    "node-2",
			Status:      Alive,
			Incarnation: 2,
		}

		swim.handleMembershipUpdate(update, Suspect)

		// Should record refuted suspicion
		assert.Equal(t, uint64(1), swim.metrics.refutedSuspicionCount.Load())
	})
}

func TestSWIM_Stop_AlreadyStopped(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.ProbeTimeout = 100 * time.Millisecond
	config.ProbeInterval = 500 * time.Millisecond

	mockTransport := newTransportMock()
	mockTransport.On("Stop").Return(nil)

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
		shutdownCh: make(chan struct{}),
		stopped:    true, // Already stopped
	}
	swim.probe = NewProbeScheduler(swim)

	err := swim.Stop()
	assert.NoError(t, err)
}

func TestSWIM_SimulateNetworkPartition(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:0"
	config.AdvertiseAddr = "127.0.0.1:0"

	// Use a real UDP transport
	transport := NewUDPTransport(config.BindAddr, config.Logger)
	err := transport.Start()
	assert.NoError(t, err)
	defer transport.Stop()

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  transport,
		metrics:    NewMetrics(),
	}

	// Should not panic
	swim.SimulateNetworkPartition()
	assert.True(t, transport.blocked)

	swim.EndNetworkPartition()
	assert.False(t, transport.blocked)
}

func TestSWIM_SimulateNetworkPartition_NonUDPTransport(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	// Use mock transport (not UDPTransport)
	mockTransport := newTransportMock()

	swim := &SWIM{
		config:     config,
		memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
	}

	// Should not panic with non-UDP transport
	assert.NotPanics(t, func() {
		swim.SimulateNetworkPartition()
		swim.EndNetworkPartition()
	})
}

func TestSWIM_HandleMembershipUpdate_Comprehensive(t *testing.T) {
	t.Run("handles update about self being failed", func(t *testing.T) {
		config := DefaultConfig()
		config.NodeID = "node-1"
		config.BindAddr = "127.0.0.1:8000"
		config.AdvertiseAddr = "127.0.0.1:8000"

		mockTransport := newTransportMock()
		mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

		swim := &SWIM{
			config:     config,
			memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
			gossip:     NewGossipManager(2, 1400),
			transport:  mockTransport,
			metrics:    NewMetrics(),
		}

		originalIncarnation := swim.memberList.LocalMember().Incarnation

		update := Update{
			MemberID: "node-1", // Self
			Status:   Failed,
		}

		swim.handleMembershipUpdate(update, Alive)

		// Should have refuted
		assert.Greater(t, swim.memberList.LocalMember().Incarnation, originalIncarnation)
	})

	t.Run("handles suspicion via gossip for other member", func(t *testing.T) {
		config := DefaultConfig()
		config.NodeID = "node-1"
		config.BindAddr = "127.0.0.1:8000"
		config.AdvertiseAddr = "127.0.0.1:8000"

		mockTransport := newTransportMock()
		mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

		memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
		memberList.AddMember("node-2", "127.0.0.1:8001", Alive, 1)

		swim := &SWIM{
			config:     config,
			memberList: memberList,
			gossip:     NewGossipManager(2, 1400),
			transport:  mockTransport,
			metrics:    NewMetrics(),
		}

		update := Update{
			MemberID:    "node-2",
			Status:      Suspect,
			Incarnation: 1,
		}

		swim.handleMembershipUpdate(update, Alive)

		// Timer should be stored
		_, exists := swim.suspectTimers.Load("node-2")
		assert.True(t, exists)
	})

	t.Run("handles failed status for other member", func(t *testing.T) {
		config := DefaultConfig()
		config.NodeID = "node-1"
		config.BindAddr = "127.0.0.1:8000"
		config.AdvertiseAddr = "127.0.0.1:8000"

		mockTransport := newTransportMock()
		mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

		memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
		memberList.AddMember("node-2", "127.0.0.1:8001", Suspect, 1)

		swim := &SWIM{
			config:     config,
			memberList: memberList,
			gossip:     NewGossipManager(2, 1400),
			transport:  mockTransport,
			metrics:    NewMetrics(),
		}

		var failedCallbackCalled bool
		swim.OnMemberFailed(func(m *Member) {
			failedCallbackCalled = true
		})

		update := Update{
			MemberID:    "node-2",
			Status:      Failed,
			Incarnation: 2,
		}

		swim.handleMembershipUpdate(update, Suspect)

		assert.True(t, failedCallbackCalled)
	})

	t.Run("handles left status for other member", func(t *testing.T) {
		config := DefaultConfig()
		config.NodeID = "node-1"
		config.BindAddr = "127.0.0.1:8000"
		config.AdvertiseAddr = "127.0.0.1:8000"

		mockTransport := newTransportMock()

		memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
		memberList.AddMember("node-2", "127.0.0.1:8001", Alive, 1)

		swim := &SWIM{
			config:     config,
			memberList: memberList,
			gossip:     NewGossipManager(2, 1400),
			transport:  mockTransport,
			metrics:    NewMetrics(),
		}

		var leaveCallbackCalled bool
		swim.OnMemberLeave(func(m *Member) {
			leaveCallbackCalled = true
		})

		update := Update{
			MemberID:    "node-2",
			Status:      Left,
			Incarnation: 2,
		}

		swim.handleMembershipUpdate(update, Alive)

		assert.True(t, leaveCallbackCalled)
	})

	t.Run("calls onMemberUpdate when status changes", func(t *testing.T) {
		config := DefaultConfig()
		config.NodeID = "node-1"
		config.BindAddr = "127.0.0.1:8000"
		config.AdvertiseAddr = "127.0.0.1:8000"

		mockTransport := newTransportMock()
		mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

		memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
		memberList.AddMember("node-2", "127.0.0.1:8001", Suspect, 1)

		swim := &SWIM{
			config:     config,
			memberList: memberList,
			gossip:     NewGossipManager(2, 1400),
			transport:  mockTransport,
			metrics:    NewMetrics(),
		}

		var updateCallbackCalled bool
		swim.OnMemberUpdate(func(m *Member) {
			updateCallbackCalled = true
		})

		update := Update{
			MemberID:    "node-2",
			Status:      Alive,
			Incarnation: 2,
		}

		swim.handleMembershipUpdate(update, Suspect)

		assert.True(t, updateCallbackCalled)
	})

	t.Run("records true false positive for Failed to Alive transition", func(t *testing.T) {
		config := DefaultConfig()
		config.NodeID = "node-1"
		config.BindAddr = "127.0.0.1:8000"
		config.AdvertiseAddr = "127.0.0.1:8000"

		mockTransport := newTransportMock()
		mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

		memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
		memberList.AddMember("node-2", "127.0.0.1:8001", Alive, 2) // Updated to Alive

		swim := &SWIM{
			config:     config,
			memberList: memberList,
			gossip:     NewGossipManager(2, 1400),
			transport:  mockTransport,
			metrics:    NewMetrics(),
		}

		update := Update{
			MemberID:    "node-2",
			Status:      Alive,
			Incarnation: 2,
		}

		swim.handleMembershipUpdate(update, Failed)

		assert.Equal(t, uint64(1), swim.metrics.falseFailureCount.Load())
	})
}

func TestSWIM_HandleSuspicionTimeout(t *testing.T) {
	t.Run("marks suspect as failed on timeout", func(t *testing.T) {
		config := DefaultConfig()
		config.NodeID = "node-1"
		config.BindAddr = "127.0.0.1:8000"
		config.AdvertiseAddr = "127.0.0.1:8000"

		mockTransport := newTransportMock()
		mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

		memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
		memberList.AddMember("node-2", "127.0.0.1:8001", Suspect, 1)

		swim := &SWIM{
			config:     config,
			memberList: memberList,
			gossip:     NewGossipManager(2, 1400),
			transport:  mockTransport,
			metrics:    NewMetrics(),
		}

		var failedCallbackCalled bool
		swim.OnMemberFailed(func(m *Member) {
			failedCallbackCalled = true
		})

		swim.handleSuspicionTimeout("node-2")

		member, _ := memberList.GetMember("node-2")
		assert.Equal(t, Failed, member.Status)
		assert.True(t, failedCallbackCalled)
	})

	t.Run("does nothing if member not found", func(t *testing.T) {
		config := DefaultConfig()
		config.NodeID = "node-1"
		config.BindAddr = "127.0.0.1:8000"
		config.AdvertiseAddr = "127.0.0.1:8000"

		swim := &SWIM{
			config:     config,
			memberList: NewMemberList(config.NodeID, config.AdvertiseAddr),
			gossip:     NewGossipManager(2, 1400),
			transport:  newTransportMock(),
			metrics:    NewMetrics(),
		}

		// Should not panic
		assert.NotPanics(t, func() {
			swim.handleSuspicionTimeout("non-existent")
		})
	})

	t.Run("does nothing if member no longer suspect", func(t *testing.T) {
		config := DefaultConfig()
		config.NodeID = "node-1"
		config.BindAddr = "127.0.0.1:8000"
		config.AdvertiseAddr = "127.0.0.1:8000"

		memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
		memberList.AddMember("node-2", "127.0.0.1:8001", Alive, 2) // Already alive

		swim := &SWIM{
			config:     config,
			memberList: memberList,
			gossip:     NewGossipManager(2, 1400),
			transport:  newTransportMock(),
			metrics:    NewMetrics(),
		}

		var failedCallbackCalled bool
		swim.OnMemberFailed(func(m *Member) {
			failedCallbackCalled = true
		})

		swim.handleSuspicionTimeout("node-2")

		member, _ := memberList.GetMember("node-2")
		assert.Equal(t, Alive, member.Status)
		assert.False(t, failedCallbackCalled)
	})
}

func TestSWIM_HandleSuspicion_WhenStopped(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-2", "127.0.0.1:8001", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  newTransportMock(),
		metrics:    NewMetrics(),
		stopped:    true, // Already stopped
	}

	member, _ := memberList.GetMember("node-2")
	swim.handleSuspicion(member)

	// Should not record suspicion when stopped
	assert.Equal(t, uint64(0), swim.metrics.GetSuspicionCount())
}

func TestSWIM_HandleMessage_AllTypes(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.ProbeTimeout = 100 * time.Millisecond
	config.ProbeInterval = 500 * time.Millisecond

	mockTransport := newTransportMock()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)
	memberList.AddMember("node-2", "127.0.0.1:8001", Alive, 1)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     NewGossipManager(2, 1400),
		transport:  mockTransport,
		metrics:    NewMetrics(),
		shutdownCh: make(chan struct{}),
	}
	swim.probe = NewProbeScheduler(swim)

	messageTypes := []MessageType{
		PingMsg, AckMsg, PingReqMsg, IndirectPingMsg, IndirectAckMsg,
		SuspectMsg, AliveMsg, ConfirmMsg, LeaveMsg, JoinMsg, SyncMsg,
	}

	for _, msgType := range messageTypes {
		msg := &Message{
			Type:        msgType,
			From:        "node-2",
			FromAddr:    "127.0.0.1:8001",
			Target:      "node-2",
			TargetAddr:  "127.0.0.1:8001",
			Incarnation: 1,
		}

		assert.NotPanics(t, func() {
			swim.handleMessage(msg)
		}, "handleMessage should not panic for type %s", msgType.String())
	}
}
