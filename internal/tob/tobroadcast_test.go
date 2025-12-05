package tob

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// mockTransportForTOB is used for testing tobroadcast.go
type mockTransportForTOB struct {
	mock.Mock
	mu             sync.RWMutex
	messageHandler func(*Message)
	sentMessages   []*Message
	startCalled    bool
	stopCalled     bool
}

func newMockTransportForTOB() *mockTransportForTOB {
	return &mockTransportForTOB{
		sentMessages: make([]*Message, 0),
	}
}

func (m *mockTransportForTOB) Start() error {
	m.startCalled = true
	args := m.Called()
	return args.Error(0)
}

func (m *mockTransportForTOB) Stop() error {
	m.stopCalled = true
	args := m.Called()
	return args.Error(0)
}

func (m *mockTransportForTOB) SendMessage(targetAddr string, msg *Message) error {
	m.mu.Lock()
	m.sentMessages = append(m.sentMessages, msg)
	m.mu.Unlock()
	args := m.Called(targetAddr, msg)
	return args.Error(0)
}

func (m *mockTransportForTOB) SetMessageHandler(handler func(*Message)) {
	m.mu.Lock()
	m.messageHandler = handler
	m.mu.Unlock()
}

func (m *mockTransportForTOB) getSentMessages() []*Message {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*Message, len(m.sentMessages))
	copy(result, m.sentMessages)
	return result
}

func (m *mockTransportForTOB) simulateMessage(msg *Message) {
	m.mu.RLock()
	handler := m.messageHandler
	m.mu.RUnlock()
	if handler != nil {
		handler(msg)
	}
}

func TestValidateConfig(t *testing.T) {
	t.Run("valid sequencer config", func(t *testing.T) {
		config := &Config{
			NodeID:        "node-1",
			BindAddr:      "127.0.0.1:8000",
			AdvertiseAddr: "127.0.0.1:8000",
			IsSequencer:   true,
			Nodes:         []string{"127.0.0.1:8000"},
		}
		err := validateConfig(config)
		assert.NoError(t, err)
	})

	t.Run("valid non-sequencer config", func(t *testing.T) {
		config := &Config{
			NodeID:        "node-1",
			BindAddr:      "127.0.0.1:8000",
			AdvertiseAddr: "127.0.0.1:8000",
			IsSequencer:   false,
			SequencerAddr: "127.0.0.1:8001",
			Nodes:         []string{"127.0.0.1:8000", "127.0.0.1:8001"},
		}
		err := validateConfig(config)
		assert.NoError(t, err)
	})

	t.Run("missing NodeID", func(t *testing.T) {
		config := &Config{
			BindAddr:      "127.0.0.1:8000",
			AdvertiseAddr: "127.0.0.1:8000",
			Nodes:         []string{"127.0.0.1:8000"},
		}
		err := validateConfig(config)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidConfig)
	})

	t.Run("missing BindAddr", func(t *testing.T) {
		config := &Config{
			NodeID:        "node-1",
			AdvertiseAddr: "127.0.0.1:8000",
			Nodes:         []string{"127.0.0.1:8000"},
		}
		err := validateConfig(config)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidConfig)
	})

	t.Run("missing AdvertiseAddr", func(t *testing.T) {
		config := &Config{
			NodeID:   "node-1",
			BindAddr: "127.0.0.1:8000",
			Nodes:    []string{"127.0.0.1:8000"},
		}
		err := validateConfig(config)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidConfig)
	})

	t.Run("missing SequencerAddr for non-sequencer", func(t *testing.T) {
		config := &Config{
			NodeID:        "node-1",
			BindAddr:      "127.0.0.1:8000",
			AdvertiseAddr: "127.0.0.1:8000",
			IsSequencer:   false,
			Nodes:         []string{"127.0.0.1:8000"},
		}
		err := validateConfig(config)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidConfig)
	})

	t.Run("missing Nodes", func(t *testing.T) {
		config := &Config{
			NodeID:        "node-1",
			BindAddr:      "127.0.0.1:8000",
			AdvertiseAddr: "127.0.0.1:8000",
			IsSequencer:   true,
		}
		err := validateConfig(config)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidConfig)
	})
}

func TestNew(t *testing.T) {
	t.Run("valid config creates TOBroadcast", func(t *testing.T) {
		config := DefaultConfig()
		config.NodeID = "node-1"
		config.BindAddr = "127.0.0.1:0"
		config.AdvertiseAddr = "127.0.0.1:0"
		config.IsSequencer = true
		config.Nodes = []string{"127.0.0.1:0"}

		tob, err := New(config)
		assert.NoError(t, err)
		assert.NotNil(t, tob)
		assert.Equal(t, config, tob.config)
		assert.NotNil(t, tob.transport)
		assert.NotNil(t, tob.delivery)
		assert.NotNil(t, tob.sequencer) // Sequencer node
		assert.NotNil(t, tob.metrics)
	})

	t.Run("non-sequencer does not create sequencer", func(t *testing.T) {
		config := DefaultConfig()
		config.NodeID = "node-1"
		config.BindAddr = "127.0.0.1:0"
		config.AdvertiseAddr = "127.0.0.1:0"
		config.IsSequencer = false
		config.SequencerAddr = "127.0.0.1:8001"
		config.Nodes = []string{"127.0.0.1:0", "127.0.0.1:8001"}

		tob, err := New(config)
		assert.NoError(t, err)
		assert.NotNil(t, tob)
		assert.Nil(t, tob.sequencer)
	})

	t.Run("invalid config returns error", func(t *testing.T) {
		config := &Config{}
		tob, err := New(config)
		assert.Error(t, err)
		assert.Nil(t, tob)
	})
}

func TestTOBroadcast_IsSequencer(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:0"
	config.AdvertiseAddr = "127.0.0.1:0"
	config.IsSequencer = true
	config.Nodes = []string{"127.0.0.1:0"}

	tob := &TOBroadcast{
		config:      config,
		isSequencer: true,
	}

	assert.True(t, tob.IsSequencer())

	tob.isSequencer = false
	assert.False(t, tob.IsSequencer())
}

func TestTOBroadcast_GetCurrentSequencer(t *testing.T) {
	tob := &TOBroadcast{
		currentSequencer: "sequencer-1",
	}

	assert.Equal(t, "sequencer-1", tob.GetCurrentSequencer())
}

func TestTOBroadcast_SetDeliveryCallback(t *testing.T) {
	tob := &TOBroadcast{}

	var callbackCalled bool
	callback := func(msg *Message) {
		callbackCalled = true
	}

	tob.SetDeliveryCallback(callback)

	assert.NotNil(t, tob.deliveryCallback)
	tob.deliveryCallback(nil)
	assert.True(t, callbackCalled)
}

func TestTOBroadcast_GetSetMetrics(t *testing.T) {
	tob := &TOBroadcast{
		metrics: NewMetrics(),
	}

	originalMetrics := tob.GetMetrics()
	assert.NotNil(t, originalMetrics)

	newMetrics := NewMetrics()
	tob.SetMetrics(newMetrics)
	assert.Equal(t, newMetrics, tob.GetMetrics())
}

func TestTOBroadcast_GetNextExpectedSequence(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:0"
	config.AdvertiseAddr = "127.0.0.1:0"
	config.IsSequencer = true
	config.Nodes = []string{"127.0.0.1:0"}

	tob := &TOBroadcast{
		config:  config,
		metrics: NewMetrics(),
	}
	tob.delivery = NewDeliveryManager(tob)

	assert.Equal(t, uint64(1), tob.GetNextExpectedSequence())
}

func TestTOBroadcast_GetPendingMessageCount(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:0"
	config.AdvertiseAddr = "127.0.0.1:0"
	config.IsSequencer = true
	config.Nodes = []string{"127.0.0.1:0"}

	tob := &TOBroadcast{
		config:  config,
		metrics: NewMetrics(),
	}
	tob.delivery = NewDeliveryManager(tob)

	assert.Equal(t, 0, tob.GetPendingMessageCount())
}

func TestTOBroadcast_Broadcast_NotStarted(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:0"
	config.AdvertiseAddr = "127.0.0.1:0"
	config.IsSequencer = true
	config.Nodes = []string{"127.0.0.1:0"}

	tob := &TOBroadcast{
		config:  config,
		started: false,
		metrics: NewMetrics(),
	}

	err := tob.Broadcast([]byte("test"))
	assert.Equal(t, ErrNotStarted, err)
}

func TestTOBroadcast_Broadcast_AsSequencer(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:0"
	config.AdvertiseAddr = "127.0.0.1:0"
	config.IsSequencer = true
	config.Nodes = []string{"127.0.0.1:0"}

	mockTransport := newMockTransportForTOB()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	tob := &TOBroadcast{
		config:      config,
		transport:   mockTransport,
		isSequencer: true,
		started:     true,
		metrics:     NewMetrics(),
	}
	tob.delivery = NewDeliveryManager(tob)
	tob.sequencer = NewSequencer(tob)
	tob.sequencer.Start()
	defer tob.sequencer.Stop()

	err := tob.Broadcast([]byte("test payload"))
	assert.NoError(t, err)

	// Wait for processing
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, uint64(1), tob.metrics.dataMsgCount.Load())
}

func TestTOBroadcast_Broadcast_AsNonSequencer(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.IsSequencer = false
	config.SequencerAddr = "127.0.0.1:8001"
	config.Nodes = []string{"127.0.0.1:8000", "127.0.0.1:8001"}
	config.SequencerTimeout = 1 * time.Second

	mockTransport := newMockTransportForTOB()
	mockTransport.On("SendMessage", "127.0.0.1:8001", mock.Anything).Return(nil)

	tob := &TOBroadcast{
		config:        config,
		transport:     mockTransport,
		isSequencer:   false,
		sequencerAddr: "127.0.0.1:8001",
		started:       true,
		lastHeartbeat: time.Now(), // Make sequencer appear reachable
		metrics:       NewMetrics(),
	}
	tob.delivery = NewDeliveryManager(tob)

	err := tob.Broadcast([]byte("test payload"))
	assert.NoError(t, err)

	sentMessages := mockTransport.getSentMessages()
	assert.Len(t, sentMessages, 1)
	assert.Equal(t, DataMsg, sentMessages[0].Type)
}

func TestTOBroadcast_Broadcast_NoSequencerAddr(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.SequencerTimeout = 1 * time.Second

	tob := &TOBroadcast{
		config:        config,
		isSequencer:   false,
		sequencerAddr: "",
		started:       true,
		lastHeartbeat: time.Now(),
		metrics:       NewMetrics(),
	}

	err := tob.Broadcast([]byte("test"))
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrNoSequencer)
}

func TestTOBroadcast_Broadcast_SequencerUnreachable(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.SequencerTimeout = 100 * time.Millisecond

	tob := &TOBroadcast{
		config:        config,
		isSequencer:   false,
		sequencerAddr: "127.0.0.1:8001",
		started:       true,
		lastHeartbeat: time.Now().Add(-1 * time.Hour), // Old heartbeat
		metrics:       NewMetrics(),
	}

	err := tob.Broadcast([]byte("test"))
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrNoSequencer)
}

func TestTOBroadcast_IsSequencerReachable(t *testing.T) {
	config := DefaultConfig()
	config.SequencerTimeout = 100 * time.Millisecond

	t.Run("sequencer is always reachable", func(t *testing.T) {
		tob := &TOBroadcast{
			config:      config,
			isSequencer: true,
		}
		assert.True(t, tob.IsSequencerReachable())
	})

	t.Run("reachable when heartbeat is recent", func(t *testing.T) {
		tob := &TOBroadcast{
			config:        config,
			isSequencer:   false,
			lastHeartbeat: time.Now(),
		}
		assert.True(t, tob.IsSequencerReachable())
	})

	t.Run("unreachable when heartbeat is old", func(t *testing.T) {
		tob := &TOBroadcast{
			config:        config,
			isSequencer:   false,
			lastHeartbeat: time.Now().Add(-1 * time.Hour),
		}
		assert.False(t, tob.IsSequencerReachable())
	})
}

func TestTOBroadcast_HandleMessage(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:0"
	config.AdvertiseAddr = "127.0.0.1:0"
	config.IsSequencer = true
	config.Nodes = []string{"127.0.0.1:0"}

	mockTransport := newMockTransportForTOB()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	tob := &TOBroadcast{
		config:      config,
		transport:   mockTransport,
		isSequencer: true,
		started:     true,
		metrics:     NewMetrics(),
	}
	tob.delivery = NewDeliveryManager(tob)
	tob.sequencer = NewSequencer(tob)
	tob.sequencer.Start()
	defer tob.sequencer.Stop()

	t.Run("handles data message as sequencer", func(t *testing.T) {
		msg := &Message{
			Type:      DataMsg,
			From:      "client",
			MessageID: "msg-1",
			Timestamp: time.Now(),
		}

		tob.handleMessage(msg)

		// Wait for processing
		time.Sleep(50 * time.Millisecond)
	})

	t.Run("handles sequenced message", func(t *testing.T) {
		var deliveredMsg *Message
		tob.deliveryCallback = func(msg *Message) {
			deliveredMsg = msg
		}

		msg := &Message{
			Type:           SequencedMsg,
			From:           "sequencer",
			SequenceNumber: tob.delivery.GetNextExpectedSeq(),
			MessageID:      "msg-2",
			Timestamp:      time.Now(),
		}

		tob.handleMessage(msg)

		assert.NotNil(t, deliveredMsg)
	})

	t.Run("handles heartbeat message", func(t *testing.T) {
		tob.lastHeartbeat = time.Time{} // Reset

		msg := &Message{
			Type:      HeartbeatMsg,
			From:      "sequencer",
			Timestamp: time.Now(),
		}

		tob.handleMessage(msg)

		tob.heartbeatMu.RLock()
		assert.False(t, tob.lastHeartbeat.IsZero())
		tob.heartbeatMu.RUnlock()
	})

	t.Run("handles unknown message type", func(t *testing.T) {
		msg := &Message{
			Type: MessageType(99),
		}

		// Should not panic
		assert.NotPanics(t, func() {
			tob.handleMessage(msg)
		})
	})
}

func TestTOBroadcast_HandleDataMessage_NotSequencer(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"

	tob := &TOBroadcast{
		config:      config,
		isSequencer: false,
		metrics:     NewMetrics(),
	}

	msg := &Message{
		Type:      DataMsg,
		MessageID: "msg-1",
	}

	// Should not panic, just log warning
	assert.NotPanics(t, func() {
		tob.handleDataMessage(msg)
	})
}

func TestTOBroadcast_MulticastSequencedMessage(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.IsSequencer = true
	config.Nodes = []string{"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}

	mockTransport := newMockTransportForTOB()
	mockTransport.On("SendMessage", "127.0.0.1:8001", mock.Anything).Return(nil)
	mockTransport.On("SendMessage", "127.0.0.1:8002", mock.Anything).Return(nil)

	tob := &TOBroadcast{
		config:      config,
		transport:   mockTransport,
		isSequencer: true,
		metrics:     NewMetrics(),
	}
	tob.delivery = NewDeliveryManager(tob)

	msg := &Message{
		Type:           SequencedMsg,
		SequenceNumber: 1,
		MessageID:      "msg-1",
		Timestamp:      time.Now(),
	}

	tob.multicastSequencedMessage(msg)

	sentMessages := mockTransport.getSentMessages()
	// Should send to 2 other nodes (not self)
	assert.Equal(t, 2, len(sentMessages))
}

func TestTOBroadcast_MulticastSequencedMessage_SendError(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "node-1"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.IsSequencer = true
	config.Nodes = []string{"127.0.0.1:8000", "127.0.0.1:8001"}

	mockTransport := newMockTransportForTOB()
	mockTransport.On("SendMessage", "127.0.0.1:8001", mock.Anything).Return(assert.AnError)

	tob := &TOBroadcast{
		config:      config,
		transport:   mockTransport,
		isSequencer: true,
		metrics:     NewMetrics(),
	}
	tob.delivery = NewDeliveryManager(tob)

	msg := &Message{
		Type:           SequencedMsg,
		SequenceNumber: 1,
		MessageID:      "msg-1",
		Timestamp:      time.Now(),
	}

	// Should not panic even if send fails
	assert.NotPanics(t, func() {
		tob.multicastSequencedMessage(msg)
	})
}

func TestTOBroadcast_CheckSequencerHealth(t *testing.T) {
	config := DefaultConfig()
	config.SequencerTimeout = 100 * time.Millisecond

	t.Run("healthy sequencer", func(t *testing.T) {
		tob := &TOBroadcast{
			config:        config,
			lastHeartbeat: time.Now(),
		}

		// Should not panic
		assert.NotPanics(t, func() {
			tob.checkSequencerHealth()
		})
	})

	t.Run("unhealthy sequencer", func(t *testing.T) {
		tob := &TOBroadcast{
			config:        config,
			lastHeartbeat: time.Now().Add(-1 * time.Hour),
		}

		// Should not panic, just log error
		assert.NotPanics(t, func() {
			tob.checkSequencerHealth()
		})
	})
}

func TestTOBroadcast_Stop_NotStarted(t *testing.T) {
	tob := &TOBroadcast{
		started: false,
	}

	err := tob.Stop()
	assert.NoError(t, err)
}

func TestErrors(t *testing.T) {
	assert.Error(t, ErrNotStarted)
	assert.Error(t, ErrSequencerStopped)
	assert.Error(t, ErrQueueFull)
	assert.Error(t, ErrNoSequencer)
	assert.Error(t, ErrInvalidConfig)

	assert.Equal(t, "TOBroadcast not started", ErrNotStarted.Error())
	assert.Equal(t, "sequencer stopped", ErrSequencerStopped.Error())
	assert.Equal(t, "message queue full", ErrQueueFull.Error())
	assert.Equal(t, "no sequencer available", ErrNoSequencer.Error())
	assert.Equal(t, "invalid configuration", ErrInvalidConfig.Error())
}
