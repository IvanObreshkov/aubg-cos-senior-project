package tob

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMessageType_String(t *testing.T) {
	tests := []struct {
		name     string
		msgType  MessageType
		expected string
	}{
		{"Data message", DataMsg, "Data"},
		{"Sequenced message", SequencedMsg, "Sequenced"},
		{"Heartbeat message", HeartbeatMsg, "Heartbeat"},
		{"Unknown message", MessageType(99), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.msgType.String())
		})
	}
}

func TestDeliveryStatus_String(t *testing.T) {
	tests := []struct {
		name     string
		status   DeliveryStatus
		expected string
	}{
		{"Pending status", Pending, "Pending"},
		{"Sequenced status", Sequenced, "Sequenced"},
		{"Delivered status", Delivered, "Delivered"},
		{"Unknown status", DeliveryStatus(99), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.status.String())
		})
	}
}

func TestMessage_Fields(t *testing.T) {
	now := time.Now()
	payload := []byte("test payload")

	msg := &Message{
		Type:           DataMsg,
		From:           "node-1",
		FromAddr:       "127.0.0.1:8000",
		SequenceNumber: 42,
		MessageID:      "msg-123",
		Payload:        payload,
		Timestamp:      now,
	}

	assert.Equal(t, DataMsg, msg.Type)
	assert.Equal(t, "node-1", msg.From)
	assert.Equal(t, "127.0.0.1:8000", msg.FromAddr)
	assert.Equal(t, uint64(42), msg.SequenceNumber)
	assert.Equal(t, "msg-123", msg.MessageID)
	assert.Equal(t, payload, msg.Payload)
	assert.Equal(t, now, msg.Timestamp)
}

func TestPendingMessage_Fields(t *testing.T) {
	now := time.Now()
	msg := &Message{
		MessageID: "msg-123",
	}

	pending := &PendingMessage{
		Message:     msg,
		Status:      Pending,
		ReceivedAt:  now,
		SequencedAt: now.Add(1 * time.Second),
		DeliveredAt: now.Add(2 * time.Second),
	}

	assert.Equal(t, msg, pending.Message)
	assert.Equal(t, Pending, pending.Status)
	assert.Equal(t, now, pending.ReceivedAt)
	assert.Equal(t, now.Add(1*time.Second), pending.SequencedAt)
	assert.Equal(t, now.Add(2*time.Second), pending.DeliveredAt)
}

func TestPendingMessage_Mutex(t *testing.T) {
	pending := &PendingMessage{
		Status: Pending,
	}

	// Test RLock and RUnlock
	pending.mu.RLock()
	status := pending.Status
	pending.mu.RUnlock()
	assert.Equal(t, Pending, status)

	// Test Lock and Unlock
	pending.mu.Lock()
	pending.Status = Sequenced
	pending.mu.Unlock()
	assert.Equal(t, Sequenced, pending.Status)
}

func TestConfig_Fields(t *testing.T) {
	logger := &defaultLogger{}
	config := &Config{
		NodeID:             "node-1",
		BindAddr:           "127.0.0.1:8000",
		AdvertiseAddr:      "127.0.0.1:8000",
		IsSequencer:        true,
		SequencerAddr:      "127.0.0.1:8001",
		SequencerID:        "sequencer-1",
		Nodes:              []string{"127.0.0.1:8000", "127.0.0.1:8001"},
		SequencerTimeout:   5 * time.Second,
		HeartbeatInterval:  1 * time.Second,
		DeliveryBufferSize: 1000,
		MessageTimeout:     10 * time.Second,
		Logger:             logger,
	}

	assert.Equal(t, "node-1", config.NodeID)
	assert.Equal(t, "127.0.0.1:8000", config.BindAddr)
	assert.Equal(t, "127.0.0.1:8000", config.AdvertiseAddr)
	assert.True(t, config.IsSequencer)
	assert.Equal(t, "127.0.0.1:8001", config.SequencerAddr)
	assert.Equal(t, "sequencer-1", config.SequencerID)
	assert.Equal(t, []string{"127.0.0.1:8000", "127.0.0.1:8001"}, config.Nodes)
	assert.Equal(t, 5*time.Second, config.SequencerTimeout)
	assert.Equal(t, 1*time.Second, config.HeartbeatInterval)
	assert.Equal(t, 1000, config.DeliveryBufferSize)
	assert.Equal(t, 10*time.Second, config.MessageTimeout)
	assert.Equal(t, logger, config.Logger)
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	assert.NotNil(t, config)
	assert.Equal(t, 5*time.Second, config.SequencerTimeout)
	assert.Equal(t, 1*time.Second, config.HeartbeatInterval)
	assert.Equal(t, 1000, config.DeliveryBufferSize)
	assert.Equal(t, 10*time.Second, config.MessageTimeout)
	assert.NotNil(t, config.Logger)
}

func TestDefaultLogger(t *testing.T) {
	logger := &defaultLogger{}

	// These should not panic
	t.Run("Debugf does not panic", func(t *testing.T) {
		assert.NotPanics(t, func() {
			logger.Debugf("test %s", "message")
		})
	})

	t.Run("Infof does not panic", func(t *testing.T) {
		assert.NotPanics(t, func() {
			logger.Infof("test %s", "message")
		})
	})

	t.Run("Warnf does not panic", func(t *testing.T) {
		assert.NotPanics(t, func() {
			logger.Warnf("test %s", "message")
		})
	})

	t.Run("Errorf does not panic", func(t *testing.T) {
		assert.NotPanics(t, func() {
			logger.Errorf("test %s", "message")
		})
	})
}

func TestMessageType_Constants(t *testing.T) {
	// Verify the iota order
	assert.Equal(t, MessageType(0), DataMsg)
	assert.Equal(t, MessageType(1), SequencedMsg)
	assert.Equal(t, MessageType(2), HeartbeatMsg)
}

func TestDeliveryStatus_Constants(t *testing.T) {
	// Verify the iota order
	assert.Equal(t, DeliveryStatus(0), Pending)
	assert.Equal(t, DeliveryStatus(1), Sequenced)
	assert.Equal(t, DeliveryStatus(2), Delivered)
}

func TestDeliveryCallback_Type(t *testing.T) {
	var callbackCalled bool
	var receivedMsg *Message

	callback := DeliveryCallback(func(msg *Message) {
		callbackCalled = true
		receivedMsg = msg
	})

	testMsg := &Message{
		MessageID: "test-msg",
	}

	callback(testMsg)

	assert.True(t, callbackCalled)
	assert.Equal(t, testMsg, receivedMsg)
}
