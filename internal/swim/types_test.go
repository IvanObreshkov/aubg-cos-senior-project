package swim

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMemberStatus_String(t *testing.T) {
	tests := []struct {
		name     string
		status   MemberStatus
		expected string
	}{
		{"Alive status", Alive, "Alive"},
		{"Suspect status", Suspect, "Suspect"},
		{"Failed status", Failed, "Failed"},
		{"Left status", Left, "Left"},
		{"Unknown status", MemberStatus(99), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.status.String())
		})
	}
}

func TestMessageType_String(t *testing.T) {
	tests := []struct {
		name     string
		msgType  MessageType
		expected string
	}{
		{"Ping message", PingMsg, "Ping"},
		{"Ack message", AckMsg, "Ack"},
		{"PingReq message", PingReqMsg, "PingReq"},
		{"IndirectPing message", IndirectPingMsg, "IndirectPing"},
		{"IndirectAck message", IndirectAckMsg, "IndirectAck"},
		{"Suspect message", SuspectMsg, "Suspect"},
		{"Alive message", AliveMsg, "Alive"},
		{"Confirm message", ConfirmMsg, "Confirm"},
		{"Leave message", LeaveMsg, "Leave"},
		{"Join message", JoinMsg, "Join"},
		{"Sync message", SyncMsg, "Sync"},
		{"Unknown message", MessageType(99), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.msgType.String())
		})
	}
}

func TestMember_Fields(t *testing.T) {
	now := time.Now()
	member := &Member{
		ID:          "node-1",
		Address:     "127.0.0.1:8000",
		Status:      Alive,
		Incarnation: 5,
		LocalTime:   now,
	}

	assert.Equal(t, "node-1", member.ID)
	assert.Equal(t, "127.0.0.1:8000", member.Address)
	assert.Equal(t, Alive, member.Status)
	assert.Equal(t, uint64(5), member.Incarnation)
	assert.Equal(t, now, member.LocalTime)
}

func TestMessage_Fields(t *testing.T) {
	piggyback := []Update{
		{MemberID: "node-2", Status: Alive},
	}

	msg := &Message{
		Type:        PingMsg,
		From:        "node-1",
		FromAddr:    "127.0.0.1:8000",
		Target:      "node-2",
		TargetAddr:  "127.0.0.1:8001",
		SeqNo:       42,
		Incarnation: 3,
		Piggyback:   piggyback,
	}

	assert.Equal(t, PingMsg, msg.Type)
	assert.Equal(t, "node-1", msg.From)
	assert.Equal(t, "127.0.0.1:8000", msg.FromAddr)
	assert.Equal(t, "node-2", msg.Target)
	assert.Equal(t, "127.0.0.1:8001", msg.TargetAddr)
	assert.Equal(t, uint64(42), msg.SeqNo)
	assert.Equal(t, uint64(3), msg.Incarnation)
	assert.Len(t, msg.Piggyback, 1)
}

func TestUpdate_Fields(t *testing.T) {
	now := time.Now()
	update := Update{
		MemberID:    "node-1",
		Address:     "127.0.0.1:8000",
		Status:      Suspect,
		Incarnation: 7,
		Timestamp:   now,
	}

	assert.Equal(t, "node-1", update.MemberID)
	assert.Equal(t, "127.0.0.1:8000", update.Address)
	assert.Equal(t, Suspect, update.Status)
	assert.Equal(t, uint64(7), update.Incarnation)
	assert.Equal(t, now, update.Timestamp)
}

func TestConfig_Fields(t *testing.T) {
	logger := &defaultLogger{}
	config := &Config{
		BindAddr:                 "127.0.0.1:8000",
		AdvertiseAddr:            "127.0.0.1:8000",
		NodeID:                   "node-1",
		ProtocolPeriod:           1 * time.Second,
		ProbeTimeout:             500 * time.Millisecond,
		ProbeInterval:            1 * time.Second,
		IndirectProbeCount:       3,
		SuspicionTimeout:         5 * time.Second,
		SuspicionMultiplier:      4,
		MaxGossipPacketSize:      1400,
		NumGossipRetransmissions: 2,
		GossipFanout:             3,
		EnableSuspicionMechanism: true,
		JoinNodes:                []string{"127.0.0.1:8001"},
		Logger:                   logger,
	}

	assert.Equal(t, "127.0.0.1:8000", config.BindAddr)
	assert.Equal(t, "127.0.0.1:8000", config.AdvertiseAddr)
	assert.Equal(t, "node-1", config.NodeID)
	assert.Equal(t, 1*time.Second, config.ProtocolPeriod)
	assert.Equal(t, 500*time.Millisecond, config.ProbeTimeout)
	assert.Equal(t, 1*time.Second, config.ProbeInterval)
	assert.Equal(t, 3, config.IndirectProbeCount)
	assert.Equal(t, 5*time.Second, config.SuspicionTimeout)
	assert.Equal(t, 4, config.SuspicionMultiplier)
	assert.Equal(t, 1400, config.MaxGossipPacketSize)
	assert.Equal(t, 2, config.NumGossipRetransmissions)
	assert.Equal(t, 3, config.GossipFanout)
	assert.True(t, config.EnableSuspicionMechanism)
	assert.Equal(t, []string{"127.0.0.1:8001"}, config.JoinNodes)
	assert.Equal(t, logger, config.Logger)
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	assert.NotNil(t, config)
	assert.Equal(t, 1*time.Second, config.ProtocolPeriod)
	assert.Equal(t, 500*time.Millisecond, config.ProbeTimeout)
	assert.Equal(t, 1*time.Second, config.ProbeInterval)
	assert.Equal(t, 3, config.IndirectProbeCount)
	assert.Equal(t, 5*time.Second, config.SuspicionTimeout)
	assert.Equal(t, 4, config.SuspicionMultiplier)
	assert.Equal(t, 1400, config.MaxGossipPacketSize)
	assert.Equal(t, 2, config.NumGossipRetransmissions)
	assert.Equal(t, 3, config.GossipFanout)
	assert.True(t, config.EnableSuspicionMechanism)
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

func TestEventType_Values(t *testing.T) {
	// Test that event types have distinct values
	assert.NotEqual(t, MemberJoinEvent, MemberLeaveEvent)
	assert.NotEqual(t, MemberLeaveEvent, MemberFailedEvent)
	assert.NotEqual(t, MemberFailedEvent, MemberUpdateEvent)
	assert.NotEqual(t, MemberJoinEvent, MemberUpdateEvent)
}

func TestMemberEventPayload(t *testing.T) {
	member := &Member{
		ID:      "node-1",
		Address: "127.0.0.1:8000",
		Status:  Suspect,
	}

	payload := MemberEventPayload{
		Member:    member,
		OldStatus: Alive,
		NewStatus: Suspect,
	}

	assert.Equal(t, member, payload.Member)
	assert.Equal(t, Alive, payload.OldStatus)
	assert.Equal(t, Suspect, payload.NewStatus)
}

func TestMemberStatus_Constants(t *testing.T) {
	// Verify the iota order
	assert.Equal(t, MemberStatus(0), Alive)
	assert.Equal(t, MemberStatus(1), Suspect)
	assert.Equal(t, MemberStatus(2), Failed)
	assert.Equal(t, MemberStatus(3), Left)
}

func TestMessageType_Constants(t *testing.T) {
	// Verify the iota order
	assert.Equal(t, MessageType(0), PingMsg)
	assert.Equal(t, MessageType(1), AckMsg)
	assert.Equal(t, MessageType(2), PingReqMsg)
	assert.Equal(t, MessageType(3), IndirectPingMsg)
	assert.Equal(t, MessageType(4), IndirectAckMsg)
	assert.Equal(t, MessageType(5), SuspectMsg)
	assert.Equal(t, MessageType(6), AliveMsg)
	assert.Equal(t, MessageType(7), ConfirmMsg)
	assert.Equal(t, MessageType(8), LeaveMsg)
	assert.Equal(t, MessageType(9), JoinMsg)
	assert.Equal(t, MessageType(10), SyncMsg)
}

func TestEventType_Constants(t *testing.T) {
	// Verify the iota order
	assert.Equal(t, EventType(0), MemberJoinEvent)
	assert.Equal(t, EventType(1), MemberLeaveEvent)
	assert.Equal(t, EventType(2), MemberFailedEvent)
	assert.Equal(t, EventType(3), MemberUpdateEvent)
}

func TestMember_MutexOperations(t *testing.T) {
	member := &Member{
		ID:      "node-1",
		Address: "127.0.0.1:8000",
		Status:  Alive,
	}

	// Test that mutex operations work correctly
	t.Run("RLock and RUnlock", func(t *testing.T) {
		member.mu.RLock()
		status := member.Status
		member.mu.RUnlock()
		assert.Equal(t, Alive, status)
	})

	t.Run("Lock and Unlock", func(t *testing.T) {
		member.mu.Lock()
		member.Status = Suspect
		member.mu.Unlock()
		assert.Equal(t, Suspect, member.Status)
	})
}
