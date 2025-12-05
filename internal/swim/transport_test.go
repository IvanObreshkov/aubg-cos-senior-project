package swim

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewUDPTransport(t *testing.T) {
	logger := &defaultLogger{}
	transport := NewUDPTransport("127.0.0.1:0", logger)

	assert.NotNil(t, transport)
	assert.Equal(t, "127.0.0.1:0", transport.bindAddr)
	assert.NotNil(t, transport.shutdownCh)
	assert.Equal(t, logger, transport.logger)
}

func TestUDPTransport_StartStop(t *testing.T) {
	logger := &defaultLogger{}
	transport := NewUDPTransport("127.0.0.1:0", logger)

	err := transport.Start()
	assert.NoError(t, err)
	assert.NotNil(t, transport.conn)

	err = transport.Stop()
	assert.NoError(t, err)
}

func TestUDPTransport_Start_InvalidAddress(t *testing.T) {
	logger := &defaultLogger{}
	transport := NewUDPTransport("invalid-address", logger)

	err := transport.Start()
	assert.Error(t, err)
}

func TestUDPTransport_SetMessageHandler(t *testing.T) {
	logger := &defaultLogger{}
	transport := NewUDPTransport("127.0.0.1:0", logger)

	handler := func(msg *Message) {
		// Handler is set
	}

	transport.SetMessageHandler(handler)

	assert.NotNil(t, transport.messageHandler)
}

func TestUDPTransport_SendMessage(t *testing.T) {
	logger := &defaultLogger{}

	// Create receiver
	receiver := NewUDPTransport("127.0.0.1:0", logger)
	err := receiver.Start()
	assert.NoError(t, err)
	defer receiver.Stop()

	// Get actual bound port
	receiverAddr := receiver.conn.LocalAddr().String()

	// Create sender
	sender := NewUDPTransport("127.0.0.1:0", logger)
	err = sender.Start()
	assert.NoError(t, err)
	defer sender.Stop()

	msg := &Message{
		Type:     PingMsg,
		From:     "sender",
		FromAddr: "127.0.0.1:9000",
		SeqNo:    1,
	}

	err = sender.SendMessage(receiverAddr, msg)
	assert.NoError(t, err)
}

func TestUDPTransport_SendMessage_NotStarted(t *testing.T) {
	logger := &defaultLogger{}
	transport := NewUDPTransport("127.0.0.1:0", logger)

	msg := &Message{
		Type: PingMsg,
	}

	err := transport.SendMessage("127.0.0.1:8000", msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "transport not started")
}

func TestUDPTransport_SendMessage_InvalidTarget(t *testing.T) {
	logger := &defaultLogger{}
	transport := NewUDPTransport("127.0.0.1:0", logger)

	err := transport.Start()
	assert.NoError(t, err)
	defer transport.Stop()

	msg := &Message{
		Type: PingMsg,
	}

	err = transport.SendMessage("invalid-address", msg)
	assert.Error(t, err)
}

func TestUDPTransport_ReceiveMessage(t *testing.T) {
	logger := &defaultLogger{}

	// Create receiver
	receiver := NewUDPTransport("127.0.0.1:0", logger)

	var wg sync.WaitGroup
	var receivedMsg *Message
	var mu sync.Mutex

	receiver.SetMessageHandler(func(msg *Message) {
		mu.Lock()
		receivedMsg = msg
		mu.Unlock()
		wg.Done()
	})

	err := receiver.Start()
	assert.NoError(t, err)
	defer receiver.Stop()

	receiverAddr := receiver.conn.LocalAddr().String()

	// Create sender
	sender := NewUDPTransport("127.0.0.1:0", logger)
	err = sender.Start()
	assert.NoError(t, err)
	defer sender.Stop()

	msg := &Message{
		Type:        PingMsg,
		From:        "sender",
		FromAddr:    "127.0.0.1:9000",
		SeqNo:       42,
		Incarnation: 5,
	}

	wg.Add(1)
	err = sender.SendMessage(receiverAddr, msg)
	assert.NoError(t, err)

	// Wait for message to be received
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		mu.Lock()
		assert.NotNil(t, receivedMsg)
		assert.Equal(t, PingMsg, receivedMsg.Type)
		assert.Equal(t, "sender", receivedMsg.From)
		assert.Equal(t, uint64(42), receivedMsg.SeqNo)
		mu.Unlock()
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func TestUDPTransport_BlockUnblock(t *testing.T) {
	logger := &defaultLogger{}
	transport := NewUDPTransport("127.0.0.1:0", logger)

	assert.False(t, transport.blocked)

	transport.BlockIncoming()
	assert.True(t, transport.blocked)

	transport.UnblockIncoming()
	assert.False(t, transport.blocked)
}

func TestUDPTransport_BlockedMessagesDropped(t *testing.T) {
	logger := &defaultLogger{}

	// Create receiver that will block messages
	receiver := NewUDPTransport("127.0.0.1:0", logger)

	var messageReceived bool
	var mu sync.Mutex

	receiver.SetMessageHandler(func(msg *Message) {
		mu.Lock()
		messageReceived = true
		mu.Unlock()
	})

	err := receiver.Start()
	assert.NoError(t, err)
	defer receiver.Stop()

	// Block incoming messages
	receiver.BlockIncoming()

	receiverAddr := receiver.conn.LocalAddr().String()

	// Create sender
	sender := NewUDPTransport("127.0.0.1:0", logger)
	err = sender.Start()
	assert.NoError(t, err)
	defer sender.Stop()

	msg := &Message{
		Type: PingMsg,
	}

	// Send message
	err = sender.SendMessage(receiverAddr, msg)
	assert.NoError(t, err)

	// Wait a bit
	time.Sleep(200 * time.Millisecond)

	// Message should have been dropped
	mu.Lock()
	assert.False(t, messageReceived)
	mu.Unlock()
}

func TestUDPTransport_NoHandler(t *testing.T) {
	logger := &defaultLogger{}

	// Create receiver without handler
	receiver := NewUDPTransport("127.0.0.1:0", logger)
	// Don't set message handler

	err := receiver.Start()
	assert.NoError(t, err)
	defer receiver.Stop()

	receiverAddr := receiver.conn.LocalAddr().String()

	// Create sender
	sender := NewUDPTransport("127.0.0.1:0", logger)
	err = sender.Start()
	assert.NoError(t, err)
	defer sender.Stop()

	msg := &Message{
		Type: PingMsg,
	}

	// Should not panic even without handler
	err = sender.SendMessage(receiverAddr, msg)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
}

func TestUDPTransport_ConcurrentSendReceive(t *testing.T) {
	logger := &defaultLogger{}

	// Create receiver
	receiver := NewUDPTransport("127.0.0.1:0", logger)

	var receivedCount int
	var mu sync.Mutex

	receiver.SetMessageHandler(func(msg *Message) {
		mu.Lock()
		receivedCount++
		mu.Unlock()
	})

	err := receiver.Start()
	assert.NoError(t, err)
	defer receiver.Stop()

	receiverAddr := receiver.conn.LocalAddr().String()

	// Create sender
	sender := NewUDPTransport("127.0.0.1:0", logger)
	err = sender.Start()
	assert.NoError(t, err)
	defer sender.Stop()

	// Send multiple messages concurrently
	var wg sync.WaitGroup
	numMessages := 10

	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		go func(seqNo int) {
			defer wg.Done()
			msg := &Message{
				Type:  PingMsg,
				SeqNo: uint64(seqNo),
			}
			sender.SendMessage(receiverAddr, msg)
		}(i)
	}

	wg.Wait()

	// Wait for messages to be processed
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	// At least some messages should have been received
	assert.Greater(t, receivedCount, 0)
	mu.Unlock()
}

func TestUDPTransport_InvalidMessageDecoding(t *testing.T) {
	logger := &defaultLogger{}

	// Create receiver
	receiver := NewUDPTransport("127.0.0.1:0", logger)

	var messageReceived bool
	var mu sync.Mutex

	receiver.SetMessageHandler(func(msg *Message) {
		mu.Lock()
		messageReceived = true
		mu.Unlock()
	})

	err := receiver.Start()
	assert.NoError(t, err)
	defer receiver.Stop()

	receiverAddr := receiver.conn.LocalAddr().String()

	// Send invalid JSON directly via UDP
	addr, err := net.ResolveUDPAddr("udp", receiverAddr)
	assert.NoError(t, err)

	conn, err := net.DialUDP("udp", nil, addr)
	assert.NoError(t, err)
	defer conn.Close()

	// Send invalid JSON
	_, err = conn.Write([]byte("not valid json"))
	assert.NoError(t, err)

	// Wait a bit
	time.Sleep(200 * time.Millisecond)

	// Handler should not have been called due to decode error
	mu.Lock()
	assert.False(t, messageReceived)
	mu.Unlock()
}

func TestUDPTransport_StopWhileListening(t *testing.T) {
	logger := &defaultLogger{}
	transport := NewUDPTransport("127.0.0.1:0", logger)

	err := transport.Start()
	assert.NoError(t, err)

	// Stop should not hang
	done := make(chan struct{})
	go func() {
		transport.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Good, stop completed
	case <-time.After(5 * time.Second):
		t.Fatal("Stop hung")
	}
}

func TestUDPTransport_MessageWithPiggyback(t *testing.T) {
	logger := &defaultLogger{}

	// Create receiver
	receiver := NewUDPTransport("127.0.0.1:0", logger)

	var receivedMsg *Message
	var wg sync.WaitGroup
	var mu sync.Mutex

	receiver.SetMessageHandler(func(msg *Message) {
		mu.Lock()
		receivedMsg = msg
		mu.Unlock()
		wg.Done()
	})

	err := receiver.Start()
	assert.NoError(t, err)
	defer receiver.Stop()

	receiverAddr := receiver.conn.LocalAddr().String()

	// Create sender
	sender := NewUDPTransport("127.0.0.1:0", logger)
	err = sender.Start()
	assert.NoError(t, err)
	defer sender.Stop()

	msg := &Message{
		Type:     PingMsg,
		From:     "sender",
		FromAddr: "127.0.0.1:9000",
		Piggyback: []Update{
			{MemberID: "node-1", Status: Alive, Incarnation: 1},
			{MemberID: "node-2", Status: Suspect, Incarnation: 2},
		},
	}

	wg.Add(1)
	err = sender.SendMessage(receiverAddr, msg)
	assert.NoError(t, err)

	// Wait for message
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		mu.Lock()
		assert.NotNil(t, receivedMsg)
		assert.Len(t, receivedMsg.Piggyback, 2)
		assert.Equal(t, "node-1", receivedMsg.Piggyback[0].MemberID)
		assert.Equal(t, "node-2", receivedMsg.Piggyback[1].MemberID)
		mu.Unlock()
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func TestTransportInterface(t *testing.T) {
	// Verify UDPTransport implements Transport interface
	var _ Transport = (*UDPTransport)(nil)
}

func TestUDPTransport_MultipleStartCalls(t *testing.T) {
	logger := &defaultLogger{}
	transport := NewUDPTransport("127.0.0.1:0", logger)

	// First start should succeed
	err := transport.Start()
	assert.NoError(t, err)
	defer transport.Stop()

	// Second start on already bound address should fail
	transport2 := NewUDPTransport(transport.conn.LocalAddr().String(), logger)
	err = transport2.Start()
	assert.Error(t, err)
}
