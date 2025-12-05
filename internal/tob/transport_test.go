package tob

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

	handler := func(msg *Message) {}

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
		Type:      DataMsg,
		From:      "sender",
		FromAddr:  "127.0.0.1:9000",
		MessageID: "msg-1",
	}

	err = sender.SendMessage(receiverAddr, msg)
	assert.NoError(t, err)
}

func TestUDPTransport_SendMessage_NotStarted(t *testing.T) {
	logger := &defaultLogger{}
	transport := NewUDPTransport("127.0.0.1:0", logger)

	msg := &Message{
		Type: DataMsg,
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
		Type: DataMsg,
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
		Type:           DataMsg,
		From:           "sender",
		FromAddr:       "127.0.0.1:9000",
		SequenceNumber: 42,
		MessageID:      "msg-123",
		Payload:        []byte("test payload"),
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
		assert.Equal(t, DataMsg, receivedMsg.Type)
		assert.Equal(t, "sender", receivedMsg.From)
		assert.Equal(t, "msg-123", receivedMsg.MessageID)
		assert.Equal(t, uint64(42), receivedMsg.SequenceNumber)
		mu.Unlock()
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
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
		Type: DataMsg,
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
				Type:           DataMsg,
				SequenceNumber: uint64(seqNo),
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

func TestUDPTransport_MessageWithPayload(t *testing.T) {
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

	largePayload := make([]byte, 1000)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	msg := &Message{
		Type:    DataMsg,
		From:    "sender",
		Payload: largePayload,
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
		assert.Equal(t, largePayload, receivedMsg.Payload)
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

func TestUDPTransport_AllMessageTypes(t *testing.T) {
	logger := &defaultLogger{}

	// Create receiver
	receiver := NewUDPTransport("127.0.0.1:0", logger)

	var receivedTypes []MessageType
	var mu sync.Mutex
	var wg sync.WaitGroup

	receiver.SetMessageHandler(func(msg *Message) {
		mu.Lock()
		receivedTypes = append(receivedTypes, msg.Type)
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

	messageTypes := []MessageType{DataMsg, SequencedMsg, HeartbeatMsg}
	wg.Add(len(messageTypes))

	for _, msgType := range messageTypes {
		msg := &Message{
			Type:      msgType,
			MessageID: "msg-" + msgType.String(),
		}
		err = sender.SendMessage(receiverAddr, msg)
		assert.NoError(t, err)
	}

	// Wait for messages
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		mu.Lock()
		assert.Len(t, receivedTypes, 3)
		mu.Unlock()
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for messages")
	}
}
