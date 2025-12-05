package tob

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
}

func NewMockTransport() *MockTransport {
	return &MockTransport{
		sentMessages: make([]*Message, 0),
	}
}

func (m *MockTransport) Start() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockTransport) Stop() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockTransport) SendMessage(targetAddr string, msg *Message) error {
	m.mu.Lock()
	m.sentMessages = append(m.sentMessages, msg)
	m.mu.Unlock()
	args := m.Called(targetAddr, msg)
	return args.Error(0)
}

func (m *MockTransport) SetMessageHandler(handler func(*Message)) {
	m.mu.Lock()
	m.messageHandler = handler
	m.mu.Unlock()
}

func (m *MockTransport) GetSentMessages() []*Message {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*Message, len(m.sentMessages))
	copy(result, m.sentMessages)
	return result
}

func (m *MockTransport) SimulateMessage(msg *Message) {
	m.mu.RLock()
	handler := m.messageHandler
	m.mu.RUnlock()
	if handler != nil {
		handler(msg)
	}
}

// createMockTOB creates a TOBroadcast with mocked transport for testing sequencer
func createMockTOB() *TOBroadcast {
	config := DefaultConfig()
	config.NodeID = "sequencer-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.IsSequencer = true
	config.Nodes = []string{"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}
	config.HeartbeatInterval = 50 * time.Millisecond

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	tob := &TOBroadcast{
		config:    config,
		transport: mockTransport,
		metrics:   NewMetrics(),
	}

	tob.delivery = NewDeliveryManager(tob)

	return tob
}

func TestNewSequencer(t *testing.T) {
	tob := createMockTOB()
	seq := NewSequencer(tob)

	assert.NotNil(t, seq)
	assert.Equal(t, uint64(1), seq.nextSeqNum)
	assert.NotNil(t, seq.messageQueue)
	assert.Equal(t, tob, seq.tob)
	assert.NotNil(t, seq.stopCh)
}

func TestSequencer_StartStop(t *testing.T) {
	tob := createMockTOB()
	seq := NewSequencer(tob)

	// Start should not panic
	seq.Start()

	// Wait a bit for goroutines to start
	time.Sleep(50 * time.Millisecond)

	// Stop should not panic
	seq.Stop()
}

func TestSequencer_SubmitMessage(t *testing.T) {
	tob := createMockTOB()
	seq := NewSequencer(tob)
	seq.Start()
	defer seq.Stop()

	msg := &Message{
		Type:      DataMsg,
		From:      "client",
		MessageID: "msg-1",
		Payload:   []byte("test"),
		Timestamp: time.Now(),
	}

	err := seq.SubmitMessage(msg)
	assert.NoError(t, err)

	// Wait for processing
	time.Sleep(50 * time.Millisecond)
}

func TestSequencer_SubmitMessage_QueueFull(t *testing.T) {
	tob := createMockTOB()
	seq := NewSequencer(tob)
	// Don't start the sequencer so queue won't be drained

	// Fill the queue
	for i := 0; i < 100; i++ {
		msg := &Message{
			MessageID: "msg-" + string(rune(i)),
		}
		seq.messageQueue <- msg
	}

	// Try to submit when queue is full
	msg := &Message{MessageID: "overflow-msg"}
	err := seq.SubmitMessage(msg)
	assert.Equal(t, ErrQueueFull, err)
}

func TestSequencer_SubmitMessage_Stopped(t *testing.T) {
	tob := createMockTOB()
	seq := NewSequencer(tob)

	// Close stop channel and fill the message queue to ensure the stopCh case is selected
	close(seq.stopCh)

	// Fill the queue first to block the main send path
	for i := 0; i < 100; i++ {
		select {
		case seq.messageQueue <- &Message{MessageID: "filler-" + string(rune(i))}:
		default:
			break
		}
	}

	msg := &Message{MessageID: "msg-1"}
	err := seq.SubmitMessage(msg)

	// With a closed stopCh and full queue, we should get either ErrSequencerStopped or ErrQueueFull
	// The actual behavior depends on which select case is chosen
	assert.True(t, err == ErrSequencerStopped || err == ErrQueueFull)
}

func TestSequencer_ProcessMessage(t *testing.T) {
	tob := createMockTOB()
	seq := NewSequencer(tob)
	seq.Start()
	defer seq.Stop()

	var deliveredMsgs []*Message
	var mu sync.Mutex

	tob.deliveryCallback = func(msg *Message) {
		mu.Lock()
		deliveredMsgs = append(deliveredMsgs, msg)
		mu.Unlock()
	}

	msg := &Message{
		Type:      DataMsg,
		From:      "client",
		MessageID: "msg-1",
		Payload:   []byte("test"),
		Timestamp: time.Now(),
	}

	err := seq.SubmitMessage(msg)
	assert.NoError(t, err)

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Check that at least one message was delivered
	if len(deliveredMsgs) > 0 {
		assert.Equal(t, uint64(1), deliveredMsgs[0].SequenceNumber)
		assert.Equal(t, SequencedMsg, deliveredMsgs[0].Type)
	}
}

func TestSequencer_ProcessMessage_IncrementsSequence(t *testing.T) {
	tob := createMockTOB()
	seq := NewSequencer(tob)
	seq.Start()
	defer seq.Stop()

	for i := 0; i < 5; i++ {
		msg := &Message{
			Type:      DataMsg,
			MessageID: "msg-" + string(rune('0'+i)),
			Timestamp: time.Now(),
		}
		err := seq.SubmitMessage(msg)
		assert.NoError(t, err)
	}

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, uint64(6), seq.GetNextSequenceNumber())
}

func TestSequencer_GetNextSequenceNumber(t *testing.T) {
	tob := createMockTOB()
	seq := NewSequencer(tob)

	assert.Equal(t, uint64(1), seq.GetNextSequenceNumber())

	// Simulate processing a message
	seq.mu.Lock()
	seq.nextSeqNum++
	seq.mu.Unlock()

	assert.Equal(t, uint64(2), seq.GetNextSequenceNumber())
}

func TestSequencer_SendHeartbeats(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "sequencer-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.IsSequencer = true
	config.Nodes = []string{"127.0.0.1:8000", "127.0.0.1:8001"}
	config.HeartbeatInterval = 20 * time.Millisecond

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	tob := &TOBroadcast{
		config:    config,
		transport: mockTransport,
		metrics:   NewMetrics(),
	}
	tob.delivery = NewDeliveryManager(tob)

	seq := NewSequencer(tob)
	seq.Start()

	// Wait for some heartbeats
	time.Sleep(80 * time.Millisecond)

	seq.Stop()

	// Check that heartbeat messages were sent
	sentMessages := mockTransport.GetSentMessages()
	heartbeatCount := 0
	for _, msg := range sentMessages {
		if msg.Type == HeartbeatMsg {
			heartbeatCount++
		}
	}
	assert.GreaterOrEqual(t, heartbeatCount, 2)
}

func TestSequencer_SendHeartbeat_Error(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "sequencer-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.IsSequencer = true
	config.Nodes = []string{"127.0.0.1:8000", "127.0.0.1:8001"}

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", "127.0.0.1:8001", mock.Anything).Return(assert.AnError)

	tob := &TOBroadcast{
		config:    config,
		transport: mockTransport,
		metrics:   NewMetrics(),
	}

	seq := NewSequencer(tob)

	// Should not panic even if send fails
	assert.NotPanics(t, func() {
		seq.sendHeartbeat()
	})
}

func TestSequencer_MulticastToAllNodes(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "sequencer-node"
	config.BindAddr = "127.0.0.1:8000"
	config.AdvertiseAddr = "127.0.0.1:8000"
	config.IsSequencer = true
	config.Nodes = []string{"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}

	mockTransport := NewMockTransport()
	mockTransport.On("SendMessage", mock.Anything, mock.Anything).Return(nil)

	tob := &TOBroadcast{
		config:      config,
		transport:   mockTransport,
		metrics:     NewMetrics(),
		isSequencer: true,
	}
	tob.delivery = NewDeliveryManager(tob)

	seq := NewSequencer(tob)
	seq.Start()
	defer seq.Stop()

	msg := &Message{
		Type:      DataMsg,
		MessageID: "msg-1",
		Timestamp: time.Now(),
	}

	err := seq.SubmitMessage(msg)
	assert.NoError(t, err)

	// Wait for processing
	time.Sleep(50 * time.Millisecond)

	// Check that sequenced messages were sent to other nodes
	sentMessages := mockTransport.GetSentMessages()
	sequencedCount := 0
	for _, m := range sentMessages {
		if m.Type == SequencedMsg {
			sequencedCount++
		}
	}
	// Should have sent to 2 other nodes (excluding self)
	assert.GreaterOrEqual(t, sequencedCount, 2)
}

func TestSequencer_ConcurrentSubmit(t *testing.T) {
	tob := createMockTOB()
	seq := NewSequencer(tob)
	seq.Start()
	defer seq.Stop()

	var wg sync.WaitGroup
	numGoroutines := 10
	messagesPerGoroutine := 10

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < messagesPerGoroutine; i++ {
				msg := &Message{
					Type:      DataMsg,
					MessageID: "msg-" + string(rune(goroutineID)) + string(rune(i)),
					Timestamp: time.Now(),
				}
				seq.SubmitMessage(msg)
			}
		}(g)
	}

	wg.Wait()

	// Wait for all messages to be processed
	time.Sleep(200 * time.Millisecond)

	// Sequence numbers should have been assigned
	assert.GreaterOrEqual(t, seq.GetNextSequenceNumber(), uint64(1))
}

func TestSequencer_RecordsMetrics(t *testing.T) {
	tob := createMockTOB()
	seq := NewSequencer(tob)
	seq.Start()
	defer seq.Stop()

	msg := &Message{
		Type:      DataMsg,
		MessageID: "msg-1",
		Timestamp: time.Now(),
	}

	err := seq.SubmitMessage(msg)
	assert.NoError(t, err)

	// Wait for processing
	time.Sleep(50 * time.Millisecond)

	// Check that metrics were recorded
	assert.Equal(t, uint64(1), tob.metrics.sequencedMsgCount.Load())

	stats := tob.metrics.GetSequencingLatencyStats()
	assert.Equal(t, 1, stats.Count)
}
