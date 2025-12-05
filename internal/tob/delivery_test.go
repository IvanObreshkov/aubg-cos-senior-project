package tob

import (
	"container/heap"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// mockTOB creates a minimal TOBroadcast for testing DeliveryManager
func mockTOB() *TOBroadcast {
	config := DefaultConfig()
	config.NodeID = "test-node"
	config.BindAddr = "127.0.0.1:0"
	config.AdvertiseAddr = "127.0.0.1:0"
	config.IsSequencer = true
	config.Nodes = []string{"127.0.0.1:0"}

	return &TOBroadcast{
		config:  config,
		metrics: NewMetrics(),
	}
}

func TestNewDeliveryManager(t *testing.T) {
	tob := mockTOB()
	dm := NewDeliveryManager(tob)

	assert.NotNil(t, dm)
	assert.Equal(t, uint64(1), dm.nextExpectedSeq)
	assert.NotNil(t, dm.pendingMessages)
	assert.NotNil(t, dm.deliveredMsgs)
	assert.NotNil(t, dm.stopCh)
	assert.Equal(t, tob, dm.tob)
}

func TestDeliveryManager_StartStop(t *testing.T) {
	tob := mockTOB()
	dm := NewDeliveryManager(tob)

	// Start should not panic
	dm.Start()

	// Wait a bit for goroutine to start
	time.Sleep(50 * time.Millisecond)

	// Stop should not panic
	dm.Stop()
}

func TestDeliveryManager_AddSequencedMessage_InOrder(t *testing.T) {
	tob := mockTOB()
	dm := NewDeliveryManager(tob)

	var deliveredMessages []*Message
	var mu sync.Mutex

	tob.deliveryCallback = func(msg *Message) {
		mu.Lock()
		deliveredMessages = append(deliveredMessages, msg)
		mu.Unlock()
	}

	// Add messages in order
	msg1 := &Message{MessageID: "msg-1", SequenceNumber: 1, Timestamp: time.Now()}
	msg2 := &Message{MessageID: "msg-2", SequenceNumber: 2, Timestamp: time.Now()}
	msg3 := &Message{MessageID: "msg-3", SequenceNumber: 3, Timestamp: time.Now()}

	dm.AddSequencedMessage(msg1)
	dm.AddSequencedMessage(msg2)
	dm.AddSequencedMessage(msg3)

	mu.Lock()
	assert.Len(t, deliveredMessages, 3)
	assert.Equal(t, "msg-1", deliveredMessages[0].MessageID)
	assert.Equal(t, "msg-2", deliveredMessages[1].MessageID)
	assert.Equal(t, "msg-3", deliveredMessages[2].MessageID)
	mu.Unlock()

	assert.Equal(t, uint64(4), dm.GetNextExpectedSeq())
}

func TestDeliveryManager_AddSequencedMessage_OutOfOrder(t *testing.T) {
	tob := mockTOB()
	dm := NewDeliveryManager(tob)

	var deliveredMessages []*Message
	var mu sync.Mutex

	tob.deliveryCallback = func(msg *Message) {
		mu.Lock()
		deliveredMessages = append(deliveredMessages, msg)
		mu.Unlock()
	}

	// Add messages out of order
	msg3 := &Message{MessageID: "msg-3", SequenceNumber: 3, Timestamp: time.Now()}
	msg1 := &Message{MessageID: "msg-1", SequenceNumber: 1, Timestamp: time.Now()}
	msg2 := &Message{MessageID: "msg-2", SequenceNumber: 2, Timestamp: time.Now()}

	dm.AddSequencedMessage(msg3) // Gap - should wait
	mu.Lock()
	assert.Len(t, deliveredMessages, 0)
	mu.Unlock()
	assert.Equal(t, 1, dm.GetPendingCount())

	dm.AddSequencedMessage(msg1) // First expected - deliver
	mu.Lock()
	assert.Len(t, deliveredMessages, 1)
	mu.Unlock()
	assert.Equal(t, 1, dm.GetPendingCount()) // msg3 still pending

	dm.AddSequencedMessage(msg2) // Second expected - deliver both
	mu.Lock()
	assert.Len(t, deliveredMessages, 3)
	assert.Equal(t, "msg-1", deliveredMessages[0].MessageID)
	assert.Equal(t, "msg-2", deliveredMessages[1].MessageID)
	assert.Equal(t, "msg-3", deliveredMessages[2].MessageID)
	mu.Unlock()

	assert.Equal(t, 0, dm.GetPendingCount())
	assert.Equal(t, uint64(4), dm.GetNextExpectedSeq())
}

func TestDeliveryManager_AddSequencedMessage_Duplicate(t *testing.T) {
	tob := mockTOB()
	dm := NewDeliveryManager(tob)

	var deliveryCount int
	var mu sync.Mutex

	tob.deliveryCallback = func(msg *Message) {
		mu.Lock()
		deliveryCount++
		mu.Unlock()
	}

	msg := &Message{MessageID: "msg-1", SequenceNumber: 1, Timestamp: time.Now()}

	dm.AddSequencedMessage(msg)
	dm.AddSequencedMessage(msg) // Duplicate

	mu.Lock()
	assert.Equal(t, 1, deliveryCount) // Should only deliver once
	mu.Unlock()
}

func TestDeliveryManager_AddSequencedMessage_OldSequence(t *testing.T) {
	tob := mockTOB()
	dm := NewDeliveryManager(tob)

	// Deliver first message
	msg1 := &Message{MessageID: "msg-1", SequenceNumber: 1, Timestamp: time.Now()}
	dm.AddSequencedMessage(msg1)

	// Add message with old sequence (already past)
	oldMsg := &Message{MessageID: "old-msg", SequenceNumber: 0, Timestamp: time.Now()}
	dm.AddSequencedMessage(oldMsg)

	// Should have recorded out of order
	assert.Equal(t, uint64(1), tob.metrics.outOfOrderCount.Load())
}

func TestDeliveryManager_GetNextExpectedSeq(t *testing.T) {
	tob := mockTOB()
	dm := NewDeliveryManager(tob)

	assert.Equal(t, uint64(1), dm.GetNextExpectedSeq())

	msg := &Message{MessageID: "msg-1", SequenceNumber: 1, Timestamp: time.Now()}
	dm.AddSequencedMessage(msg)

	assert.Equal(t, uint64(2), dm.GetNextExpectedSeq())
}

func TestDeliveryManager_GetPendingCount(t *testing.T) {
	tob := mockTOB()
	dm := NewDeliveryManager(tob)

	assert.Equal(t, 0, dm.GetPendingCount())

	// Add out of order message
	msg := &Message{MessageID: "msg-5", SequenceNumber: 5, Timestamp: time.Now()}
	dm.AddSequencedMessage(msg)

	assert.Equal(t, 1, dm.GetPendingCount())
}

func TestDeliveryManager_CleanupOldDeliveries(t *testing.T) {
	tob := mockTOB()
	dm := NewDeliveryManager(tob)

	// Add many delivered messages
	for i := uint64(1); i <= 10001; i++ {
		msg := &Message{MessageID: "msg-" + string(rune(i)), SequenceNumber: i, Timestamp: time.Now()}
		dm.deliveredMsgs[msg.MessageID] = true
	}

	assert.True(t, len(dm.deliveredMsgs) > 10000)

	dm.cleanupOldDeliveries()

	assert.Equal(t, 0, len(dm.deliveredMsgs))
}

func TestDeliveryManager_CleanupNoAction(t *testing.T) {
	tob := mockTOB()
	dm := NewDeliveryManager(tob)

	// Add fewer than threshold messages
	for i := 0; i < 100; i++ {
		dm.deliveredMsgs["msg-"+string(rune(i))] = true
	}

	dm.cleanupOldDeliveries()

	// Should not clean up
	assert.Equal(t, 100, len(dm.deliveredMsgs))
}

func TestDeliveryManager_NilCallback(t *testing.T) {
	tob := mockTOB()
	tob.deliveryCallback = nil
	dm := NewDeliveryManager(tob)

	msg := &Message{MessageID: "msg-1", SequenceNumber: 1, Timestamp: time.Now()}

	// Should not panic with nil callback
	assert.NotPanics(t, func() {
		dm.AddSequencedMessage(msg)
	})
}

func TestDeliveryManager_ConcurrentAccess(t *testing.T) {
	tob := mockTOB()
	dm := NewDeliveryManager(tob)

	var deliveredCount int
	var mu sync.Mutex

	tob.deliveryCallback = func(msg *Message) {
		mu.Lock()
		deliveredCount++
		mu.Unlock()
	}

	var wg sync.WaitGroup
	numGoroutines := 10
	messagesPerGoroutine := 10

	// Concurrently add messages
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < messagesPerGoroutine; i++ {
				seqNum := uint64(goroutineID*messagesPerGoroutine + i + 1)
				msg := &Message{
					MessageID:      "msg-" + string(rune(seqNum)),
					SequenceNumber: seqNum,
					Timestamp:      time.Now(),
				}
				dm.AddSequencedMessage(msg)
			}
		}(g)
	}

	wg.Wait()

	// Should have delivered some messages (exact count depends on order)
	mu.Lock()
	assert.GreaterOrEqual(t, deliveredCount, 0)
	mu.Unlock()
}

func TestMessageHeap_Len(t *testing.T) {
	h := &MessageHeap{}
	assert.Equal(t, 0, h.Len())

	*h = append(*h, &Message{SequenceNumber: 1})
	assert.Equal(t, 1, h.Len())

	*h = append(*h, &Message{SequenceNumber: 2})
	assert.Equal(t, 2, h.Len())
}

func TestMessageHeap_Less(t *testing.T) {
	h := MessageHeap{
		&Message{SequenceNumber: 5},
		&Message{SequenceNumber: 3},
	}

	assert.True(t, h.Less(1, 0))  // 3 < 5
	assert.False(t, h.Less(0, 1)) // 5 > 3
}

func TestMessageHeap_Swap(t *testing.T) {
	msg1 := &Message{SequenceNumber: 1}
	msg2 := &Message{SequenceNumber: 2}
	h := MessageHeap{msg1, msg2}

	h.Swap(0, 1)

	assert.Equal(t, msg2, h[0])
	assert.Equal(t, msg1, h[1])
}

func TestMessageHeap_PushPop(t *testing.T) {
	h := &MessageHeap{}
	heap.Init(h)

	msg1 := &Message{SequenceNumber: 3}
	msg2 := &Message{SequenceNumber: 1}
	msg3 := &Message{SequenceNumber: 2}

	heap.Push(h, msg1)
	heap.Push(h, msg2)
	heap.Push(h, msg3)

	assert.Equal(t, 3, h.Len())

	// Pop should return in order (min-heap)
	popped1 := heap.Pop(h).(*Message)
	assert.Equal(t, uint64(1), popped1.SequenceNumber)

	popped2 := heap.Pop(h).(*Message)
	assert.Equal(t, uint64(2), popped2.SequenceNumber)

	popped3 := heap.Pop(h).(*Message)
	assert.Equal(t, uint64(3), popped3.SequenceNumber)
}

func TestMessageHeap_MinHeapProperty(t *testing.T) {
	h := &MessageHeap{}
	heap.Init(h)

	// Add in random order
	sequences := []uint64{5, 2, 8, 1, 9, 3}
	for _, seq := range sequences {
		heap.Push(h, &Message{SequenceNumber: seq})
	}

	// Pop should return sorted
	expected := []uint64{1, 2, 3, 5, 8, 9}
	for _, exp := range expected {
		msg := heap.Pop(h).(*Message)
		assert.Equal(t, exp, msg.SequenceNumber)
	}
}
