package tob

import (
	"sync"
	"time"
)

// Sequencer manages sequence number assignment for Total Order Broadcast
// Paper (Section 4.1): "Fixed Sequencer algorithm uses a centralized sequencer that
// assigns a sequence number to each message in the order it receives them"
//
// Implementation detail: Uses a buffered channel (queue) to preserve FIFO order
type Sequencer struct {
	nextSeqNum   uint64
	mu           sync.Mutex
	messageQueue chan *Message
	tob          *TOBroadcast
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// NewSequencer creates a new sequencer
func NewSequencer(tob *TOBroadcast) *Sequencer {
	return &Sequencer{
		nextSeqNum:   1, // Implementation choice: sequence numbers start from 1
		messageQueue: make(chan *Message, 100),
		tob:          tob,
		stopCh:       make(chan struct{}),
	}
}

// Start begins the sequencer operation
func (s *Sequencer) Start() {
	s.tob.config.Logger.Infof("[Sequencer] Starting sequencer on node %s", s.tob.config.NodeID)

	s.wg.Add(1)
	go s.runSequencer()

	// Implementation detail: Heartbeat mechanism for failure detection (extension beyond Section 4.1)
	s.wg.Add(1)
	go s.sendHeartbeats()
}

// Stop stops the sequencer
func (s *Sequencer) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

// SubmitMessage submits a message to the sequencer for sequence number assignment
// Paper (Section 4.1): "When a process wants to broadcast a message m, it sends m to the sequencer"
func (s *Sequencer) SubmitMessage(msg *Message) error {
	select {
	case s.messageQueue <- msg:
		s.tob.config.Logger.Debugf("[Sequencer] Message %s queued for sequencing", msg.MessageID)
		return nil
	case <-s.stopCh:
		return ErrSequencerStopped
	default:
		s.tob.config.Logger.Warnf("[Sequencer] Message queue full, dropping message %s", msg.MessageID)
		return ErrQueueFull
	}
}

// runSequencer processes messages and assigns sequence numbers
// Paper (Section 4.1): "The sequencer assigns a sequence number to each message
// in the order it receives them"
func (s *Sequencer) runSequencer() {
	defer s.wg.Done()

	for {
		select {
		case msg := <-s.messageQueue:
			s.processMessage(msg)
		case <-s.stopCh:
			return
		}
	}
}

// processMessage assigns a sequence number and broadcasts it
// Paper (Section 4.1): "The sequencer multicasts the message along with its sequence number
// to all processes (including itself)"
func (s *Sequencer) processMessage(msg *Message) {
	startTime := time.Now()

	// Assign sequence number
	s.mu.Lock()
	seqNum := s.nextSeqNum
	s.nextSeqNum++
	s.mu.Unlock()

	s.tob.config.Logger.Debugf("[Sequencer] Assigned sequence number %d to message %s from %s",
		seqNum, msg.MessageID, msg.From)

	// Create sequenced message
	sequencedMsg := &Message{
		Type:           SequencedMsg,
		From:           msg.From,
		FromAddr:       msg.FromAddr,
		SequenceNumber: seqNum,
		MessageID:      msg.MessageID,
		Payload:        msg.Payload,
		Timestamp:      msg.Timestamp,
	}

	// Multicast to all nodes
	// Paper: "The sequencer multicasts the sequenced message to all processes"
	s.tob.multicastSequencedMessage(sequencedMsg)

	// Record sequencing latency
	sequencingLatency := time.Since(startTime)
	s.tob.metrics.RecordSequencingLatency(sequencingLatency)
	s.tob.metrics.RecordSequencedMsg()
}

// sendHeartbeats sends periodic heartbeats to detect sequencer failures
func (s *Sequencer) sendHeartbeats() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.tob.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.sendHeartbeat()
		case <-s.stopCh:
			return
		}
	}
}

// sendHeartbeat sends a single heartbeat to all nodes
func (s *Sequencer) sendHeartbeat() {
	msg := &Message{
		Type:      HeartbeatMsg,
		From:      s.tob.config.NodeID,
		FromAddr:  s.tob.config.AdvertiseAddr,
		Timestamp: time.Now(),
	}

	// Multicast heartbeat to all nodes
	for _, nodeAddr := range s.tob.config.Nodes {
		if nodeAddr != s.tob.config.AdvertiseAddr {
			if err := s.tob.transport.SendMessage(nodeAddr, msg); err != nil {
				s.tob.config.Logger.Errorf("[Sequencer] Failed to send heartbeat to %s: %v", nodeAddr, err)
			} else {
				s.tob.metrics.RecordHeartbeatMsg()
			}
		}
	}
}

// GetNextSequenceNumber returns the next sequence number (for monitoring)
func (s *Sequencer) GetNextSequenceNumber() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextSeqNum
}
