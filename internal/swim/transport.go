package swim

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

// Transport handles network communication between SWIM nodes
type Transport interface {
	// Start begins listening for incoming messages
	Start() error
	// Stop shuts down the transport
	Stop() error
	// SendMessage sends a message to a target address
	SendMessage(targetAddr string, msg *Message) error
	// SetMessageHandler sets the handler for incoming messages
	SetMessageHandler(handler func(*Message))
}

// UDPTransport implements Transport using UDP
// Section 5: "The current implementation uses UDP for all point-to-point message transmissions"
type UDPTransport struct {
	bindAddr       string
	conn           *net.UDPConn
	messageHandler func(*Message)
	mu             sync.RWMutex
	shutdownCh     chan struct{}
	wg             sync.WaitGroup
	logger         Logger
	blocked        bool // For testing: drop all incoming messages when true
}

// NewUDPTransport creates a new UDP transport
func NewUDPTransport(bindAddr string, logger Logger) *UDPTransport {
	return &UDPTransport{
		bindAddr:   bindAddr,
		shutdownCh: make(chan struct{}),
		logger:     logger,
	}
}

// Start begins listening for incoming UDP messages
func (t *UDPTransport) Start() error {
	addr, err := net.ResolveUDPAddr("udp", t.bindAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on UDP: %w", err)
	}

	t.conn = conn
	t.wg.Add(1)

	go t.listen()

	t.logger.Infof("[Transport] Started UDP transport on %s", t.bindAddr)
	return nil
}

// Stop shuts down the transport
func (t *UDPTransport) Stop() error {
	close(t.shutdownCh)
	if t.conn != nil {
		if err := t.conn.Close(); err != nil {
			t.logger.Errorf("[Transport] Error closing connection: %v", err)
		}
	}
	t.wg.Wait()
	t.logger.Infof("[Transport] Stopped UDP transport")
	return nil
}

// listen continuously reads messages from the UDP socket
func (t *UDPTransport) listen() {
	defer t.wg.Done()

	buffer := make([]byte, 65536) // Max UDP packet size

	for {
		select {
		case <-t.shutdownCh:
			return
		default:
		}

		// Set read deadline to allow checking shutdown channel
		if err := t.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			t.logger.Errorf("[Transport] Error setting read deadline: %v", err)
			continue
		}

		n, addr, err := t.conn.ReadFromUDP(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			// Check if connection is closed
			select {
			case <-t.shutdownCh:
				return
			default:
				t.logger.Errorf("[Transport] Error reading from UDP: %v", err)
				continue
			}
		}

		// Decode message
		msg := &Message{}
		if err := json.Unmarshal(buffer[:n], msg); err != nil {
			t.logger.Errorf("[Transport] Error decoding message from %s: %v", addr, err)
			continue
		}

		// Handle message
		t.mu.RLock()
		handler := t.messageHandler
		blocked := t.blocked
		t.mu.RUnlock()

		// Drop message if transport is blocked (for testing)
		if blocked {
			continue
		}

		if handler != nil {
			handler(msg)
		}
	}
}

// SendMessage sends a message to a target address
func (t *UDPTransport) SendMessage(targetAddr string, msg *Message) error {
	if t.conn == nil {
		return fmt.Errorf("transport not started")
	}

	// Encode message
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}

	// Resolve target address
	addr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve target address: %w", err)
	}

	// Send message
	_, err = t.conn.WriteToUDP(data, addr)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

// SetMessageHandler sets the handler for incoming messages
func (t *UDPTransport) SetMessageHandler(handler func(*Message)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.messageHandler = handler
}

// BlockIncoming blocks all incoming messages (for testing network partitions)
func (t *UDPTransport) BlockIncoming() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blocked = true
}

// UnblockIncoming resumes processing incoming messages
func (t *UDPTransport) UnblockIncoming() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blocked = false
}
