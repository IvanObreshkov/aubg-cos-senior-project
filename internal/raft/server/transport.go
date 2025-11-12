package server

import (
	"aubg-cos-senior-project/internal/raft/proto"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// RPCTimeout is the maximum time to wait for a single RPC attempt
	// Section 5.6 states that broadcast time should be an order of magnitude less than the election timeout (150-300ms)
	// For typical networks, RPC round-trip times are << 15ms, so a 50-75ms timeout provides a comfortable safety margin.
	RPCTimeout = 50 * time.Millisecond

	// MaxRequestVoteRetries is the number of times to retry a failed RequestVote RPC
	// RequestVote retries are bounded by the election timeout - if an election fails,
	// a new election with a new term will be started. 3 attempts × 50ms = ~150ms,
	// which is within the election timeout window (150-300ms).
	MaxRequestVoteRetries = 3

	// MaxAppendEntriesRetries controls retry behavior for AppendEntries RPCs
	// Section 5.3 from the Raft paper states: "If followers crash or run slowly, or if
	// network packets are lost, the leader retries AppendEntries RPCs indefinitely
	// (even after it has responded to the client) until all followers eventually store
	// all log entries."
	// However, for the initial implementation with just heartbeats, we use a high but
	// finite limit. When full log replication is implemented, this should be moved to
	// per-follower replication goroutines with indefinite retry.
	// TODO: Implement per-follower replication goroutines with indefinite retry
	MaxAppendEntriesRetries = 100

	// RetryBackoffBase is the base duration for exponential backoff between retries
	RetryBackoffBase = 10 * time.Millisecond

	// MaxRetryBackoff is the maximum backoff duration between retries
	MaxRetryBackoff = 100 * time.Millisecond
)

type Transport struct {
	// A map to store the underlying grpc.ClientConn for each peer. It is a map[ServerID]*grpc.ClientConn.
	// sync.Map provides thread-safe access to the map, and is optimized for read operations, reducing the overhead of
	// manual locks
	clientsConnPool *sync.Map
	// Optional metrics collector
	metrics MetricsCollector
}

// getClientConn retrieves a grpc.ClientConn for the given server.ServerID from the connection pool
func (t *Transport) getClientConn(peerID ServerID) (*grpc.ClientConn, error) {
	clientConn, ok := t.clientsConnPool.Load(peerID)
	if !ok {
		return nil, fmt.Errorf("gRPC client connection not found for server with addr: %v", peerID)
	}

	// We must type assert the value returned by Load, as it is of type `any` by default
	conn, ok := clientConn.(*grpc.ClientConn)
	if !ok {
		return nil, fmt.Errorf("invalid clientConn type for server with addr: %v. Type is %T", peerID, clientConn)
	}

	return conn, nil
}

func (t *Transport) RequestVote(ctx context.Context, peerID ServerID, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	// Record metrics if available
	if t.metrics != nil {
		t.metrics.RecordRequestVote()
	}

	conn, err := t.getClientConn(peerID)
	if err != nil {
		// Peer no longer in cluster - this is expected during membership changes
		return nil, fmt.Errorf("peer %s not found (likely removed from cluster): %w", peerID, err)
	}

	// Create the RaftServiceClient on the fly. This is just a wrapper around the connection, and we need it as it
	// provides the methods needed to exec the RPC calls
	client := proto.NewRaftServiceClient(conn)

	// Retry loop with timeout per attempt
	var resp *proto.RequestVoteResponse
	var lastErr error

	for attempt := 0; attempt < MaxRequestVoteRetries; attempt++ {
		// Create a new context with timeout for each attempt
		rpcCtx, cancel := context.WithTimeout(ctx, RPCTimeout)

		resp, lastErr = client.RequestVote(rpcCtx, req)
		cancel() // Always clean up the context

		if lastErr == nil {
			// Success - return immediately
			return resp, nil
		}

		// Check if parent context is cancelled (e.g., server shutting down)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("RequestVote to %s cancelled: %w", peerID, ctx.Err())
		default:
		}

		// Don't sleep after the last attempt
		if attempt < MaxRequestVoteRetries-1 {
			// Exponential backoff: 10ms, 20ms, 30ms
			backoff := RetryBackoffBase * time.Duration(attempt+1)
			if backoff > MaxRetryBackoff {
				backoff = MaxRetryBackoff
			}
			time.Sleep(backoff)
		}
	}

	// All retries exhausted - log once here
	log.Printf("[TRANSPORT] RequestVote to %s failed after %d attempts: %v", peerID, MaxRequestVoteRetries, lastErr)
	return nil, fmt.Errorf("RequestVote to %s failed after %d attempts: %w", peerID, MaxRequestVoteRetries, lastErr)
}

func (t *Transport) AppendEntries(ctx context.Context, peerID ServerID, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	// Record metrics if available
	if t.metrics != nil {
		if len(req.Entries) == 0 {
			t.metrics.RecordHeartbeat()
		} else {
			t.metrics.RecordAppendEntries()
		}
	}

	conn, err := t.getClientConn(peerID)
	if err != nil {
		// Peer no longer in cluster - this is expected during membership changes
		// Don't retry, just return immediately
		return nil, fmt.Errorf("peer %s not found (likely removed from cluster): %w", peerID, err)
	}

	client := proto.NewRaftServiceClient(conn)

	// Retry loop with timeout per attempt
	// Section 5.3: "The leader retries AppendEntries RPCs indefinitely until all
	// followers eventually store all log entries"
	var resp *proto.AppendEntriesResponse
	var lastErr error

	for attempt := 0; attempt < MaxAppendEntriesRetries; attempt++ {
		// Create a new context with timeout for each attempt
		rpcCtx, cancel := context.WithTimeout(ctx, RPCTimeout)

		resp, lastErr = client.AppendEntries(rpcCtx, req)
		cancel() // Always clean up the context

		if lastErr == nil {
			// Success - log if we had to retry
			if attempt > 0 {
				log.Printf("[TRANSPORT] AppendEntries to %s succeeded after %d retries", peerID, attempt)
			}
			return resp, nil
		}

		// Check if parent context is cancelled (leader stepping down, server shutting down, etc.)
		select {
		case <-ctx.Done():
			log.Printf("[TRANSPORT] AppendEntries to %s cancelled after %d attempts: %v", peerID, attempt, ctx.Err())
			return nil, fmt.Errorf("AppendEntries to %s cancelled: %w", peerID, ctx.Err())
		default:
		}

		// Don't sleep after the last attempt
		if attempt < MaxAppendEntriesRetries-1 {
			// Exponential backoff with cap
			backoff := RetryBackoffBase * time.Duration(attempt+1)
			if backoff > MaxRetryBackoff {
				backoff = MaxRetryBackoff
			}
			time.Sleep(backoff)
		}

		// Log periodic progress for long-running retries
		if attempt > 0 && attempt%10 == 0 {
			log.Printf("[TRANSPORT] AppendEntries to %s still retrying (attempt %d/%d)", peerID, attempt, MaxAppendEntriesRetries)
		}
	}

	// All retries exhausted - log once here
	// Note: In a full implementation with per-follower replication goroutines,
	// this would continue retrying indefinitely as per the Raft paper
	log.Printf("[TRANSPORT] AppendEntries to %s failed after %d attempts: %v", peerID, MaxAppendEntriesRetries, lastErr)
	return nil, fmt.Errorf("AppendEntries to %s failed after %d attempts: %w", peerID, MaxAppendEntriesRetries, lastErr)
}

func (t *Transport) HeartbeatRPC(ctx context.Context, peerID ServerID) (*proto.AppendEntriesResponse, error) {
	// This is passed from the server which is sending the request
	currTerm, ok := GetServerCurrTerm(ctx)
	if !ok {
		panic(fmt.Sprintf("required currTerm not found in ctx: %v", currTerm))
	}

	leaderID, ok := GetServerID(ctx)
	if !ok {
		panic("required leaderID not found in ctx")
	}

	// TODO: Retrieve server info from the ctx
	hbReq := &proto.AppendEntriesRequest{
		Term:     currTerm,
		LeaderId: string(leaderID),
		// TODO: Get these from actual log state
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		// This in empty for heartbeats as per Section 5.2
		Entries:      nil,
		LeaderCommit: 0,
	}

	return t.AppendEntries(ctx, peerID, hbReq)
}

// Initializes a gRPC channel from the current server to every other from its peers
func (t *Transport) initClients(peerIDs []ServerID) {
	for _, id := range peerIDs {
		target := fmt.Sprintf("%s:///%s", raftScheme, id) // "raft:///UUID"
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed establishing a gRPC channel to peer %v. Err: %v", id, err)
			// Failing to establish a connection to a single Node, should not prevent conn to other nodes, so log it
			// and continue
			continue
		}

		t.clientsConnPool.Store(id, conn)
	}
}

// CloseAllClients closes all gRPC client connections initiated by the server
func (t *Transport) CloseAllClients() {
	// Range is a thread-safe way to iterate over a sync.Map.
	t.clientsConnPool.Range(func(key, value any) bool {
		if conn, ok := value.(*grpc.ClientConn); ok {
			if err := conn.Close(); err != nil {
				log.Printf("Failed to close connection to %s: %v", key, err)
			}
		}
		// Return true to continue the iteration.
		return true
	})
	log.Println("All gRPC client connections closed.")
}

// AddPeer adds a gRPC connection for a new peer that joined the cluster
func (t *Transport) AddPeer(peerID ServerID, peerAddr ServerAddress) error {
	// Check if connection already exists
	if _, err := t.getClientConn(peerID); err == nil {
		// Connection already exists, nothing to do
		return nil
	}

	// Register the peer's address with the DNS resolver first
	RegisterResolverPeer(peerID, peerAddr)

	// Create new connection
	target := fmt.Sprintf("%s:///%s", raftScheme, peerID) // "raft:///UUID"
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to establish gRPC connection to peer %s: %w", peerID, err)
	}

	t.clientsConnPool.Store(peerID, conn)
	log.Printf("[TRANSPORT] Added gRPC connection for new peer %s at %s", peerID, peerAddr)
	return nil
}

// RemovePeer closes and removes the gRPC connection for a peer that left the cluster
func (t *Transport) RemovePeer(peerID ServerID) {
	if value, ok := t.clientsConnPool.LoadAndDelete(peerID); ok {
		if conn, ok := value.(*grpc.ClientConn); ok {
			if err := conn.Close(); err != nil {
				log.Printf("[TRANSPORT] Failed to close connection to removed peer %s: %v", peerID, err)
			} else {
				log.Printf("[TRANSPORT] Closed connection to removed peer: %s", peerID)
			}
		}
	}
}

func NewTransport(peerIDs []ServerID, metrics MetricsCollector) *Transport {
	transport := &Transport{
		clientsConnPool: &sync.Map{},
		metrics:         metrics,
	}

	transport.initClients(peerIDs)

	return transport
}
