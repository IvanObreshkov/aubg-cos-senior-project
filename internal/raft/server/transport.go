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

	// MaxRPCRetries is the number of times to retry a failed RPC
	MaxRPCRetries = 3

	// RetryBackoffBase is the base duration for exponential backoff between retries
	RetryBackoffBase = 10 * time.Millisecond
)

type Transport struct {
	// A map to store the underlying grpc.ClientConn for each peer. It is a map[ServerID]*grpc.ClientConn.
	// sync.Map provides thread-safe access to the map, and is optimized for read operations, reducing the overhead of
	// manual locks
	clientsConnPool *sync.Map
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
	conn, err := t.getClientConn(peerID)
	if err != nil {
		return nil, err
	}

	// Create the RaftServiceClient on the fly. This is just a wrapper around the connection, and we need it as it
	// provides the methods needed to exec the RPC calls
	client := proto.NewRaftServiceClient(conn)

	// Retry loop with timeout per attempt
	var resp *proto.RequestVoteResponse
	var lastErr error

	for attempt := 0; attempt < MaxRPCRetries; attempt++ {
		// Create a new context with timeout for each attempt
		rpcCtx, cancel := context.WithTimeout(ctx, RPCTimeout)

		resp, lastErr = client.RequestVote(rpcCtx, req)
		cancel() // Always clean up the context

		if lastErr == nil {
			// Success - return immediately
			return resp, nil
		}

		// Don't sleep after the last attempt
		if attempt < MaxRPCRetries-1 {
			// Exponential backoff: 10ms, 20ms, 30ms, etc.
			backoff := RetryBackoffBase * time.Duration(attempt+1)
			time.Sleep(backoff)
		}
	}

	// All retries exhausted - log once here
	log.Printf("RequestVote to %s failed after %d attempts: %v", peerID, MaxRPCRetries, lastErr)
	return nil, fmt.Errorf("RequestVote to %s failed after %d attempts: %w", peerID, MaxRPCRetries, lastErr)
}

func (t *Transport) AppendEntries(ctx context.Context, peerID ServerID, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	conn, err := t.getClientConn(peerID)
	if err != nil {
		return nil, err
	}

	client := proto.NewRaftServiceClient(conn)

	// Retry loop with timeout per attempt
	var resp *proto.AppendEntriesResponse
	var lastErr error

	for attempt := 0; attempt < MaxRPCRetries; attempt++ {
		// Create a new context with timeout for each attempt
		rpcCtx, cancel := context.WithTimeout(ctx, RPCTimeout)

		resp, lastErr = client.AppendEntries(rpcCtx, req)
		cancel() // Always clean up the context

		if lastErr == nil {
			// Success - return immediately
			return resp, nil
		}

		// Don't sleep after the last attempt
		if attempt < MaxRPCRetries-1 {
			// Exponential backoff: 10ms, 20ms, 30ms, etc.
			backoff := RetryBackoffBase * time.Duration(attempt+1)
			time.Sleep(backoff)
		}
	}

	// All retries exhausted - log once here
	log.Printf("AppendEntries to %s failed after %d attempts: %v", peerID, MaxRPCRetries, lastErr)
	return nil, fmt.Errorf("AppendEntries to %s failed after %d attempts: %w", peerID, MaxRPCRetries, lastErr)
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

func NewTransport(peerIDs []ServerID) *Transport {
	transport := &Transport{clientsConnPool: &sync.Map{}}

	transport.initClients(peerIDs)

	return transport
}
