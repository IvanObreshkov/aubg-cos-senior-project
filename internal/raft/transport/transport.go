package transport

import (
	"aubg-cos-senior-project/internal/raft"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"sync"
)

type Transport struct {
	// A map to store gRPC clients for each peer address. This acts as a conn pool.
	clients map[raft.ServerAddress]RaftServiceClient
	// We use a mutex to make access to the map thread-safe.
	mutex sync.RWMutex
}

func (t *Transport) RequestVote(ctx context.Context, peerAddress raft.ServerAddress, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	t.mutex.RLock()
	client, ok := t.clients[peerAddress]
	t.mutex.Unlock()

	if !ok {
		return nil, fmt.Errorf("gRPC client not found for server with addr: %v", peerAddress)
	}

	// TODO: Maybe add a Timeout to the ctx
	return client.RequestVote(ctx, req)
}

func (t *Transport) initClients(serverPeers []raft.ServerAddress) {
	for _, addr := range serverPeers {
		conn, err := grpc.NewClient(string(addr))
		if err != nil {
			log.Printf("Failed establishing a gRPC channel to peer with addr: %v. Err: %v", addr, err)
			// Failing to establish a connection to a single Node, should not prevent conn to other nodes, so log it
			// and continue
			continue
		}

		client := NewRaftServiceClient(conn)

		t.mutex.Lock()
		t.clients[addr] = client
		t.mutex.Unlock()
	}
}

func NewTransport(serverPeers []raft.ServerAddress) *Transport {
	return &Transport{
		clients: nil,
		mutex:   sync.RWMutex{},
	}
}
