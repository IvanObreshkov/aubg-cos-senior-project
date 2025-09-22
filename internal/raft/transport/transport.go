package transport

import (
	"aubg-cos-senior-project/internal/raft"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"sync"
)

type Transport struct {
	// A map to store the underlying grpc.ClientConn for each peer address.
	// sync.Map provides thread-safe access to the map, and is optimized for read operations, reducing the overhead of
	// locks
	clientsConnPool *sync.Map
}

func (t *Transport) RequestVote(ctx context.Context, peerAddress raft.ServerAddress, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	clientConn, ok := t.clientsConnPool.Load(peerAddress)
	if !ok {
		return nil, fmt.Errorf("gRPC client connection not found for server with addr: %v", peerAddress)
	}

	// We must type assert the value returned by Load.
	conn, ok := clientConn.(*grpc.ClientConn)
	if !ok {
		return nil, fmt.Errorf("invalid clientConn type for server with addr: %v. Type is %T", peerAddress,
			clientConn)
	}

	// Create the RaftServiceClient on the fly, as this is just a wrapper around the connection
	client := NewRaftServiceClient(conn)

	// TODO: Maybe add a Timeout to the ctx
	return client.RequestVote(ctx, req)
}

// Initializes a gRPC channel from the current server to every other from its peers
func (t *Transport) initClients(serverPeers []raft.ServerAddress) {
	for _, addr := range serverPeers {
		conn, err := grpc.NewClient(string(addr), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed establishing a gRPC channel to peer with addr: %v. Err: %v", addr, err)
			// Failing to establish a connection to a single Node, should not prevent conn to other nodes, so log it
			// and continue
			continue
		}

		t.clientsConnPool.Store(addr, conn)
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

func NewTransport(serverPeers []raft.ServerAddress) *Transport {
	transport := &Transport{clientsConnPool: &sync.Map{}}

	transport.initClients(serverPeers)

	return transport
}
