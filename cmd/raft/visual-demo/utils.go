package main

import (
	"aubg-cos-senior-project/internal/raft/proto"
	"aubg-cos-senior-project/internal/raft/server"
	"aubg-cos-senior-project/internal/raft/state_machine"
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// addEvent adds an event to both the global event log and the per-server event log.
func addEvent(event EventMessage) {
	eventsMu.Lock()
	defer eventsMu.Unlock()
	events = append(events, event)
	if len(events) > maxEvents {
		events = events[len(events)-maxEvents:]
	}

	// Also add to per-server event log if it's a server event
	if event.ServerID != "" && event.ServerID != "Client" && event.ServerID != "Auto-Client" {
		serverEventsMu.Lock()
		defer serverEventsMu.Unlock()

		if serverEvents[event.ServerID] == nil {
			serverEvents[event.ServerID] = []EventMessage{}
		}
		serverEvents[event.ServerID] = append(serverEvents[event.ServerID], event)
		if len(serverEvents[event.ServerID]) > maxServerEvents {
			serverEvents[event.ServerID] = serverEvents[event.ServerID][len(serverEvents[event.ServerID])-maxServerEvents:]
		}
	}
}

// getServerName returns the friendly name (Server-1, Server-2) for a server UUID.
// Falls back to showing truncated UUID if mapping doesn't exist.
func getServerName(serverID server.ServerID) string {
	serverIDToNameMu.RLock()
	defer serverIDToNameMu.RUnlock()
	if name, ok := serverIDToName[serverID]; ok {
		return name
	}
	// Fallback to showing truncated UUID
	idStr := string(serverID)
	if len(idStr) > 12 {
		return idStr[:12] + "..."
	}
	return idStr
}

// queryServerState queries a Raft server for its current state via gRPC.
// Returns nil if the server is unreachable or the query times out.
func queryServerState(addr string) *proto.GetServerStateResponse {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	defer conn.Close()

	client := proto.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resp, err := client.GetServerState(ctx, &proto.GetServerStateRequest{})
	if err != nil {
		return nil
	}

	return resp
}

// getLogEntries retrieves the most recent log entries from a server.
// Returns a simplified string representation for display purposes.
func getLogEntries(addr string, count int) []string {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	defer conn.Close()

	client := proto.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Get server state first to know the log size
	state, err := client.GetServerState(ctx, &proto.GetServerStateRequest{})
	if err != nil || state.LastLogIndex == 0 {
		return []string{}
	}

	// Get recent entries
	var entries []string
	startIndex := uint64(1)
	if state.LastLogIndex > uint64(count) {
		startIndex = state.LastLogIndex - uint64(count) + 1
	}

	for i := startIndex; i <= state.LastLogIndex; i++ {
		entries = append(entries, fmt.Sprintf("Entry %d", i))
	}

	return entries
}

// getStateMachineState retrieves the current key-value pairs from a server's state machine.
// Returns nil if the server doesn't exist or doesn't have a KV state machine.
func getStateMachineState(serverIndex int) map[string]string {
	if serverIndex < 0 || serverIndex >= len(servers) {
		return nil
	}

	srv := servers[serverIndex]
	if srv == nil || srv.StateMachine == nil {
		return nil
	}

	// Type assert to KVStateMachine to access the GetAll method
	kvSM, ok := srv.StateMachine.(*state_machine.KVStateMachine)
	if !ok {
		return nil
	}

	// Get all key-value pairs from the state machine
	return kvSM.GetAll()
}

// findLeader finds the current cluster leader by querying all servers.
// Returns the leader's address, or empty string if no leader exists.
func findLeader() string {
	for _, addr := range serverAddrs {
		if state := queryServerState(addr); state != nil && state.State == "Leader" {
			return addr
		}
	}
	return ""
}

// submitCommand submits a command to the specified server (typically the leader).
// Returns success status and the log index where the command was stored.
func submitCommand(addr string, cmd string) (bool, uint64) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false, 0
	}
	defer conn.Close()

	client := proto.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.ClientCommand(ctx, &proto.ClientCommandRequest{
		Command: []byte(cmd),
	})
	if err != nil {
		return false, 0
	}

	return resp.Success, resp.Index
}
