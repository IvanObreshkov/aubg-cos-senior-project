package main

import (
	"fmt"
	"time"
)

// monitorLogReplication continuously monitors server state changes to detect
// log replication events. This goroutine runs until shutdown and checks
// server states every 200ms to detect changes in:
// - Log indices (new entries appended)
// - Commit indices (entries committed)
// - Applied indices (entries applied to state machine)
// - Terms (elections)
// - Server states (Leader/Follower/Candidate transitions)
func monitorLogReplication() {
	ticker := time.NewTicker(200 * time.Millisecond) // Check 5 times per second
	defer ticker.Stop()

	for {
		select {
		case <-shutdownCtx.Done():
			// Shutdown signal received, stop monitoring
			return
		case <-ticker.C:
			monitorAllServers()
		}
	}
}

// monitorAllServers checks each server's state and detects changes.
func monitorAllServers() {
	for i, addr := range serverAddrs {
		serverName := fmt.Sprintf("Server-%d", i+1)
		state := queryServerState(addr)
		if state == nil {
			continue
		}

		currentInfo := ServerInfo{
			ID:           serverName,
			State:        state.State,
			Term:         state.CurrentTerm,
			CommitIndex:  state.CommitIndex,
			LastApplied:  state.LastApplied,
			LastLogIndex: state.LastLogIndex,
		}

		lastStateMu.Lock()
		lastInfo, exists := lastServerState[serverName]
		isLeader := currentInfo.State == "Leader"

		// Detect and record various state changes
		detectLogAppend(serverName, currentInfo, lastInfo, exists, isLeader)
		detectCommit(serverName, currentInfo, lastInfo, exists, isLeader)
		detectStateApplication(serverName, currentInfo, lastInfo, exists)
		detectTermChange(serverName, currentInfo, lastInfo, exists)
		detectStateChange(serverName, currentInfo, lastInfo, exists)

		lastServerState[serverName] = currentInfo
		lastStateMu.Unlock()
	}
}

// detectLogAppend detects when new entries are appended to a server's log.
// For leaders: this happens when a client command is received.
// For followers: this happens when AppendEntries RPC is received from leader.
func detectLogAppend(serverName string, current, last ServerInfo, exists, isLeader bool) {
	if !exists || current.LastLogIndex <= last.LastLogIndex {
		return
	}

	entriesAdded := current.LastLogIndex - last.LastLogIndex
	var message string
	var eventType string

	if isLeader {
		// Leader appended command from client
		message = fmt.Sprintf("%s [LEADER] Appended command to log at index %d", serverName, current.LastLogIndex)
		eventType = "log_appended"
	} else {
		// Follower received AppendEntries RPC from leader
		message = fmt.Sprintf("%s [RPC] Received AppendEntries from leader → appended %d entries (index: %d → %d)",
			serverName, entriesAdded, last.LastLogIndex, current.LastLogIndex)
		eventType = "rpc_append_entries"
	}

	addEvent(EventMessage{
		Type:      eventType,
		ServerID:  serverName,
		Message:   message,
		Timestamp: time.Now(),
		Details: map[string]any{
			"oldIndex":   last.LastLogIndex,
			"newIndex":   current.LastLogIndex,
			"isLeader":   isLeader,
			"numEntries": entriesAdded,
		},
	})
}

// detectCommit detects when entries are committed (made durable).
// For leaders: this happens when majority of servers have replicated the entry.
// For followers: this happens when leader informs them via heartbeat.
func detectCommit(serverName string, current, last ServerInfo, exists, isLeader bool) {
	if !exists || current.CommitIndex <= last.CommitIndex {
		return
	}

	var message string
	if isLeader {
		// Leader committed after receiving majority acknowledgments
		message = fmt.Sprintf("%s [LEADER] ✓ Entry %d is now COMMITTED (majority replicated)",
			serverName, current.CommitIndex)
	} else {
		// Follower learned about commit from leader's heartbeat
		message = fmt.Sprintf("%s Updated commitIndex from %d to %d (learned from leader)",
			serverName, last.CommitIndex, current.CommitIndex)
	}

	addEvent(EventMessage{
		Type:      "entry_committed",
		ServerID:  serverName,
		Message:   message,
		Timestamp: time.Now(),
		Details: map[string]any{
			"oldCommit": last.CommitIndex,
			"newCommit": current.CommitIndex,
			"isLeader":  isLeader,
		},
	})
}

// detectStateApplication detects when committed entries are applied to the state machine.
// This is when the actual KV store operations (SET, DEL) are executed.
func detectStateApplication(serverName string, current, last ServerInfo, exists bool) {
	if !exists || current.LastApplied <= last.LastApplied {
		return
	}

	entriesApplied := current.LastApplied - last.LastApplied
	message := fmt.Sprintf("%s Applied %d log entries to state machine (lastApplied: %d → %d)",
		serverName, entriesApplied, last.LastApplied, current.LastApplied)

	addEvent(EventMessage{
		Type:      "entry_applied",
		ServerID:  serverName,
		Message:   message,
		Timestamp: time.Now(),
		Details: map[string]any{
			"oldApplied": last.LastApplied,
			"newApplied": current.LastApplied,
			"numApplied": entriesApplied,
		},
	})
}

// detectTermChange detects when a server's term increases (elections happening).
func detectTermChange(serverName string, current, last ServerInfo, exists bool) {
	if !exists || current.Term <= last.Term {
		return
	}

	message := fmt.Sprintf("%s [TERM-%d→%d] Term increased",
		serverName, last.Term, current.Term)

	addEvent(EventMessage{
		Type:      "term_changed",
		ServerID:  serverName,
		Message:   message,
		Timestamp: time.Now(),
		Details: map[string]any{
			"oldTerm": last.Term,
			"newTerm": current.Term,
		},
	})
}

// detectStateChange detects when a server transitions between states
// (Follower ↔ Candidate ↔ Leader).
func detectStateChange(serverName string, current, last ServerInfo, exists bool) {
	if !exists || current.State == last.State {
		return
	}

	message := fmt.Sprintf("%s Transitioning %s → %s",
		serverName, last.State, current.State)

	addEvent(EventMessage{
		Type:      "state_changed",
		ServerID:  serverName,
		Message:   message,
		Timestamp: time.Now(),
		Details: map[string]any{
			"oldState": last.State,
			"newState": current.State,
		},
	})
}
