package state_machine

import (
	"aubg-cos-senior-project/internal/raft/proto"
	"log"
	"strings"
	"sync"
)

// KVStateMachine is a simple key-value store that implements the StateMachine interface
type KVStateMachine struct {
	mu    sync.RWMutex
	store map[string]string
	id    string // Server ID for logging
}

// NewKVStateMachine creates a new key-value state machine
func NewKVStateMachine(serverID string) *KVStateMachine {
	return &KVStateMachine{
		store: make(map[string]string),
		id:    serverID,
	}
}

// Apply applies log entries to the state machine
// Commands are expected to be in the format: "SET key=value" or "DEL key"
func (kv *KVStateMachine) Apply(logs []proto.LogEntry) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for _, entry := range logs {
		if entry.Type != proto.LogEntryType_LOG_COMMAND {
			// Skip non-command entries (e.g., configuration changes)
			continue
		}

		command := string(entry.Command)
		parts := strings.Fields(command)

		if len(parts) == 0 {
			continue
		}

		op := strings.ToUpper(parts[0])
		switch op {
		case "SET":
			if len(parts) >= 2 {
				// Parse "key=value"
				kvPair := strings.SplitN(parts[1], "=", 2)
				if len(kvPair) == 2 {
					key := kvPair[0]
					value := kvPair[1]
					kv.store[key] = value
					log.Printf("[KV-SM-%s] Applied SET: %s=%s (index=%d)",
						kv.id, key, value, entry.Index)
				}
			}
		case "DEL":
			if len(parts) >= 2 {
				key := parts[1]
				delete(kv.store, key)
				log.Printf("[KV-SM-%s] Applied DEL: %s (index=%d)",
					kv.id, key, entry.Index)
			}
		default:
			log.Printf("[KV-SM-%s] Unknown command: %s (index=%d)",
				kv.id, command, entry.Index)
		}
	}
}
