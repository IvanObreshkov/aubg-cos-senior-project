package swim

import (
	"sync"
	"time"
)

// GossipManager handles infection-style dissemination of membership updates
// Section 4.4: "Updates are sent via piggybacking on ping and ack messages"
// Section 5: "infection-style dissemination component... multicast this information"
type GossipManager struct {
	mu                 sync.RWMutex
	updates            []gossipUpdate
	maxRetransmissions int // ξ in the paper (Section 5: "ξ=2 in current implementation")
	maxPacketSize      int // Maximum size of piggybacked updates
}

// gossipUpdate tracks a membership update and how many times it's been transmitted
type gossipUpdate struct {
	Update        Update
	Transmissions int       // Number of times this update has been piggybacked
	FirstSeenTime time.Time // When we first received/created this update
}

// NewGossipManager creates a new gossip manager
func NewGossipManager(maxRetransmissions, maxPacketSize int) *GossipManager {
	return &GossipManager{
		updates:            make([]gossipUpdate, 0),
		maxRetransmissions: maxRetransmissions,
		maxPacketSize:      maxPacketSize,
	}
}

// AddUpdate adds a new membership update to be gossiped
// Section 5: "when a membership update is received at member Mi, it is queued for piggybacking"
func (gm *GossipManager) AddUpdate(update Update) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	// Check if we already have this update
	for i, existing := range gm.updates {
		if existing.Update.MemberID == update.MemberID {
			// Only replace if this is newer information
			if update.Incarnation > existing.Update.Incarnation ||
				(update.Incarnation == existing.Update.Incarnation &&
					shouldOverride(existing.Update.Status, update.Status)) {
				gm.updates[i] = gossipUpdate{
					Update:        update,
					Transmissions: 0,
					FirstSeenTime: time.Now(),
				}
			}
			return
		}
	}

	// Add new update
	gm.updates = append(gm.updates, gossipUpdate{
		Update:        update,
		Transmissions: 0,
		FirstSeenTime: time.Now(),
	})
}

// GetUpdatesToGossip returns updates to piggyback on a message
// Section 5: "piggyback up to the MTU (maximum transmission unit) worth of recent updates"
func (gm *GossipManager) GetUpdatesToGossip(maxUpdates int) []Update {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if len(gm.updates) == 0 {
		return nil
	}

	// Prioritize updates that haven't been sent as much
	// This implements the "random" selection mentioned in Section 5
	result := make([]Update, 0, maxUpdates)
	estimatedSize := 0

	for i := 0; i < len(gm.updates) && len(result) < maxUpdates; i++ {
		update := &gm.updates[i]

		// Estimate size (rough approximation)
		updateSize := len(update.Update.MemberID) + len(update.Update.Address) + 50
		if estimatedSize+updateSize > gm.maxPacketSize {
			break
		}

		result = append(result, update.Update)
		update.Transmissions++
		estimatedSize += updateSize
	}

	// Remove updates that have been transmitted enough times
	// Section 5: "piggybacked on ping or ack messages for ξ protocol periods"
	gm.cleanupTransmittedUpdates()

	return result
}

// cleanupTransmittedUpdates removes updates that have been transmitted enough times
func (gm *GossipManager) cleanupTransmittedUpdates() {
	newUpdates := make([]gossipUpdate, 0, len(gm.updates))
	for _, update := range gm.updates {
		if update.Transmissions < gm.maxRetransmissions {
			newUpdates = append(newUpdates, update)
		}
	}
	gm.updates = newUpdates
}

// ProcessIncomingUpdates processes updates from piggybacked gossip
// Section 4.4: "disseminate membership updates... piggybacking on ping and ack messages"
func (gm *GossipManager) ProcessIncomingUpdates(updates []Update, memberList *MemberList, onUpdate func(Update, MemberStatus)) {
	for _, update := range updates {
		// Try to apply the update to our membership list, getting the old status
		changed, oldStatus := memberList.AddMember(update.MemberID, update.Address, update.Status, update.Incarnation)

		if changed {
			// If this is new information, queue it for further gossip
			// Section 5: "infection-style dissemination ensures that membership updates are reliably propagated"
			gm.AddUpdate(update)

			// Notify about the update with old status
			if onUpdate != nil {
				onUpdate(update, oldStatus)
			}
		}
	}
}

// Clear removes all pending updates
func (gm *GossipManager) Clear() {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	gm.updates = make([]gossipUpdate, 0)
}

// NumPendingUpdates returns the number of updates waiting to be gossiped
func (gm *GossipManager) NumPendingUpdates() int {
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	return len(gm.updates)
}
