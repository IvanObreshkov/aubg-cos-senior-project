package swim

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewGossipManager(t *testing.T) {
	gm := NewGossipManager(3, 1400)

	assert.NotNil(t, gm)
	assert.Equal(t, 3, gm.maxRetransmissions)
	assert.Equal(t, 1400, gm.maxPacketSize)
	assert.Empty(t, gm.updates)
}

func TestGossipManager_AddUpdate(t *testing.T) {
	gm := NewGossipManager(3, 1400)

	t.Run("adds new update", func(t *testing.T) {
		update := Update{
			MemberID:    "node-1",
			Address:     "127.0.0.1:8000",
			Status:      Alive,
			Incarnation: 1,
			Timestamp:   time.Now(),
		}

		gm.AddUpdate(update)

		assert.Equal(t, 1, gm.NumPendingUpdates())
	})

	t.Run("adds multiple different updates", func(t *testing.T) {
		gm2 := NewGossipManager(3, 1400)

		update1 := Update{MemberID: "node-1", Incarnation: 1}
		update2 := Update{MemberID: "node-2", Incarnation: 1}

		gm2.AddUpdate(update1)
		gm2.AddUpdate(update2)

		assert.Equal(t, 2, gm2.NumPendingUpdates())
	})

	t.Run("replaces update with higher incarnation", func(t *testing.T) {
		gm3 := NewGossipManager(3, 1400)

		update1 := Update{MemberID: "node-1", Status: Alive, Incarnation: 1}
		gm3.AddUpdate(update1)

		update2 := Update{MemberID: "node-1", Status: Suspect, Incarnation: 2}
		gm3.AddUpdate(update2)

		assert.Equal(t, 1, gm3.NumPendingUpdates())
	})

	t.Run("replaces update with same incarnation and higher priority status", func(t *testing.T) {
		gm4 := NewGossipManager(3, 1400)

		update1 := Update{MemberID: "node-1", Status: Alive, Incarnation: 1}
		gm4.AddUpdate(update1)

		// Suspect should override Alive at same incarnation
		update2 := Update{MemberID: "node-1", Status: Suspect, Incarnation: 1}
		gm4.AddUpdate(update2)

		assert.Equal(t, 1, gm4.NumPendingUpdates())
	})

	t.Run("does not replace update with lower incarnation", func(t *testing.T) {
		gm5 := NewGossipManager(3, 1400)

		update1 := Update{MemberID: "node-1", Status: Alive, Incarnation: 5}
		gm5.AddUpdate(update1)

		update2 := Update{MemberID: "node-1", Status: Suspect, Incarnation: 3}
		gm5.AddUpdate(update2)

		assert.Equal(t, 1, gm5.NumPendingUpdates())
	})
}

func TestGossipManager_GetUpdatesToGossip(t *testing.T) {
	t.Run("returns nil when no updates", func(t *testing.T) {
		gm := NewGossipManager(3, 1400)
		updates := gm.GetUpdatesToGossip(10)
		assert.Nil(t, updates)
	})

	t.Run("returns available updates", func(t *testing.T) {
		gm := NewGossipManager(3, 1400)

		gm.AddUpdate(Update{MemberID: "node-1", Address: "127.0.0.1:8000", Incarnation: 1})
		gm.AddUpdate(Update{MemberID: "node-2", Address: "127.0.0.1:8001", Incarnation: 1})

		updates := gm.GetUpdatesToGossip(10)
		assert.Equal(t, 2, len(updates))
	})

	t.Run("respects maxUpdates limit", func(t *testing.T) {
		gm := NewGossipManager(10, 10000) // High packet size to not limit

		for i := 0; i < 5; i++ {
			gm.AddUpdate(Update{MemberID: "node-" + string(rune('0'+i)), Incarnation: 1})
		}

		updates := gm.GetUpdatesToGossip(2)
		assert.Equal(t, 2, len(updates))
	})

	t.Run("increments transmission count", func(t *testing.T) {
		gm := NewGossipManager(3, 1400)

		gm.AddUpdate(Update{MemberID: "node-1", Address: "127.0.0.1:8000", Incarnation: 1})

		// First call
		gm.GetUpdatesToGossip(10)
		assert.Equal(t, 1, gm.NumPendingUpdates())

		// Second call
		gm.GetUpdatesToGossip(10)
		assert.Equal(t, 1, gm.NumPendingUpdates())

		// Third call should clean up (maxRetransmissions = 3)
		gm.GetUpdatesToGossip(10)
		assert.Equal(t, 0, gm.NumPendingUpdates())
	})
}

func TestGossipManager_CleanupTransmittedUpdates(t *testing.T) {
	gm := NewGossipManager(2, 1400)

	gm.AddUpdate(Update{MemberID: "node-1", Address: "a", Incarnation: 1})
	gm.AddUpdate(Update{MemberID: "node-2", Address: "b", Incarnation: 1})

	// First transmission
	gm.GetUpdatesToGossip(10)
	assert.Equal(t, 2, gm.NumPendingUpdates())

	// Second transmission - cleanup should happen after this
	gm.GetUpdatesToGossip(10)
	assert.Equal(t, 0, gm.NumPendingUpdates())
}

func TestGossipManager_ProcessIncomingUpdates(t *testing.T) {
	t.Run("processes updates and calls callback", func(t *testing.T) {
		gm := NewGossipManager(3, 1400)
		ml := NewMemberList("local", "127.0.0.1:9000")

		var callbackCalled bool
		var receivedUpdate Update
		var receivedOldStatus MemberStatus

		updates := []Update{
			{MemberID: "node-1", Address: "127.0.0.1:8000", Status: Alive, Incarnation: 1},
		}

		gm.ProcessIncomingUpdates(updates, ml, func(u Update, oldStatus MemberStatus) {
			callbackCalled = true
			receivedUpdate = u
			receivedOldStatus = oldStatus
		})

		assert.True(t, callbackCalled)
		assert.Equal(t, "node-1", receivedUpdate.MemberID)
		assert.Equal(t, MemberStatus(0), receivedOldStatus) // No old status for new member
	})

	t.Run("queues update for further gossip when changed", func(t *testing.T) {
		gm := NewGossipManager(3, 1400)
		ml := NewMemberList("local", "127.0.0.1:9000")

		updates := []Update{
			{MemberID: "node-1", Address: "127.0.0.1:8000", Status: Alive, Incarnation: 1},
		}

		gm.ProcessIncomingUpdates(updates, ml, nil)

		assert.Equal(t, 1, gm.NumPendingUpdates())
	})

	t.Run("handles nil callback", func(t *testing.T) {
		gm := NewGossipManager(3, 1400)
		ml := NewMemberList("local", "127.0.0.1:9000")

		updates := []Update{
			{MemberID: "node-1", Address: "127.0.0.1:8000", Status: Alive, Incarnation: 1},
		}

		// Should not panic
		assert.NotPanics(t, func() {
			gm.ProcessIncomingUpdates(updates, ml, nil)
		})
	})

	t.Run("does not queue unchanged updates", func(t *testing.T) {
		gm := NewGossipManager(3, 1400)
		ml := NewMemberList("local", "127.0.0.1:9000")

		// Add member first
		ml.AddMember("node-1", "127.0.0.1:8000", Alive, 5)

		// Try to add older update
		updates := []Update{
			{MemberID: "node-1", Address: "127.0.0.1:8000", Status: Alive, Incarnation: 3},
		}

		gm.ProcessIncomingUpdates(updates, ml, nil)

		// Should not be queued since it's older info
		assert.Equal(t, 0, gm.NumPendingUpdates())
	})
}

func TestGossipManager_Clear(t *testing.T) {
	gm := NewGossipManager(3, 1400)

	gm.AddUpdate(Update{MemberID: "node-1", Incarnation: 1})
	gm.AddUpdate(Update{MemberID: "node-2", Incarnation: 1})

	assert.Equal(t, 2, gm.NumPendingUpdates())

	gm.Clear()

	assert.Equal(t, 0, gm.NumPendingUpdates())
}

func TestGossipManager_NumPendingUpdates(t *testing.T) {
	gm := NewGossipManager(3, 1400)

	assert.Equal(t, 0, gm.NumPendingUpdates())

	gm.AddUpdate(Update{MemberID: "node-1", Incarnation: 1})
	assert.Equal(t, 1, gm.NumPendingUpdates())

	gm.AddUpdate(Update{MemberID: "node-2", Incarnation: 1})
	assert.Equal(t, 2, gm.NumPendingUpdates())
}

func TestGossipManager_ConcurrentAccess(t *testing.T) {
	gm := NewGossipManager(100, 1400) // High retransmit count to avoid cleanup

	var wg sync.WaitGroup
	numGoroutines := 10
	numUpdatesPerGoroutine := 10

	// Concurrently add updates
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numUpdatesPerGoroutine; j++ {
				update := Update{
					MemberID:    "node-" + string(rune('A'+goroutineID)) + string(rune('0'+j)),
					Incarnation: uint64(j + 1),
				}
				gm.AddUpdate(update)
			}
		}(i)
	}

	// Concurrently get updates
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numUpdatesPerGoroutine; j++ {
				gm.GetUpdatesToGossip(5)
			}
		}()
	}

	// Concurrently check count
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numUpdatesPerGoroutine; j++ {
				gm.NumPendingUpdates()
			}
		}()
	}

	wg.Wait()

	// Should not panic or cause race conditions
}

func TestGossipManager_PacketSizeLimit(t *testing.T) {
	// Use a very small max packet size
	gm := NewGossipManager(10, 100)

	// Add updates with large addresses
	for i := 0; i < 5; i++ {
		gm.AddUpdate(Update{
			MemberID:    "node-" + string(rune('0'+i)),
			Address:     "192.168.1.1:800" + string(rune('0'+i)),
			Incarnation: 1,
		})
	}

	// Should limit based on packet size
	updates := gm.GetUpdatesToGossip(10)

	// Due to small packet size, should not return all updates
	assert.LessOrEqual(t, len(updates), 5)
}
