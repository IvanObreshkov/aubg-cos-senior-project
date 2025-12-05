package swim

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMemberList(t *testing.T) {
	ml := NewMemberList("node-1", "127.0.0.1:8000")

	assert.NotNil(t, ml)
	assert.NotNil(t, ml.members)
	assert.NotNil(t, ml.local)
	assert.Equal(t, "node-1", ml.local.ID)
	assert.Equal(t, "127.0.0.1:8000", ml.local.Address)
	assert.Equal(t, Alive, ml.local.Status)
	assert.Equal(t, uint64(0), ml.local.Incarnation)
}

func TestMemberList_AddMember(t *testing.T) {
	t.Run("adds new member", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")

		changed, oldStatus := ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)

		assert.True(t, changed)
		assert.Equal(t, MemberStatus(0), oldStatus) // No old status for new member
		assert.Equal(t, 1, ml.NumMembers())
	})

	t.Run("updates member with higher incarnation", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")
		ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)

		changed, oldStatus := ml.AddMember("node-1", "127.0.0.1:8000", Suspect, 2)

		assert.True(t, changed)
		assert.Equal(t, Alive, oldStatus)

		member, _ := ml.GetMember("node-1")
		assert.Equal(t, Suspect, member.Status)
		assert.Equal(t, uint64(2), member.Incarnation)
	})

	t.Run("updates member with same incarnation and priority status", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")
		ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)

		// Suspect should override Alive
		changed, oldStatus := ml.AddMember("node-1", "127.0.0.1:8000", Suspect, 1)

		assert.True(t, changed)
		assert.Equal(t, Alive, oldStatus)
	})

	t.Run("does not update member with lower incarnation", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")
		ml.AddMember("node-1", "127.0.0.1:8000", Alive, 5)

		changed, oldStatus := ml.AddMember("node-1", "127.0.0.1:8000", Suspect, 3)

		assert.False(t, changed)
		assert.Equal(t, Alive, oldStatus)

		member, _ := ml.GetMember("node-1")
		assert.Equal(t, Alive, member.Status)
	})

	t.Run("does not update member with same incarnation and no priority", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")
		ml.AddMember("node-1", "127.0.0.1:8000", Suspect, 1)

		// Same status shouldn't update
		changed, _ := ml.AddMember("node-1", "127.0.0.1:8000", Suspect, 1)

		assert.False(t, changed)
	})
}

func TestShouldOverride(t *testing.T) {
	tests := []struct {
		name      string
		oldStatus MemberStatus
		newStatus MemberStatus
		expected  bool
	}{
		{"same status", Alive, Alive, false},
		{"Alive overrides Suspect", Suspect, Alive, true},
		{"Suspect overrides Alive", Alive, Suspect, true},
		{"Failed overrides Alive", Alive, Failed, true},
		{"Failed overrides Suspect", Suspect, Failed, true},
		{"Left overrides everything", Alive, Left, true},
		{"Left overrides Suspect", Suspect, Left, true},
		{"Left overrides Failed", Failed, Left, true},
		{"Alive does not override Failed", Failed, Alive, false},
		{"Suspect does not override Failed", Failed, Suspect, false},
		{"Failed does not override Left", Left, Failed, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldOverride(tt.oldStatus, tt.newStatus)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMemberList_GetMember(t *testing.T) {
	ml := NewMemberList("local", "127.0.0.1:9000")
	ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)

	t.Run("returns existing member", func(t *testing.T) {
		member, exists := ml.GetMember("node-1")

		assert.True(t, exists)
		assert.Equal(t, "node-1", member.ID)
		assert.Equal(t, "127.0.0.1:8000", member.Address)
	})

	t.Run("returns false for non-existing member", func(t *testing.T) {
		member, exists := ml.GetMember("node-999")

		assert.False(t, exists)
		assert.Nil(t, member)
	})
}

func TestMemberList_UpdateMemberStatus(t *testing.T) {
	t.Run("updates status with higher incarnation", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")
		ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)

		updated := ml.UpdateMemberStatus("node-1", Suspect, 2)

		assert.True(t, updated)
		member, _ := ml.GetMember("node-1")
		assert.Equal(t, Suspect, member.Status)
	})

	t.Run("updates status with same incarnation and priority", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")
		ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)

		updated := ml.UpdateMemberStatus("node-1", Failed, 1)

		assert.True(t, updated)
		member, _ := ml.GetMember("node-1")
		assert.Equal(t, Failed, member.Status)
	})

	t.Run("does not update non-existing member", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")

		updated := ml.UpdateMemberStatus("node-999", Suspect, 1)

		assert.False(t, updated)
	})

	t.Run("does not update with lower incarnation", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")
		ml.AddMember("node-1", "127.0.0.1:8000", Alive, 5)

		updated := ml.UpdateMemberStatus("node-1", Suspect, 3)

		assert.False(t, updated)
		member, _ := ml.GetMember("node-1")
		assert.Equal(t, Alive, member.Status)
	})
}

func TestMemberList_RemoveMember(t *testing.T) {
	ml := NewMemberList("local", "127.0.0.1:9000")
	ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)

	assert.Equal(t, 1, ml.NumMembers())

	ml.RemoveMember("node-1")

	assert.Equal(t, 0, ml.NumMembers())
	_, exists := ml.GetMember("node-1")
	assert.False(t, exists)
}

func TestMemberList_GetMembers(t *testing.T) {
	ml := NewMemberList("local", "127.0.0.1:9000")
	ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)
	ml.AddMember("node-2", "127.0.0.1:8001", Suspect, 2)
	ml.AddMember("node-3", "127.0.0.1:8002", Failed, 3)

	members := ml.GetMembers()

	assert.Equal(t, 3, len(members))

	// Verify copies are returned
	for _, m := range members {
		if m.ID == "node-1" {
			m.Status = Failed
		}
	}
	original, _ := ml.GetMember("node-1")
	assert.Equal(t, Alive, original.Status) // Should not be changed
}

func TestMemberList_GetAliveMembers(t *testing.T) {
	ml := NewMemberList("local", "127.0.0.1:9000")
	ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)
	ml.AddMember("node-2", "127.0.0.1:8001", Alive, 2)
	ml.AddMember("node-3", "127.0.0.1:8002", Suspect, 3)
	ml.AddMember("node-4", "127.0.0.1:8003", Failed, 4)

	aliveMembers := ml.GetAliveMembers()

	assert.Equal(t, 2, len(aliveMembers))
	for _, m := range aliveMembers {
		assert.Equal(t, Alive, m.Status)
	}
}

func TestMemberList_GetRandomMember(t *testing.T) {
	t.Run("returns nil when no members", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")

		member := ml.GetRandomMember()

		assert.Nil(t, member)
	})

	t.Run("returns a member when available", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")
		ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)

		member := ml.GetRandomMember()

		assert.NotNil(t, member)
		assert.Equal(t, "node-1", member.ID)
	})

	t.Run("excludes specified IDs", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")
		ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)
		ml.AddMember("node-2", "127.0.0.1:8001", Alive, 2)

		member := ml.GetRandomMember("node-1")

		assert.NotNil(t, member)
		assert.Equal(t, "node-2", member.ID)
	})

	t.Run("returns nil when all are excluded", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")
		ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)

		member := ml.GetRandomMember("node-1")

		assert.Nil(t, member)
	})

	t.Run("excludes failed and left members", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")
		ml.AddMember("node-1", "127.0.0.1:8000", Failed, 1)
		ml.AddMember("node-2", "127.0.0.1:8001", Left, 2)
		ml.AddMember("node-3", "127.0.0.1:8002", Alive, 3)

		member := ml.GetRandomMember()

		assert.NotNil(t, member)
		assert.Equal(t, "node-3", member.ID)
	})

	t.Run("includes suspect members", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")
		ml.AddMember("node-1", "127.0.0.1:8000", Suspect, 1)

		member := ml.GetRandomMember()

		assert.NotNil(t, member)
		assert.Equal(t, "node-1", member.ID)
	})
}

func TestMemberList_GetRandomMembers(t *testing.T) {
	t.Run("returns nil when no members", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")

		members := ml.GetRandomMembers(3)

		assert.Nil(t, members)
	})

	t.Run("returns requested count", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")
		ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)
		ml.AddMember("node-2", "127.0.0.1:8001", Alive, 2)
		ml.AddMember("node-3", "127.0.0.1:8002", Alive, 3)

		members := ml.GetRandomMembers(2)

		assert.Equal(t, 2, len(members))
	})

	t.Run("returns all when fewer available than requested", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")
		ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)
		ml.AddMember("node-2", "127.0.0.1:8001", Alive, 2)

		members := ml.GetRandomMembers(5)

		assert.Equal(t, 2, len(members))
	})

	t.Run("excludes specified IDs", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")
		ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)
		ml.AddMember("node-2", "127.0.0.1:8001", Alive, 2)
		ml.AddMember("node-3", "127.0.0.1:8002", Alive, 3)

		members := ml.GetRandomMembers(3, "node-1", "node-2")

		assert.Equal(t, 1, len(members))
		assert.Equal(t, "node-3", members[0].ID)
	})

	t.Run("excludes failed and left members", func(t *testing.T) {
		ml := NewMemberList("local", "127.0.0.1:9000")
		ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)
		ml.AddMember("node-2", "127.0.0.1:8001", Failed, 2)
		ml.AddMember("node-3", "127.0.0.1:8002", Left, 3)

		members := ml.GetRandomMembers(3)

		assert.Equal(t, 1, len(members))
		assert.Equal(t, "node-1", members[0].ID)
	})
}

func TestMemberList_NumMembers(t *testing.T) {
	ml := NewMemberList("local", "127.0.0.1:9000")

	assert.Equal(t, 0, ml.NumMembers())

	ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)
	assert.Equal(t, 1, ml.NumMembers())

	ml.AddMember("node-2", "127.0.0.1:8001", Alive, 1)
	assert.Equal(t, 2, ml.NumMembers())

	ml.RemoveMember("node-1")
	assert.Equal(t, 1, ml.NumMembers())
}

func TestMemberList_NumAliveMembers(t *testing.T) {
	ml := NewMemberList("local", "127.0.0.1:9000")

	assert.Equal(t, 0, ml.NumAliveMembers())

	ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)
	assert.Equal(t, 1, ml.NumAliveMembers())

	ml.AddMember("node-2", "127.0.0.1:8001", Suspect, 1)
	assert.Equal(t, 1, ml.NumAliveMembers())

	ml.AddMember("node-3", "127.0.0.1:8002", Alive, 1)
	assert.Equal(t, 2, ml.NumAliveMembers())
}

func TestMemberList_LocalMember(t *testing.T) {
	ml := NewMemberList("local-node", "127.0.0.1:9000")

	local := ml.LocalMember()

	assert.NotNil(t, local)
	assert.Equal(t, "local-node", local.ID)
	assert.Equal(t, "127.0.0.1:9000", local.Address)
}

func TestMemberList_IncrementIncarnation(t *testing.T) {
	ml := NewMemberList("local", "127.0.0.1:9000")

	assert.Equal(t, uint64(0), ml.LocalMember().Incarnation)

	inc1 := ml.IncrementIncarnation()
	assert.Equal(t, uint64(1), inc1)
	assert.Equal(t, uint64(1), ml.LocalMember().Incarnation)

	inc2 := ml.IncrementIncarnation()
	assert.Equal(t, uint64(2), inc2)
	assert.Equal(t, uint64(2), ml.LocalMember().Incarnation)
}

func TestMemberList_ConcurrentAccess(t *testing.T) {
	ml := NewMemberList("local", "127.0.0.1:9000")

	var wg sync.WaitGroup
	numGoroutines := 10

	// Concurrently add members
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			nodeID := "node-" + string(rune('A'+id))
			ml.AddMember(nodeID, "127.0.0.1:800"+string(rune('0'+id)), Alive, 1)
		}(i)
	}

	// Concurrently read members
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ml.GetMembers()
			ml.GetAliveMembers()
			ml.NumMembers()
			ml.NumAliveMembers()
		}()
	}

	// Concurrently get random members
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ml.GetRandomMember()
			ml.GetRandomMembers(3)
		}()
	}

	// Concurrently increment incarnation
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ml.IncrementIncarnation()
		}()
	}

	wg.Wait()

	// Should not panic or cause race conditions
}

func TestMemberList_AddMemberUpdatesLocalTime(t *testing.T) {
	ml := NewMemberList("local", "127.0.0.1:9000")

	before := time.Now()
	ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)
	after := time.Now()

	member, _ := ml.GetMember("node-1")
	assert.True(t, member.LocalTime.After(before) || member.LocalTime.Equal(before))
	assert.True(t, member.LocalTime.Before(after) || member.LocalTime.Equal(after))
}

func TestMemberList_UpdateMemberStatusUpdatesLocalTime(t *testing.T) {
	ml := NewMemberList("local", "127.0.0.1:9000")
	ml.AddMember("node-1", "127.0.0.1:8000", Alive, 1)

	time.Sleep(1 * time.Millisecond) // Ensure time difference

	before := time.Now()
	ml.UpdateMemberStatus("node-1", Suspect, 2)
	after := time.Now()

	member, _ := ml.GetMember("node-1")
	assert.True(t, member.LocalTime.After(before) || member.LocalTime.Equal(before))
	assert.True(t, member.LocalTime.Before(after) || member.LocalTime.Equal(after))
}
