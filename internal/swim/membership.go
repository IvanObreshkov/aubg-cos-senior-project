package swim

import (
	"math/rand"
	"sync"
	"time"
)

// MemberList manages the membership list
type MemberList struct {
	mu      sync.RWMutex
	members map[string]*Member // Key is member ID
	local   *Member            // The local member
}

// NewMemberList creates a new membership list
func NewMemberList(localID, localAddr string) *MemberList {
	local := &Member{
		ID:          localID,
		Address:     localAddr,
		Status:      Alive,
		Incarnation: 0,
		LocalTime:   time.Now(),
	}

	return &MemberList{
		members: make(map[string]*Member),
		local:   local,
	}
}

// AddMember adds or updates a member in the list
// Returns (changed, oldStatus) where changed indicates if member was added/updated, oldStatus is previous status
func (ml *MemberList) AddMember(id, addr string, status MemberStatus, incarnation uint64) (bool, MemberStatus) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	existing, exists := ml.members[id]
	if !exists {
		ml.members[id] = &Member{
			ID:          id,
			Address:     addr,
			Status:      status,
			Incarnation: incarnation,
			LocalTime:   time.Now(),
		}
		return true, 0 // No old status for new member
	}

	oldStatus := existing.Status

	// Only update if incarnation is higher, or same incarnation with status priority
	if incarnation > existing.Incarnation {
		existing.Incarnation = incarnation
		existing.Status = status
		existing.LocalTime = time.Now()
		return true, oldStatus
	} else if incarnation == existing.Incarnation {
		// Status priority: Alive > Suspect > Failed > Left
		// This ensures newer information overwrites older
		if shouldOverride(existing.Status, status) {
			existing.Status = status
			existing.LocalTime = time.Now()
			return true, oldStatus
		}
	}

	return false, oldStatus
}

// shouldOverride determines if newStatus should override oldStatus
func shouldOverride(oldStatus, newStatus MemberStatus) bool {
	// If statuses are same, no override
	if oldStatus == newStatus {
		return false
	}

	// Alive overrides Suspect
	if newStatus == Alive && oldStatus == Suspect {
		return true
	}

	// Suspect overrides Alive (someone detected failure)
	if newStatus == Suspect && oldStatus == Alive {
		return true
	}

	// Confirm/Failed overrides everything except Left
	if newStatus == Failed && (oldStatus == Alive || oldStatus == Suspect) {
		return true
	}

	// Left overrides everything (voluntary departure is final)
	if newStatus == Left {
		return true
	}

	return false
}

// GetMember retrieves a member by ID
func (ml *MemberList) GetMember(id string) (*Member, bool) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	member, exists := ml.members[id]
	return member, exists
}

// UpdateMemberStatus updates a member's status
func (ml *MemberList) UpdateMemberStatus(id string, status MemberStatus, incarnation uint64) bool {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	member, exists := ml.members[id]
	if !exists {
		return false
	}

	// Apply incarnation and status priority rules
	if incarnation > member.Incarnation {
		member.Incarnation = incarnation
		member.Status = status
		member.LocalTime = time.Now()
		return true
	} else if incarnation == member.Incarnation && shouldOverride(member.Status, status) {
		member.Status = status
		member.LocalTime = time.Now()
		return true
	}

	return false
}

// RemoveMember removes a member from the list
func (ml *MemberList) RemoveMember(id string) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	delete(ml.members, id)
}

// GetMembers returns a snapshot of all members
func (ml *MemberList) GetMembers() []*Member {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	members := make([]*Member, 0, len(ml.members))
	for _, m := range ml.members {
		// Create a copy to avoid race conditions
		memberCopy := &Member{
			ID:          m.ID,
			Address:     m.Address,
			Status:      m.Status,
			Incarnation: m.Incarnation,
			LocalTime:   m.LocalTime,
		}
		members = append(members, memberCopy)
	}
	return members
}

// GetAliveMembers returns only alive members
func (ml *MemberList) GetAliveMembers() []*Member {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	members := make([]*Member, 0)
	for _, m := range ml.members {
		if m.Status == Alive {
			memberCopy := &Member{
				ID:          m.ID,
				Address:     m.Address,
				Status:      m.Status,
				Incarnation: m.Incarnation,
				LocalTime:   m.LocalTime,
			}
			members = append(members, memberCopy)
		}
	}
	return members
}

// GetRandomMember returns a random member (excluding local and optionally failed/left)
func (ml *MemberList) GetRandomMember(excludeIDs ...string) *Member {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	excludeMap := make(map[string]bool)
	for _, id := range excludeIDs {
		excludeMap[id] = true
	}

	candidates := make([]*Member, 0)
	for _, m := range ml.members {
		// Exclude specified IDs and non-contactable members
		if !excludeMap[m.ID] && (m.Status == Alive || m.Status == Suspect) {
			candidates = append(candidates, m)
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	// Use crypto/rand for better randomness in production
	return candidates[rand.Intn(len(candidates))]
}

// GetRandomMembers returns n random members (excluding specified IDs)
func (ml *MemberList) GetRandomMembers(count int, excludeIDs ...string) []*Member {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	excludeMap := make(map[string]bool)
	for _, id := range excludeIDs {
		excludeMap[id] = true
	}

	candidates := make([]*Member, 0)
	for _, m := range ml.members {
		if !excludeMap[m.ID] && (m.Status == Alive || m.Status == Suspect) {
			candidates = append(candidates, m)
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	// Don't try to return more than available
	if count > len(candidates) {
		count = len(candidates)
	}

	// Fisher-Yates shuffle to get random subset
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	return candidates[:count]
}

// NumMembers returns the total number of members
func (ml *MemberList) NumMembers() int {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return len(ml.members)
}

// NumAliveMembers returns the number of alive members
func (ml *MemberList) NumAliveMembers() int {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	count := 0
	for _, m := range ml.members {
		if m.Status == Alive {
			count++
		}
	}
	return count
}

// LocalMember returns the local member
func (ml *MemberList) LocalMember() *Member {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return ml.local
}

// IncrementIncarnation increments the local member's incarnation number
func (ml *MemberList) IncrementIncarnation() uint64 {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.local.Incarnation++
	return ml.local.Incarnation
}
