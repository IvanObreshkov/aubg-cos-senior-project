package server

import (
	"aubg-cos-senior-project/internal/raft/proto"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestServerState_GetSetState(t *testing.T) {
	s := &serverState{}

	t.Run("default state", func(t *testing.T) {
		state := s.getState()
		assert.Equal(t, State(0), state)
	})

	t.Run("sets and gets state", func(t *testing.T) {
		s.setState(Leader)
		assert.Equal(t, Leader, s.getState())

		s.setState(Follower)
		assert.Equal(t, Follower, s.getState())

		s.setState(Candidate)
		assert.Equal(t, Candidate, s.getState())
	})
}

func TestServerState_GetSetCurrentTerm(t *testing.T) {
	s := &serverState{}

	t.Run("default term is 0", func(t *testing.T) {
		term := s.getCurrentTerm()
		assert.Equal(t, uint64(0), term)
	})

	t.Run("sets and gets term", func(t *testing.T) {
		s.setCurrentTerm(5)
		assert.Equal(t, uint64(5), s.getCurrentTerm())

		s.setCurrentTerm(10)
		assert.Equal(t, uint64(10), s.getCurrentTerm())
	})
}

func TestServerState_GetSetVotedFor(t *testing.T) {
	s := &serverState{}

	t.Run("default votedFor is nil", func(t *testing.T) {
		votedFor := s.getVotedFor()
		assert.Nil(t, votedFor)
	})

	t.Run("sets and gets votedFor", func(t *testing.T) {
		candidateID := ServerID("server-123")
		s.setVotedFor(&candidateID)

		votedFor := s.getVotedFor()
		assert.NotNil(t, votedFor)
		assert.Equal(t, candidateID, *votedFor)
	})

	t.Run("clears votedFor with nil", func(t *testing.T) {
		s.setVotedFor(nil)
		assert.Nil(t, s.getVotedFor())
	})
}

func TestServerState_GetSetElectionTimeout(t *testing.T) {
	s := &serverState{}

	timeout := 200 * time.Millisecond
	s.setElectionTimeout(timeout)

	assert.Equal(t, timeout, s.getElectionTimeout())
}

func TestServerState_CommitAndLastApplied(t *testing.T) {
	s := &serverState{}

	t.Run("default commitIndex is 0", func(t *testing.T) {
		s.mu.RLock()
		assert.Equal(t, uint64(0), s.commitIndex)
		s.mu.RUnlock()
	})

	t.Run("default lastApplied is 0", func(t *testing.T) {
		s.mu.RLock()
		assert.Equal(t, uint64(0), s.lastApplied)
		s.mu.RUnlock()
	})

	t.Run("sets commitIndex", func(t *testing.T) {
		s.mu.Lock()
		s.commitIndex = 10
		s.mu.Unlock()

		s.mu.RLock()
		assert.Equal(t, uint64(10), s.commitIndex)
		s.mu.RUnlock()
	})

	t.Run("sets lastApplied", func(t *testing.T) {
		s.mu.Lock()
		s.lastApplied = 5
		s.mu.Unlock()

		s.mu.RLock()
		assert.Equal(t, uint64(5), s.lastApplied)
		s.mu.RUnlock()
	})
}

func TestServerState_NextAndMatchIndex(t *testing.T) {
	s := &serverState{
		nextIndex:  make(map[ServerID]uint64),
		matchIndex: make(map[ServerID]uint64),
	}

	peerID := ServerID("peer-1")

	t.Run("sets and gets next index for peer", func(t *testing.T) {
		s.mu.Lock()
		s.nextIndex[peerID] = 10
		s.mu.Unlock()

		s.mu.RLock()
		index := s.nextIndex[peerID]
		s.mu.RUnlock()
		assert.Equal(t, uint64(10), index)
	})

	t.Run("sets and gets match index for peer", func(t *testing.T) {
		s.mu.Lock()
		s.matchIndex[peerID] = 8
		s.mu.Unlock()

		s.mu.RLock()
		index := s.matchIndex[peerID]
		s.mu.RUnlock()
		assert.Equal(t, uint64(8), index)
	})
}

func TestServerState_LastLeaderContact(t *testing.T) {
	s := &serverState{}

	t.Run("default is zero time", func(t *testing.T) {
		contact := s.getLastLeaderContact()
		assert.True(t, contact.IsZero())
	})

	t.Run("sets and gets last leader contact", func(t *testing.T) {
		now := time.Now()
		s.setLastLeaderContact(now)

		contact := s.getLastLeaderContact()
		assert.Equal(t, now, contact)
	})
}

func TestServerState_CurrentLeader(t *testing.T) {
	s := &serverState{}

	t.Run("default is nil", func(t *testing.T) {
		s.mu.RLock()
		leader := s.currentLeader
		s.mu.RUnlock()
		assert.Nil(t, leader)
	})

	t.Run("sets and gets current leader", func(t *testing.T) {
		leaderID := ServerID("leader-1")
		s.mu.Lock()
		s.currentLeader = &leaderID
		s.mu.Unlock()

		s.mu.RLock()
		leader := s.currentLeader
		s.mu.RUnlock()
		assert.NotNil(t, leader)
		assert.Equal(t, leaderID, *leader)
	})

	t.Run("clears leader with nil", func(t *testing.T) {
		s.mu.Lock()
		s.currentLeader = nil
		s.mu.Unlock()

		s.mu.RLock()
		leader := s.currentLeader
		s.mu.RUnlock()
		assert.Nil(t, leader)
	})
}

func TestServerState_VotersThisTerm(t *testing.T) {
	s := &serverState{}

	voter1 := ServerID("voter-1")
	voter2 := ServerID("voter-2")

	t.Run("records votes", func(t *testing.T) {
		s.initVotersForTerm(5)

		isNew := s.recordVote(voter1)
		assert.True(t, isNew)

		isNew = s.recordVote(voter2)
		assert.True(t, isNew)

		// Duplicate vote
		isNew = s.recordVote(voter1)
		assert.False(t, isNew)

		assert.Equal(t, 2, s.getVoteCount())
	})

	t.Run("clears voters", func(t *testing.T) {
		s.clearVoters()
		assert.Equal(t, 0, s.getVoteCount())
	})
}

func TestServerState_Configuration(t *testing.T) {
	s := &serverState{}

	config := &proto.Configuration{
		Servers: []*proto.ServerConfig{
			{Id: "server1", Address: "localhost:5001"},
		},
		IsJoint: false,
	}

	t.Run("sets and gets committed config", func(t *testing.T) {
		s.setCommittedConfig(config)
		retrieved := s.getCommittedConfig()

		assert.NotNil(t, retrieved)
		assert.Equal(t, config, retrieved)
	})

	t.Run("sets and gets latest config", func(t *testing.T) {
		s.setLatestConfig(config)
		retrieved := s.getLatestConfig()

		assert.NotNil(t, retrieved)
		assert.Equal(t, config, retrieved)
	})
}

func TestServerState_ConfigChangeInProgress(t *testing.T) {
	s := &serverState{}

	t.Run("default is false", func(t *testing.T) {
		assert.False(t, s.isConfigChangeInProgress())
	})

	t.Run("sets config change in progress", func(t *testing.T) {
		s.setConfigChangeInProgress(true)
		assert.True(t, s.isConfigChangeInProgress())

		s.setConfigChangeInProgress(false)
		assert.False(t, s.isConfigChangeInProgress())
	})
}

func TestServerState_ConfigChangeIndex(t *testing.T) {
	s := &serverState{}

	t.Run("sets and gets config change index", func(t *testing.T) {
		s.setConfigChangeIndex(42)
		assert.Equal(t, uint64(42), s.getConfigChangeIndex())
	})
}

func TestServerState_RemovedFromCluster(t *testing.T) {
	s := &serverState{}

	t.Run("default is false", func(t *testing.T) {
		assert.False(t, s.isRemovedFromCluster())
	})

	t.Run("sets removed from cluster", func(t *testing.T) {
		s.setRemovedFromCluster(true)
		assert.True(t, s.isRemovedFromCluster())

		s.setRemovedFromCluster(false)
		assert.False(t, s.isRemovedFromCluster())
	})
}

func TestServerState_ElectionStartTime(t *testing.T) {
	s := &serverState{}

	t.Run("default is zero time", func(t *testing.T) {
		s.mu.RLock()
		startTime := s.electionStartTime
		s.mu.RUnlock()
		assert.True(t, startTime.IsZero())
	})

	t.Run("sets and gets election start time", func(t *testing.T) {
		now := time.Now()
		s.mu.Lock()
		s.electionStartTime = now
		s.mu.Unlock()

		s.mu.RLock()
		startTime := s.electionStartTime
		s.mu.RUnlock()
		assert.Equal(t, now, startTime)
	})
}

func TestServerState_Concurrency(t *testing.T) {
	s := &serverState{}

	t.Run("handles concurrent state access", func(t *testing.T) {
		var wg sync.WaitGroup
		iterations := 1000

		// Concurrent state changes
		for i := 0; i < iterations; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				state := State(idx % 3)
				s.setState(state)
				s.getState()
			}(i)
		}

		// Concurrent term changes
		for i := 0; i < iterations; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				s.setCurrentTerm(uint64(idx))
				s.getCurrentTerm()
			}(i)
		}

		wg.Wait()
	})

	t.Run("handles concurrent voter operations", func(t *testing.T) {
		s.initVotersForTerm(100)
		var wg sync.WaitGroup

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				voterID := ServerID("voter-" + string(rune(idx)))
				s.recordVote(voterID)
			}(i)
		}

		wg.Wait()
	})
}

func TestState_String(t *testing.T) {
	assert.Equal(t, "Leader", Leader.String())
	assert.Equal(t, "Follower", Follower.String())
	assert.Equal(t, "Candidate", Candidate.String())
	assert.Equal(t, "Unknown", State(99).String())
}
