package server

import (
	"aubg-cos-senior-project/internal/pubsub"
	"aubg-cos-senior-project/internal/raft/mocks"
	"aubg-cos-senior-project/internal/raft/proto"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestServer creates a minimal test server for config tests
func createTestServer(t *testing.T, id ServerID, address ServerAddress) *Server {
	mockLog := mocks.NewMockLogStorage()
	mockSM := mocks.NewMockStateMachine()
	mockMetrics := mocks.NewMockMetricsCollector()
	pubSubClient := pubsub.NewPubSub()

	s := &Server{
		ID:           id,
		Address:      address,
		Log:          mockLog,
		StateMachine: mockSM,
		Metrics:      mockMetrics,
		pubSub:       pubSubClient,
		serverState: serverState{
			state:           Follower,
			currentTerm:     0,
			votedFor:        nil,
			commitIndex:     0,
			lastApplied:     0,
			nextIndex:       make(map[ServerID]uint64),
			matchIndex:      make(map[ServerID]uint64),
			votersThisTerm:  make(map[ServerID]struct{}),
			electionTimeout: 200 * time.Millisecond,
		},
		peers: []ServerID{},
	}

	return s
}

func TestGetServersFromConfig(t *testing.T) {
	config := &proto.Configuration{
		Servers: []*proto.ServerConfig{
			{Id: "server-1", Address: "localhost:5001"},
			{Id: "server-2", Address: "localhost:5002"},
			{Id: "server-3", Address: "localhost:5003"},
		},
		IsJoint: false,
	}

	servers := getServersFromConfig(config)

	assert.Len(t, servers, 3)
	assert.Contains(t, servers, ServerID("server-1"))
	assert.Contains(t, servers, ServerID("server-2"))
	assert.Contains(t, servers, ServerID("server-3"))
}

func TestGetServersFromConfig_Nil(t *testing.T) {
	servers := getServersFromConfig(nil)
	assert.Nil(t, servers)
}

func TestIsServerInConfig(t *testing.T) {
	config := &proto.Configuration{
		Servers: []*proto.ServerConfig{
			{Id: "server-1", Address: "localhost:5001"},
			{Id: "server-2", Address: "localhost:5002"},
		},
		IsJoint: false,
	}

	t.Run("returns true for server in config", func(t *testing.T) {
		assert.True(t, isServerInConfig("server-1", config))
		assert.True(t, isServerInConfig("server-2", config))
	})

	t.Run("returns false for server not in config", func(t *testing.T) {
		assert.False(t, isServerInConfig("server-3", config))
	})

	t.Run("returns false for nil config", func(t *testing.T) {
		assert.False(t, isServerInConfig("server-1", nil))
	})
}

func TestIsServerInJointConfig(t *testing.T) {
	t.Run("server in new configuration", func(t *testing.T) {
		config := &proto.Configuration{
			Servers: []*proto.ServerConfig{
				{Id: "server-1"},
				{Id: "server-2"},
			},
			IsJoint: true,
			OldServers: []*proto.ServerConfig{
				{Id: "server-3"},
			},
		}

		assert.True(t, isServerInJointConfig("server-1", config))
		assert.True(t, isServerInJointConfig("server-2", config))
	})

	t.Run("server in old configuration", func(t *testing.T) {
		config := &proto.Configuration{
			Servers: []*proto.ServerConfig{
				{Id: "server-1"},
			},
			IsJoint: true,
			OldServers: []*proto.ServerConfig{
				{Id: "server-2"},
				{Id: "server-3"},
			},
		}

		assert.True(t, isServerInJointConfig("server-2", config))
		assert.True(t, isServerInJointConfig("server-3", config))
	})

	t.Run("server not in either configuration", func(t *testing.T) {
		config := &proto.Configuration{
			Servers: []*proto.ServerConfig{
				{Id: "server-1"},
			},
			IsJoint: true,
			OldServers: []*proto.ServerConfig{
				{Id: "server-2"},
			},
		}

		assert.False(t, isServerInJointConfig("server-4", config))
	})

	t.Run("returns false for nil config", func(t *testing.T) {
		assert.False(t, isServerInJointConfig("server-1", nil))
	})
}

func TestQuorumSizeForConfig(t *testing.T) {
	s := createTestServer(t, "server-1", "localhost:5001")

	t.Run("3 servers", func(t *testing.T) {
		config := &proto.Configuration{
			Servers: []*proto.ServerConfig{
				{Id: "server-1"},
				{Id: "server-2"},
				{Id: "server-3"},
			},
		}

		quorum := s.quorumSizeForConfig(config)
		assert.Equal(t, 2, quorum)
	})

	t.Run("5 servers", func(t *testing.T) {
		config := &proto.Configuration{
			Servers: []*proto.ServerConfig{
				{Id: "s1"}, {Id: "s2"}, {Id: "s3"}, {Id: "s4"}, {Id: "s5"},
			},
		}

		quorum := s.quorumSizeForConfig(config)
		assert.Equal(t, 3, quorum)
	})

	t.Run("nil config falls back to peers", func(t *testing.T) {
		s.peers = []ServerID{"s2", "s3"}
		quorum := s.quorumSizeForConfig(nil)
		assert.Equal(t, 2, quorum)
	})
}

func TestIsQuorumInConfig(t *testing.T) {
	s := createTestServer(t, "server-1", "localhost:5001")

	config := &proto.Configuration{
		Servers: []*proto.ServerConfig{
			{Id: "server-1"},
			{Id: "server-2"},
			{Id: "server-3"},
		},
	}

	t.Run("has quorum with 2 out of 3", func(t *testing.T) {
		responded := map[ServerID]bool{
			"server-1": true,
			"server-2": true,
		}

		assert.True(t, s.isQuorumInConfig(config, responded))
	})

	t.Run("has quorum with all 3", func(t *testing.T) {
		responded := map[ServerID]bool{
			"server-1": true,
			"server-2": true,
			"server-3": true,
		}

		assert.True(t, s.isQuorumInConfig(config, responded))
	})

	t.Run("no quorum with only 1 out of 3", func(t *testing.T) {
		responded := map[ServerID]bool{
			"server-1": true,
		}

		assert.False(t, s.isQuorumInConfig(config, responded))
	})

	t.Run("returns false for nil config", func(t *testing.T) {
		responded := map[ServerID]bool{
			"server-1": true,
		}

		assert.False(t, s.isQuorumInConfig(nil, responded))
	})
}

func TestHasJointQuorum(t *testing.T) {
	s := createTestServer(t, "server-1", "localhost:5001")

	config := &proto.Configuration{
		Servers: []*proto.ServerConfig{
			{Id: "server-1"},
			{Id: "server-2"},
			{Id: "server-3"},
		},
		IsJoint: true,
		OldServers: []*proto.ServerConfig{
			{Id: "server-1"},
			{Id: "server-2"},
			{Id: "server-4"},
		},
	}

	t.Run("has joint quorum when majority in both", func(t *testing.T) {
		responded := map[ServerID]bool{
			"server-1": true,
			"server-2": true,
		}

		assert.True(t, s.hasJointQuorum(config, responded))
	})

	t.Run("no joint quorum when only majority in new config", func(t *testing.T) {
		responded := map[ServerID]bool{
			"server-1": true,
			"server-3": true,
		}

		assert.False(t, s.hasJointQuorum(config, responded))
	})

	t.Run("no joint quorum when only majority in old config", func(t *testing.T) {
		responded := map[ServerID]bool{
			"server-1": true,
			"server-4": true,
		}

		assert.False(t, s.hasJointQuorum(config, responded))
	})

	t.Run("returns false for non-joint config", func(t *testing.T) {
		nonJointConfig := &proto.Configuration{
			Servers: []*proto.ServerConfig{{Id: "s1"}},
			IsJoint: false,
		}

		responded := map[ServerID]bool{"s1": true}
		assert.False(t, s.hasJointQuorum(nonJointConfig, responded))
	})
}

func TestServer_AddServer(t *testing.T) {
	t.Run("non-leader rejects request", func(t *testing.T) {
		s := createTestServer(t, "server-1", "localhost:5001")
		s.state = Follower

		req := &proto.AddServerRequest{
			ServerId:      "server-2",
			ServerAddress: "localhost:5002",
		}

		resp, err := s.AddServer(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, proto.ConfigChangeStatus_NOT_LEADER, resp.Status)
	})

	t.Run("leader with config change in progress rejects", func(t *testing.T) {
		s := createTestServer(t, "server-1", "localhost:5001")
		s.state = Leader
		s.configChangeInProgress = true

		req := &proto.AddServerRequest{
			ServerId:      "server-2",
			ServerAddress: "localhost:5002",
		}

		resp, err := s.AddServer(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, proto.ConfigChangeStatus_IN_PROGRESS, resp.Status)
	})

	t.Run("leader adds new server successfully", func(t *testing.T) {
		s := createTestServer(t, "server-1", "localhost:5001")
		mockLog := s.Log.(*mocks.MockLogStorage)

		s.state = Leader
		s.currentTerm = 5
		s.committedConfig = &proto.Configuration{
			Servers: []*proto.ServerConfig{
				{Id: "server-1", Address: "localhost:5001"},
			},
			IsJoint: false,
		}
		s.latestConfig = s.committedConfig
		s.nextIndex = make(map[ServerID]uint64)
		s.matchIndex = make(map[ServerID]uint64)
		s.transport = NewTransport([]ServerID{}, s.Metrics) // Initialize transport

		req := &proto.AddServerRequest{
			ServerId:      "server-2",
			ServerAddress: "localhost:5002",
		}

		resp, err := s.AddServer(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, proto.ConfigChangeStatus_OK, resp.Status)

		// Verify joint config was created
		assert.True(t, s.latestConfig.IsJoint)
		assert.Len(t, s.latestConfig.Servers, 2)
		assert.Len(t, s.latestConfig.OldServers, 1)

		// Verify log entry was appended
		lastIndex, _ := mockLog.GetLastIndex()
		entry, err := mockLog.GetEntry(lastIndex)
		require.NoError(t, err)
		assert.Equal(t, proto.LogEntryType_LOG_CONFIGURATION, entry.Type)

		// Verify config change tracking
		assert.True(t, s.configChangeInProgress)
		assert.Equal(t, lastIndex, s.configChangeIndex)
	})

	t.Run("adding existing server returns OK", func(t *testing.T) {
		s := createTestServer(t, "server-1", "localhost:5001")
		s.state = Leader
		s.committedConfig = &proto.Configuration{
			Servers: []*proto.ServerConfig{
				{Id: "server-1", Address: "localhost:5001"},
				{Id: "server-2", Address: "localhost:5002"},
			},
			IsJoint: false,
		}

		req := &proto.AddServerRequest{
			ServerId:      "server-2",
			ServerAddress: "localhost:5002",
		}

		resp, err := s.AddServer(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, proto.ConfigChangeStatus_OK, resp.Status)
	})
}

func TestServer_RemoveServer(t *testing.T) {
	t.Run("non-leader rejects request", func(t *testing.T) {
		s := createTestServer(t, "server-1", "localhost:5001")
		s.state = Follower

		req := &proto.RemoveServerRequest{
			ServerId: "server-2",
		}

		resp, err := s.RemoveServer(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, proto.ConfigChangeStatus_NOT_LEADER, resp.Status)
	})

	t.Run("leader with config change in progress rejects", func(t *testing.T) {
		s := createTestServer(t, "server-1", "localhost:5001")
		s.state = Leader
		s.configChangeInProgress = true

		req := &proto.RemoveServerRequest{
			ServerId: "server-2",
		}

		resp, err := s.RemoveServer(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, proto.ConfigChangeStatus_IN_PROGRESS, resp.Status)
	})

	t.Run("leader removes server successfully", func(t *testing.T) {
		s := createTestServer(t, "server-1", "localhost:5001")
		mockLog := s.Log.(*mocks.MockLogStorage)

		s.state = Leader
		s.currentTerm = 5
		s.committedConfig = &proto.Configuration{
			Servers: []*proto.ServerConfig{
				{Id: "server-1", Address: "localhost:5001"},
				{Id: "server-2", Address: "localhost:5002"},
				{Id: "server-3", Address: "localhost:5003"},
			},
			IsJoint: false,
		}
		s.latestConfig = s.committedConfig

		req := &proto.RemoveServerRequest{
			ServerId: "server-2",
		}

		resp, err := s.RemoveServer(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, proto.ConfigChangeStatus_OK, resp.Status)

		// Verify joint config was created
		assert.True(t, s.latestConfig.IsJoint)
		assert.Len(t, s.latestConfig.Servers, 2) // server-1 and server-3
		assert.Len(t, s.latestConfig.OldServers, 3)

		// Verify log entry was appended
		lastIndex, _ := mockLog.GetLastIndex()
		entry, err := mockLog.GetEntry(lastIndex)
		require.NoError(t, err)
		assert.Equal(t, proto.LogEntryType_LOG_CONFIGURATION, entry.Type)

		// Verify config change tracking
		assert.True(t, s.configChangeInProgress)
	})

	t.Run("removing non-existent server returns OK", func(t *testing.T) {
		s := createTestServer(t, "server-1", "localhost:5001")
		s.state = Leader
		s.committedConfig = &proto.Configuration{
			Servers: []*proto.ServerConfig{
				{Id: "server-1", Address: "localhost:5001"},
			},
			IsJoint: false,
		}

		req := &proto.RemoveServerRequest{
			ServerId: "server-999",
		}

		resp, err := s.RemoveServer(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, proto.ConfigChangeStatus_OK, resp.Status)
	})
}

func TestServer_ApplyConfigurationEntry(t *testing.T) {
	t.Run("applies joint configuration", func(t *testing.T) {
		s := createTestServer(t, "server-1", "localhost:5001")

		jointConfig := &proto.Configuration{
			Servers: []*proto.ServerConfig{
				{Id: "server-1", Address: "localhost:5001"},
				{Id: "server-2", Address: "localhost:5002"},
			},
			IsJoint: true,
			OldServers: []*proto.ServerConfig{
				{Id: "server-1", Address: "localhost:5001"},
			},
		}

		entry := &proto.LogEntry{
			Index:         1,
			Term:          1,
			Type:          proto.LogEntryType_LOG_CONFIGURATION,
			Configuration: jointConfig,
		}

		err := s.applyConfigurationEntry(entry)
		require.NoError(t, err)

		assert.Equal(t, jointConfig, s.latestConfig)
	})

	t.Run("applies final configuration", func(t *testing.T) {
		s := createTestServer(t, "server-1", "localhost:5001")
		s.configChangeInProgress = true
		s.configChangeIndex = 1
		s.transport = NewTransport([]ServerID{}, s.Metrics) // Initialize transport

		finalConfig := &proto.Configuration{
			Servers: []*proto.ServerConfig{
				{Id: "server-1", Address: "localhost:5001"},
				{Id: "server-2", Address: "localhost:5002"},
			},
			IsJoint: false,
		}

		entry := &proto.LogEntry{
			Index:         2,
			Term:          1,
			Type:          proto.LogEntryType_LOG_CONFIGURATION,
			Configuration: finalConfig,
		}

		err := s.applyConfigurationEntry(entry)
		require.NoError(t, err)

		assert.Equal(t, finalConfig, s.committedConfig)
		assert.Equal(t, finalConfig, s.latestConfig)
		assert.False(t, s.configChangeInProgress)
		assert.Equal(t, uint64(0), s.configChangeIndex)
	})

	t.Run("detects server removal in joint config", func(t *testing.T) {
		s := createTestServer(t, "server-2", "localhost:5002")
		s.state = Follower

		// Config that doesn't include server-2
		jointConfig := &proto.Configuration{
			Servers: []*proto.ServerConfig{
				{Id: "server-1", Address: "localhost:5001"},
				{Id: "server-3", Address: "localhost:5003"},
			},
			IsJoint: true,
			OldServers: []*proto.ServerConfig{
				{Id: "server-1", Address: "localhost:5001"},
				{Id: "server-2", Address: "localhost:5002"},
			},
		}

		entry := &proto.LogEntry{
			Index:         1,
			Term:          1,
			Type:          proto.LogEntryType_LOG_CONFIGURATION,
			Configuration: jointConfig,
		}

		err := s.applyConfigurationEntry(entry)
		require.NoError(t, err)

		assert.True(t, s.removedFromCluster)
		assert.Nil(t, s.peers)
	})

	t.Run("detects server removal in final config", func(t *testing.T) {
		s := createTestServer(t, "server-2", "localhost:5002")
		s.state = Leader
		s.transport = NewTransport([]ServerID{}, s.Metrics) // Initialize transport

		// Config that doesn't include server-2
		finalConfig := &proto.Configuration{
			Servers: []*proto.ServerConfig{
				{Id: "server-1", Address: "localhost:5001"},
				{Id: "server-3", Address: "localhost:5003"},
			},
			IsJoint: false,
		}

		entry := &proto.LogEntry{
			Index:         1,
			Term:          1,
			Type:          proto.LogEntryType_LOG_CONFIGURATION,
			Configuration: finalConfig,
		}

		err := s.applyConfigurationEntry(entry)
		require.NoError(t, err)

		assert.True(t, s.removedFromCluster)
		assert.Nil(t, s.peers)
		assert.Equal(t, Follower, s.state)
	})

	t.Run("returns error for non-configuration entry", func(t *testing.T) {
		s := createTestServer(t, "server-1", "localhost:5001")

		entry := &proto.LogEntry{
			Index:   1,
			Term:    1,
			Type:    proto.LogEntryType_LOG_COMMAND,
			Command: []byte("test"),
		}

		err := s.applyConfigurationEntry(entry)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not a configuration entry")
	})

	t.Run("returns error for nil configuration", func(t *testing.T) {
		s := createTestServer(t, "server-1", "localhost:5001")

		entry := &proto.LogEntry{
			Index:         1,
			Term:          1,
			Type:          proto.LogEntryType_LOG_CONFIGURATION,
			Configuration: nil,
		}

		err := s.applyConfigurationEntry(entry)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no configuration")
	})
}
