package server

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

func TestRaftBuilder_Scheme(t *testing.T) {
	builder := raftBuilder{}
	assert.Equal(t, "raft", builder.Scheme())
}

func TestRegisterResolverPeer(t *testing.T) {
	// Clean up before test
	globalIDRegistry.mu.Lock()
	globalIDRegistry.records = make(map[ServerID]ServerAddress)
	globalIDRegistry.watchers = make(map[ServerID]map[*raftResolver]struct{})
	globalIDRegistry.mu.Unlock()

	t.Run("registers peer address", func(t *testing.T) {
		serverID := ServerID("test-server-1")
		serverAddr := ServerAddress("localhost:5001")

		RegisterResolverPeer(serverID, serverAddr)

		globalIDRegistry.mu.RLock()
		addr, ok := globalIDRegistry.records[serverID]
		globalIDRegistry.mu.RUnlock()

		assert.True(t, ok)
		assert.Equal(t, serverAddr, addr)
	})

	t.Run("updates existing peer address", func(t *testing.T) {
		serverID := ServerID("test-server-2")

		RegisterResolverPeer(serverID, "localhost:5002")
		RegisterResolverPeer(serverID, "localhost:5003")

		globalIDRegistry.mu.RLock()
		addr := globalIDRegistry.records[serverID]
		globalIDRegistry.mu.RUnlock()

		assert.Equal(t, ServerAddress("localhost:5003"), addr)
	})
}

func TestRaftResolver_Build(t *testing.T) {
	// Clean up before test
	globalIDRegistry.mu.Lock()
	globalIDRegistry.records = make(map[ServerID]ServerAddress)
	globalIDRegistry.watchers = make(map[ServerID]map[*raftResolver]struct{})
	globalIDRegistry.mu.Unlock()

	builder := raftBuilder{}

	t.Run("builds resolver with endpoint in target", func(t *testing.T) {
		target := resolver.Target{
			URL: url.URL{
				Scheme: "raft",
				Path:   "/test-server-1",
			},
		}

		// Mock client conn
		cc := &mockClientConn{}

		res, err := builder.Build(target, cc, resolver.BuildOptions{})
		assert.NoError(t, err)
		assert.NotNil(t, res)

		// Clean up
		res.Close()
	})

	t.Run("returns error for empty endpoint", func(t *testing.T) {
		target := resolver.Target{
			URL: url.URL{
				Scheme: "raft",
				Path:   "",
			},
		}

		cc := &mockClientConn{}

		_, err := builder.Build(target, cc, resolver.BuildOptions{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty target endpoint")
	})
}

func TestRaftResolver_ResolveNow(t *testing.T) {
	// Clean up before test
	globalIDRegistry.mu.Lock()
	globalIDRegistry.records = make(map[ServerID]ServerAddress)
	globalIDRegistry.watchers = make(map[ServerID]map[*raftResolver]struct{})
	globalIDRegistry.mu.Unlock()

	serverID := ServerID("resolve-test")
	RegisterResolverPeer(serverID, "localhost:6001")

	builder := raftBuilder{}
	target := resolver.Target{
		URL: url.URL{
			Scheme: "raft",
			Path:   "/" + string(serverID),
		},
	}

	cc := &mockClientConn{}
	res, err := builder.Build(target, cc, resolver.BuildOptions{})
	assert.NoError(t, err)

	// ResolveNow should trigger an update
	res.ResolveNow(resolver.ResolveNowOptions{})

	// Verify state was updated
	assert.Len(t, cc.states, 2) // Initial build + ResolveNow

	res.Close()
}

func TestRaftResolver_Close(t *testing.T) {
	// Clean up before test
	globalIDRegistry.mu.Lock()
	globalIDRegistry.records = make(map[ServerID]ServerAddress)
	globalIDRegistry.watchers = make(map[ServerID]map[*raftResolver]struct{})
	globalIDRegistry.mu.Unlock()

	serverID := ServerID("close-test")
	RegisterResolverPeer(serverID, "localhost:7001")

	builder := raftBuilder{}
	target := resolver.Target{
		URL: url.URL{
			Scheme: "raft",
			Path:   "/" + string(serverID),
		},
	}

	cc := &mockClientConn{}
	res, err := builder.Build(target, cc, resolver.BuildOptions{})
	assert.NoError(t, err)

	// Verify watcher was added
	globalIDRegistry.mu.RLock()
	watchers := globalIDRegistry.watchers[serverID]
	globalIDRegistry.mu.RUnlock()
	assert.Len(t, watchers, 1)

	// Close resolver
	res.Close()

	// Verify watcher was removed
	globalIDRegistry.mu.RLock()
	watchers = globalIDRegistry.watchers[serverID]
	globalIDRegistry.mu.RUnlock()
	assert.Len(t, watchers, 0)
}

func TestRaftResolver_PushCurrent(t *testing.T) {
	// Clean up before test
	globalIDRegistry.mu.Lock()
	globalIDRegistry.records = make(map[ServerID]ServerAddress)
	globalIDRegistry.watchers = make(map[ServerID]map[*raftResolver]struct{})
	globalIDRegistry.mu.Unlock()

	t.Run("pushes address when available", func(t *testing.T) {
		serverID := ServerID("push-test-1")
		serverAddr := ServerAddress("localhost:8001")
		RegisterResolverPeer(serverID, serverAddr)

		builder := raftBuilder{}
		target := resolver.Target{
			URL: url.URL{
				Scheme: "raft",
				Path:   "/" + string(serverID),
			},
		}

		cc := &mockClientConn{}
		res, err := builder.Build(target, cc, resolver.BuildOptions{})
		assert.NoError(t, err)
		defer res.Close()

		// Verify address was pushed
		assert.NotEmpty(t, cc.states)
		lastState := cc.states[len(cc.states)-1]
		assert.Len(t, lastState.Addresses, 1)
		assert.Equal(t, string(serverAddr), lastState.Addresses[0].Addr)
	})

	t.Run("pushes empty when address not available", func(t *testing.T) {
		serverID := ServerID("push-test-2")
		// Don't register address

		builder := raftBuilder{}
		target := resolver.Target{
			URL: url.URL{
				Scheme: "raft",
				Path:   "/" + string(serverID),
			},
		}

		cc := &mockClientConn{}
		res, err := builder.Build(target, cc, resolver.BuildOptions{})
		assert.NoError(t, err)
		defer res.Close()

		// Verify nil addresses
		assert.NotEmpty(t, cc.states)
		lastState := cc.states[len(cc.states)-1]
		assert.Len(t, lastState.Addresses, 0)
	})
}

func TestRaftResolver_UpdateOnRegister(t *testing.T) {
	// Clean up before test
	globalIDRegistry.mu.Lock()
	globalIDRegistry.records = make(map[ServerID]ServerAddress)
	globalIDRegistry.watchers = make(map[ServerID]map[*raftResolver]struct{})
	globalIDRegistry.mu.Unlock()

	serverID := ServerID("update-test")

	builder := raftBuilder{}
	target := resolver.Target{
		URL: url.URL{
			Scheme: "raft",
			Path:   "/" + string(serverID),
		},
	}

	cc := &mockClientConn{}
	res, err := builder.Build(target, cc, resolver.BuildOptions{})
	assert.NoError(t, err)
	defer res.Close()

	initialStates := len(cc.states)

	// Register address - should trigger update
	RegisterResolverPeer(serverID, "localhost:9001")

	// Verify update was triggered
	assert.Greater(t, len(cc.states), initialStates)
}

// Mock client conn for testing
type mockClientConn struct {
	states []resolver.State
}

func (m *mockClientConn) UpdateState(s resolver.State) error {
	m.states = append(m.states, s)
	return nil
}

func (m *mockClientConn) ReportError(err error) {}

func (m *mockClientConn) NewAddress(addresses []resolver.Address) {}

func (m *mockClientConn) NewServiceConfig(serviceConfig string) {}

func (m *mockClientConn) ParseServiceConfig(serviceConfigJSON string) *serviceconfig.ParseResult {
	return &serviceconfig.ParseResult{}
}
