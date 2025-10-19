package server

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc/resolver"
)

// ---- Simple in-process registry: ServerID -> ServerAddress ----

type idRegistry struct {
	mu       sync.RWMutex
	records  map[ServerID]ServerAddress
	watchers map[ServerID]map[*raftResolver]struct{}
}

var globalIDRegistry = &idRegistry{
	records:  make(map[ServerID]ServerAddress),
	watchers: make(map[ServerID]map[*raftResolver]struct{}),
}

// RegisterResolverPeer sets/updates the address for an ID and notifies any active resolvers.
func RegisterResolverPeer(id ServerID, addr ServerAddress) {
	globalIDRegistry.mu.Lock()
	globalIDRegistry.records[id] = addr
	watchers := globalIDRegistry.watchers[id]
	globalIDRegistry.mu.Unlock()

	// Notify after unlocking to avoid re-entrancy.
	for w := range watchers {
		w.pushCurrent()
	}
}

// ---- gRPC name resolver ("raft" scheme) ----

const raftScheme = "raft"

type raftBuilder struct{}

func (raftBuilder) Scheme() string { return raftScheme }

func (raftBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	// Accept "raft:///UUID" or "raft://cluster/UUID".
	id := ServerID(target.Endpoint())
	if id == "" {
		// Some versions carry endpoint in URL.Path when using triple slash.
		if p := target.URL.Path; len(p) > 0 {
			if p[0] == '/' {
				p = p[1:]
			}
			id = ServerID(p)
		}
	}
	if id == "" {
		return nil, fmt.Errorf("raft resolver: empty target endpoint: %+v", target)
	}

	r := &raftResolver{id: id, cc: cc}
	r.subscribe()
	r.pushCurrent()
	return r, nil
}

type raftResolver struct {
	id ServerID
	cc resolver.ClientConn
}

func (r *raftResolver) ResolveNow(resolver.ResolveNowOptions) { r.pushCurrent() }

func (r *raftResolver) Close() {
	globalIDRegistry.mu.Lock()
	defer globalIDRegistry.mu.Unlock()
	if set, ok := globalIDRegistry.watchers[r.id]; ok {
		delete(set, r)
		if len(set) == 0 {
			delete(globalIDRegistry.watchers, r.id)
		}
	}
}

func (r *raftResolver) subscribe() {
	globalIDRegistry.mu.Lock()
	defer globalIDRegistry.mu.Unlock()
	set := globalIDRegistry.watchers[r.id]
	if set == nil {
		set = make(map[*raftResolver]struct{})
		globalIDRegistry.watchers[r.id] = set
	}
	set[r] = struct{}{}
}

func (r *raftResolver) pushCurrent() {
	globalIDRegistry.mu.RLock()
	addr, ok := globalIDRegistry.records[r.id]
	globalIDRegistry.mu.RUnlock()

	if !ok || addr == "" {
		_ = r.cc.UpdateState(resolver.State{Addresses: nil}) // no address yet; gRPC will retry
		return
	}

	_ = r.cc.UpdateState(resolver.State{
		Addresses: []resolver.Address{{Addr: string(addr)}},
	})
}

// Register the resolver on init.
func init() {
	resolver.Register(raftBuilder{})
	_ = context.Background()
}
