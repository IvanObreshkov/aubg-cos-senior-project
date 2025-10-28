package server

import (
	"aubg-cos-senior-project/internal/raft/proto"
	"context"
	"fmt"
	"log"
)

// getServersFromConfig extracts server IDs from a configuration
func getServersFromConfig(config *proto.Configuration) []ServerID {
	if config == nil {
		return nil
	}

	servers := make([]ServerID, 0, len(config.Servers))
	for _, s := range config.Servers {
		servers = append(servers, ServerID(s.Id))
	}
	return servers
}

// isServerInConfig checks if a server is in the given configuration
func isServerInConfig(serverID ServerID, config *proto.Configuration) bool {
	if config == nil {
		return false
	}

	for _, s := range config.Servers {
		if ServerID(s.Id) == serverID {
			return true
		}
	}
	return false
}

// isServerInJointConfig checks if a server is in either old or new configuration during joint consensus
func isServerInJointConfig(serverID ServerID, config *proto.Configuration) bool {
	if config == nil {
		return false
	}

	// Check new configuration
	if isServerInConfig(serverID, config) {
		return true
	}

	// Check old configuration if in joint consensus
	if config.IsJoint && len(config.OldServers) > 0 {
		for _, s := range config.OldServers {
			if ServerID(s.Id) == serverID {
				return true
			}
		}
	}

	return false
}

// quorumSizeForConfig calculates the quorum size for a given configuration
// Section 6: During joint consensus, need majority in BOTH old and new configurations
func (s *Server) quorumSizeForConfig(config *proto.Configuration) int {
	if config == nil {
		return s.quorumSize() // fallback to peers-based calculation
	}

	voters := len(config.Servers)
	return voters/2 + 1
}

// isQuorumInConfig checks if we have a quorum of responses from servers in the given configuration
func (s *Server) isQuorumInConfig(config *proto.Configuration, respondedServers map[ServerID]bool) bool {
	if config == nil {
		return false
	}

	count := 0
	for _, server := range config.Servers {
		if respondedServers[ServerID(server.Id)] {
			count++
		}
	}

	quorum := s.quorumSizeForConfig(config)
	return count >= quorum
}

// hasJointQuorum checks if we have a quorum in BOTH old and new configurations during joint consensus
// Section 6: "agreement (for elections and entry commitment) requires separate majorities from both
// the old and new configurations"
func (s *Server) hasJointQuorum(config *proto.Configuration, respondedServers map[ServerID]bool) bool {
	if config == nil || !config.IsJoint {
		return false
	}

	// Need quorum in new configuration
	if !s.isQuorumInConfig(config, respondedServers) {
		return false
	}

	// Need quorum in old configuration
	oldConfig := &proto.Configuration{
		Servers: config.OldServers,
		IsJoint: false,
	}

	return s.isQuorumInConfig(oldConfig, respondedServers)
}

// initializeConfiguration initializes the server's configuration on startup
// Creates an initial configuration with just this server
func (s *Server) initializeConfiguration() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create initial configuration with just the known servers
	servers := make([]*proto.ServerConfig, 0, len(s.peers)+1)

	// Add self
	servers = append(servers, &proto.ServerConfig{
		Id:      string(s.ID),
		Address: string(s.Address),
	})

	// Add peers (if any)
	for _, peerID := range s.peers {
		// For now, we don't have peer addresses stored, so we'll skip this
		// In a real implementation, SetPeers should provide addresses
		_ = peerID
	}

	initialConfig := &proto.Configuration{
		Servers: servers,
		IsJoint: false,
	}

	s.committedConfig = initialConfig
	s.latestConfig = initialConfig
}

// AddServer handles the AddServer RPC (Section 6)
// Only the leader can process this request
func (s *Server) AddServer(ctx context.Context, req *proto.AddServerRequest) (*proto.AddServerResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Must be leader
	if s.state != Leader {
		return &proto.AddServerResponse{
			Status:   proto.ConfigChangeStatus_NOT_LEADER,
			LeaderId: "", // We don't track current leader ID
		}, nil
	}

	// Check if configuration change already in progress
	if s.configChangeInProgress {
		return &proto.AddServerResponse{
			Status: proto.ConfigChangeStatus_IN_PROGRESS,
		}, nil
	}

	log.Printf("[SERVER-%s] [TERM-%d] AddServer request for %s at %s",
		s.ID, s.currentTerm, req.ServerId, req.ServerAddress)

	// Get current committed configuration
	currentConfig := s.committedConfig
	if currentConfig == nil {
		currentConfig = s.latestConfig
	}

	// Check if server already in configuration
	if isServerInConfig(ServerID(req.ServerId), currentConfig) {
		log.Printf("[SERVER-%s] Server %s already in configuration", s.ID, req.ServerId)
		return &proto.AddServerResponse{
			Status: proto.ConfigChangeStatus_OK,
		}, nil
	}

	// Create new configuration with the added server
	// Section 6: First append C_old,new (joint consensus)
	newServers := make([]*proto.ServerConfig, len(currentConfig.Servers)+1)
	copy(newServers, currentConfig.Servers)
	newServers[len(currentConfig.Servers)] = &proto.ServerConfig{
		Id:      req.ServerId,
		Address: req.ServerAddress,
	}

	jointConfig := &proto.Configuration{
		Servers:    newServers,
		IsJoint:    true,
		OldServers: currentConfig.Servers,
	}

	// Append configuration entry to log
	entry := &proto.LogEntry{
		Term:          s.currentTerm,
		Type:          proto.LogEntryType_LOG_CONFIGURATION,
		Configuration: jointConfig,
	}

	// Get next log index
	lastIndex, err := s.Log.GetLastIndex()
	if err != nil {
		log.Printf("[SERVER-%s] Failed to get last log index: %v", s.ID, err)
		return &proto.AddServerResponse{
			Status: proto.ConfigChangeStatus_TIMEOUT,
		}, nil
	}

	entry.Index = lastIndex + 1

	// Append to log
	if err := s.Log.AppendEntry(entry); err != nil {
		log.Printf("[SERVER-%s] Failed to append configuration entry: %v", s.ID, err)
		return &proto.AddServerResponse{
			Status: proto.ConfigChangeStatus_TIMEOUT,
		}, nil
	}

	// Update latest config and mark change in progress
	s.latestConfig = jointConfig
	s.configChangeInProgress = true
	s.configChangeIndex = entry.Index

	log.Printf("[SERVER-%s] [TERM-%d] Appended C_old,new configuration at index %d",
		s.ID, s.currentTerm, entry.Index)

	// Section 6: Start replicating to the new server immediately
	// Initialize leader state for the new server (if we're the leader)
	if s.state == Leader {
		newServerID := ServerID(req.ServerId)

		// Add the new server to peers list for replication
		s.peers = append(s.peers, newServerID)

		// Initialize nextIndex and matchIndex for the new server
		lastLogIndex, _ := s.getLastLogIndexAndTerm()
		s.nextIndex[newServerID] = lastLogIndex + 1
		s.matchIndex[newServerID] = 0

		// Establish gRPC connection to the new server
		newServerAddr := ServerAddress(req.ServerAddress)
		if err := s.transport.AddPeer(newServerID, newServerAddr); err != nil {
			log.Printf("[SERVER-%s] Warning: Failed to add peer %s at %s: %v", s.ID, newServerID, newServerAddr, err)
		}

		log.Printf("[SERVER-%s] [TERM-%d] Started replicating to new server %s (nextIndex=%d)",
			s.ID, s.currentTerm, newServerID, lastLogIndex+1)
	}

	// Replicate the configuration change immediately
	go s.ReplicateToFollowers()

	return &proto.AddServerResponse{
		Status: proto.ConfigChangeStatus_OK,
	}, nil
}

// RemoveServer handles the RemoveServer RPC (Section 6)
// Only the leader can process this request
func (s *Server) RemoveServer(ctx context.Context, req *proto.RemoveServerRequest) (*proto.RemoveServerResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Must be leader
	if s.state != Leader {
		return &proto.RemoveServerResponse{
			Status:   proto.ConfigChangeStatus_NOT_LEADER,
			LeaderId: "",
		}, nil
	}

	// Check if configuration change already in progress
	if s.configChangeInProgress {
		return &proto.RemoveServerResponse{
			Status: proto.ConfigChangeStatus_IN_PROGRESS,
		}, nil
	}

	log.Printf("[SERVER-%s] [TERM-%d] RemoveServer request for %s",
		s.ID, s.currentTerm, req.ServerId)

	// Get current committed configuration
	currentConfig := s.committedConfig
	if currentConfig == nil {
		currentConfig = s.latestConfig
	}

	// Check if server in configuration
	if !isServerInConfig(ServerID(req.ServerId), currentConfig) {
		log.Printf("[SERVER-%s] Server %s not in configuration", s.ID, req.ServerId)
		return &proto.RemoveServerResponse{
			Status: proto.ConfigChangeStatus_OK,
		}, nil
	}

	// Create new configuration without the removed server
	// Section 6: First append C_old,new (joint consensus)
	newServers := make([]*proto.ServerConfig, 0, len(currentConfig.Servers)-1)
	for _, server := range currentConfig.Servers {
		if server.Id != req.ServerId {
			newServers = append(newServers, server)
		}
	}

	jointConfig := &proto.Configuration{
		Servers:    newServers,
		IsJoint:    true,
		OldServers: currentConfig.Servers,
	}

	// Append configuration entry to log
	entry := &proto.LogEntry{
		Term:          s.currentTerm,
		Type:          proto.LogEntryType_LOG_CONFIGURATION,
		Configuration: jointConfig,
	}

	// Get next log index
	lastIndex, err := s.Log.GetLastIndex()
	if err != nil {
		log.Printf("[SERVER-%s] Failed to get last log index: %v", s.ID, err)
		return &proto.RemoveServerResponse{
			Status: proto.ConfigChangeStatus_TIMEOUT,
		}, nil
	}

	entry.Index = lastIndex + 1

	// Append to log
	if err := s.Log.AppendEntry(entry); err != nil {
		log.Printf("[SERVER-%s] Failed to append configuration entry: %v", s.ID, err)
		return &proto.RemoveServerResponse{
			Status: proto.ConfigChangeStatus_TIMEOUT,
		}, nil
	}

	// Update latest config and mark change in progress
	s.latestConfig = jointConfig
	s.configChangeInProgress = true
	s.configChangeIndex = entry.Index

	log.Printf("[SERVER-%s] [TERM-%d] Appended C_old,new configuration at index %d",
		s.ID, s.currentTerm, entry.Index)

	// Section 6: Update peers list to reflect the joint configuration
	// Note: We don't remove the connection yet - that happens when C_new is committed
	// But we update the peers list so replication logic knows about the change
	if s.state == Leader {
		// Update peers list to reflect new configuration
		// During joint consensus, keep replicating to all servers in both configs
		newPeers := make([]ServerID, 0)
		for _, server := range jointConfig.Servers {
			serverID := ServerID(server.Id)
			if serverID != s.ID {
				newPeers = append(newPeers, serverID)
			}
		}

		// Keep the old peers list during joint consensus to maintain replication
		// The actual removal happens when C_new is committed
	}

	// Replicate the configuration change immediately
	go s.ReplicateToFollowers()

	return &proto.RemoveServerResponse{
		Status: proto.ConfigChangeStatus_OK,
	}, nil
}

// applyConfigurationEntry applies a configuration change to the server state
// Called when a configuration entry is committed
func (s *Server) applyConfigurationEntry(entry *proto.LogEntry) error {
	if entry.Type != proto.LogEntryType_LOG_CONFIGURATION {
		return fmt.Errorf("entry is not a configuration entry")
	}

	config := entry.Configuration
	if config == nil {
		return fmt.Errorf("configuration entry has no configuration")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[SERVER-%s] [TERM-%d] Applying configuration at index %d (joint=%v, servers=%d)",
		s.ID, s.currentTerm, entry.Index, config.IsJoint, len(config.Servers))

	if config.IsJoint {
		// This is C_old,new - we're in joint consensus mode
		s.latestConfig = config

		// Check if we're being removed in this joint configuration
		// If we're not in the new configuration, we should stop participating immediately
		if !isServerInConfig(s.ID, config) {
			log.Printf("[SERVER-%s] Server NOT in C_old,new configuration - will be removed", s.ID)

			// Mark that we've been removed from the cluster
			s.removedFromCluster = true

			// Clear our peers list since we're being removed
			s.peers = nil

			// Stop election timer - we should not participate anymore
			if s.electionTimeoutTimer != nil {
				s.electionTimeoutTimer.Stop()
				s.electionTimeoutTimer = nil
			}

			if s.state == Leader {
				// Stop heartbeats
				if s.heartbeatTimer != nil {
					s.heartbeatTimer.Stop()
				}
			}

			s.state = Follower
			s.votedFor = nil

			log.Printf("[SERVER-%s] Stopped all activity - server is being removed from cluster", s.ID)
		}

		// Once C_old,new is committed, leader should append C_new
		// Section 6: "Once C_old,new has been committed, C_new can be committed without C_old"
		if s.state == Leader && entry.Index == s.configChangeIndex {
			// Append C_new
			s.appendCnew(config)
		}
	} else {
		// This is C_new - joint consensus complete
		s.committedConfig = config
		s.latestConfig = config
		s.configChangeInProgress = false
		s.configChangeIndex = 0

		log.Printf("[SERVER-%s] [TERM-%d] Configuration change complete - now have %d servers",
			s.ID, s.currentTerm, len(config.Servers))

		// Build old and new peer sets for comparison
		oldPeers := make(map[ServerID]bool)
		for _, peerID := range s.peers {
			oldPeers[peerID] = true
		}

		// Update peers list and establish connections for new peers
		newPeers := make([]ServerID, 0, len(config.Servers)-1)
		for _, server := range config.Servers {
			serverID := ServerID(server.Id)
			if serverID != s.ID {
				newPeers = append(newPeers, serverID)

				// If this is a new peer, establish a gRPC connection
				if !oldPeers[serverID] {
					serverAddr := ServerAddress(server.Address)
					if err := s.transport.AddPeer(serverID, serverAddr); err != nil {
						log.Printf("[SERVER-%s] Warning: Failed to add peer %s at %s: %v", s.ID, serverID, serverAddr, err)
					} else {
						log.Printf("[SERVER-%s] Added connection to new peer %s", s.ID, serverID)
					}
				}
			}
		}
		s.peers = newPeers

		log.Printf("[SERVER-%s] Updated peers list to %d peers", s.ID, len(s.peers))

		// Remove connections for peers that are no longer in the cluster
		newPeerSet := make(map[ServerID]bool)
		for _, peerID := range newPeers {
			newPeerSet[peerID] = true
		}
		for peerID := range oldPeers {
			if !newPeerSet[peerID] {
				s.transport.RemovePeer(peerID)
				// Clean up leader state for removed peers
				if s.state == Leader {
					delete(s.nextIndex, peerID)
					delete(s.matchIndex, peerID)
				}
			}
		}

		// Check if we're being removed
		if !isServerInConfig(s.ID, config) {
			log.Printf("[SERVER-%s] Server removed from configuration, stepping down", s.ID)

			// Mark that we've been removed from the cluster
			s.removedFromCluster = true

			// Clear our peers list since we're no longer part of the cluster
			s.peers = nil

			if s.state == Leader {
				// Stop heartbeats
				if s.heartbeatTimer != nil {
					s.heartbeatTimer.Stop()
				}
			}
			s.state = Follower
			s.votedFor = nil

			// Stop election timer - we're not part of the cluster anymore
			if s.electionTimeoutTimer != nil {
				s.electionTimeoutTimer.Stop()
				s.electionTimeoutTimer = nil
			}

			log.Printf("[SERVER-%s] Stopped all timers - server is now isolated", s.ID)
		}
	}

	return nil
}

// appendCnew appends C_new after C_old,new is committed (Section 6)
// Must be called with s.mu held
func (s *Server) appendCnew(jointConfig *proto.Configuration) {
	// Create C_new from C_old,new
	cnew := &proto.Configuration{
		Servers: jointConfig.Servers,
		IsJoint: false,
	}

	entry := &proto.LogEntry{
		Term:          s.currentTerm,
		Type:          proto.LogEntryType_LOG_CONFIGURATION,
		Configuration: cnew,
	}

	// Get next log index
	lastIndex, err := s.Log.GetLastIndex()
	if err != nil {
		log.Printf("[SERVER-%s] Failed to get last log index: %v", s.ID, err)
		return
	}

	entry.Index = lastIndex + 1

	// Append to log
	if err := s.Log.AppendEntry(entry); err != nil {
		log.Printf("[SERVER-%s] Failed to append C_new entry: %v", s.ID, err)
		return
	}

	s.latestConfig = cnew
	s.configChangeIndex = entry.Index

	log.Printf("[SERVER-%s] [TERM-%d] Appended C_new configuration at index %d",
		s.ID, s.currentTerm, entry.Index)

	// Replicate C_new to followers immediately (don't wait for heartbeat)
	go s.ReplicateToFollowers()
}
