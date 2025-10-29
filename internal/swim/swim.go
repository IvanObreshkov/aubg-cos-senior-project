package swim

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// SWIM implements the SWIM protocol for cluster membership and failure detection
// Section 1: "SWIM... is a generic software module... for peer-to-peer group membership"
type SWIM struct {
	config     *Config
	memberList *MemberList
	gossip     *GossipManager
	transport  Transport
	probe      *ProbeScheduler

	suspectTimers sync.Map // map[memberID]*time.Timer

	shutdownCh chan struct{}
	wg         sync.WaitGroup

	// Event callbacks
	onMemberJoin   func(*Member)
	onMemberLeave  func(*Member)
	onMemberFailed func(*Member)
	onMemberUpdate func(*Member)

	mu sync.RWMutex
}

// New creates a new SWIM instance
func New(config *Config) (*SWIM, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Validate configuration
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Create member list
	memberList := NewMemberList(config.NodeID, config.AdvertiseAddr)

	// Create gossip manager
	gossip := NewGossipManager(config.NumGossipRetransmissions, config.MaxGossipPacketSize)

	// Create transport
	transport := NewUDPTransport(config.BindAddr, config.Logger)

	swim := &SWIM{
		config:     config,
		memberList: memberList,
		gossip:     gossip,
		transport:  transport,
		shutdownCh: make(chan struct{}),
	}

	// Create probe scheduler
	swim.probe = NewProbeScheduler(swim)

	// Set message handler
	transport.SetMessageHandler(swim.handleMessage)

	return swim, nil
}

// validateConfig validates the SWIM configuration
func validateConfig(config *Config) error {
	if config.NodeID == "" {
		return fmt.Errorf("NodeID is required")
	}
	if config.BindAddr == "" {
		return fmt.Errorf("BindAddr is required")
	}
	if config.AdvertiseAddr == "" {
		return fmt.Errorf("AdvertiseAddr is required")
	}
	if config.ProbeTimeout >= config.ProbeInterval {
		return fmt.Errorf("ProbeTimeout must be less than ProbeInterval")
	}
	return nil
}

// Start starts the SWIM protocol
func (s *SWIM) Start() error {
	s.config.Logger.Infof("[SWIM] Starting SWIM node %s at %s", s.config.NodeID, s.config.AdvertiseAddr)

	// Start transport
	if err := s.transport.Start(); err != nil {
		return fmt.Errorf("failed to start transport: %w", err)
	}

	// Join cluster if seed nodes are provided
	if len(s.config.JoinNodes) > 0 {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.joinCluster()
		}()
	}

	// Start probe scheduler
	s.probe.Start()

	// Start periodic maintenance
	s.wg.Add(1)
	go s.runMaintenance()

	s.config.Logger.Infof("[SWIM] SWIM node started successfully")
	return nil
}

// Stop stops the SWIM protocol
func (s *SWIM) Stop() error {
	s.config.Logger.Infof("[SWIM] Stopping SWIM node %s", s.config.NodeID)

	// Announce voluntary leave
	// Section 4.1: "voluntary leave notification"
	s.announceLeave()

	// Stop components
	s.probe.Stop()
	close(s.shutdownCh)
	if err := s.transport.Stop(); err != nil {
		s.config.Logger.Errorf("[SWIM] Error stopping transport: %v", err)
	}

	s.wg.Wait()

	s.config.Logger.Infof("[SWIM] SWIM node stopped")
	return nil
}

// joinCluster attempts to join the cluster via seed nodes
func (s *SWIM) joinCluster() {
	s.config.Logger.Infof("[SWIM] Attempting to join cluster via seed nodes: %v", s.config.JoinNodes)

	for _, seedAddr := range s.config.JoinNodes {
		msg := &Message{
			Type:        JoinMsg,
			From:        s.config.NodeID,
			FromAddr:    s.config.AdvertiseAddr,
			Incarnation: s.memberList.LocalMember().Incarnation,
		}

		if err := s.transport.SendMessage(seedAddr, msg); err != nil {
			s.config.Logger.Errorf("[SWIM] Failed to send join message to %s: %v", seedAddr, err)
			continue
		}

		s.config.Logger.Infof("[SWIM] Sent join request to %s", seedAddr)

		// Wait a bit for response
		time.Sleep(100 * time.Millisecond)

		// Request full membership sync
		syncMsg := &Message{
			Type:     SyncMsg,
			From:     s.config.NodeID,
			FromAddr: s.config.AdvertiseAddr,
		}

		if err := s.transport.SendMessage(seedAddr, syncMsg); err != nil {
			s.config.Logger.Errorf("[SWIM] Failed to send sync message to %s: %v", seedAddr, err)
		}
	}
}

// announceLeave announces voluntary departure from the cluster
func (s *SWIM) announceLeave() {
	// Increment incarnation for leave announcement
	incarnation := s.memberList.IncrementIncarnation()

	update := Update{
		MemberID:    s.config.NodeID,
		Address:     s.config.AdvertiseAddr,
		Status:      Left,
		Incarnation: incarnation,
		Timestamp:   time.Now(),
	}

	// Send to all alive members
	members := s.memberList.GetAliveMembers()
	for _, member := range members {
		msg := &Message{
			Type:        LeaveMsg,
			From:        s.config.NodeID,
			FromAddr:    s.config.AdvertiseAddr,
			Incarnation: incarnation,
			Piggyback:   []Update{update},
		}

		if err := s.transport.SendMessage(member.Address, msg); err != nil {
			s.config.Logger.Errorf("[SWIM] Error sending leave message to %s: %v", member.ID, err)
		}
	}
}

// runMaintenance performs periodic maintenance tasks
func (s *SWIM) runMaintenance() {
	defer s.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.performMaintenance()
		case <-s.shutdownCh:
			return
		}
	}
}

// performMaintenance cleans up old members and performs housekeeping
func (s *SWIM) performMaintenance() {
	members := s.memberList.GetMembers()
	now := time.Now()

	for _, member := range members {
		// Remove members that have been failed or left for a long time
		if (member.Status == Failed || member.Status == Left) &&
			now.Sub(member.LocalTime) > 60*time.Second {
			s.memberList.RemoveMember(member.ID)
			s.config.Logger.Debugf("[SWIM] Removed old member %s from list", member.ID)
		}
	}
}

// handleMessage handles incoming SWIM protocol messages
func (s *SWIM) handleMessage(msg *Message) {
	// Process piggybacked updates first
	// Section 4.4: "Updates are sent via piggybacking"
	if len(msg.Piggyback) > 0 {
		s.gossip.ProcessIncomingUpdates(msg.Piggyback, s.memberList, func(update Update) {
			s.handleMembershipUpdate(update)
		})
	}

	// Handle message based on type
	switch msg.Type {
	case PingMsg:
		s.handlePing(msg)
	case AckMsg:
		s.probe.HandleAck(msg)
	case PingReqMsg:
		s.handlePingReq(msg)
	case IndirectPingMsg:
		s.handleIndirectPing(msg)
	case IndirectAckMsg:
		s.probe.HandleIndirectAck(msg)
	case SuspectMsg:
		s.handleSuspectMsg(msg)
	case AliveMsg:
		s.handleAliveMsg(msg)
	case ConfirmMsg:
		s.handleConfirmMsg(msg)
	case LeaveMsg:
		s.handleLeaveMsg(msg)
	case JoinMsg:
		s.handleJoinMsg(msg)
	case SyncMsg:
		s.handleSyncMsg(msg)
	default:
		s.config.Logger.Warnf("[SWIM] Unknown message type: %v", msg.Type)
	}
}

// handlePing handles an incoming ping message
// Section 3: "Mi responds with an ack"
func (s *SWIM) handlePing(msg *Message) {
	s.config.Logger.Debugf("[SWIM] Received ping from %s (seq=%d)", msg.From, msg.SeqNo)

	// Update member info if needed
	s.memberList.AddMember(msg.From, msg.FromAddr, Alive, msg.Incarnation)

	// Send ACK
	ackMsg := &Message{
		Type:        AckMsg,
		From:        s.config.NodeID,
		FromAddr:    s.config.AdvertiseAddr,
		Target:      msg.From,
		TargetAddr:  msg.FromAddr,
		SeqNo:       msg.SeqNo,
		Incarnation: s.memberList.LocalMember().Incarnation,
		Piggyback:   s.gossip.GetUpdatesToGossip(10),
	}

	if err := s.transport.SendMessage(msg.FromAddr, ackMsg); err != nil {
		s.config.Logger.Errorf("[SWIM] Error sending ACK to %s: %v", msg.From, err)
	}
}

// handlePingReq handles an incoming ping-req message
// Section 3: "Mk sends an indirect ping to Mi... and forwards the ack from Mi, if received"
func (s *SWIM) handlePingReq(msg *Message) {
	s.config.Logger.Debugf("[SWIM] Received ping-req from %s for target %s (seq=%d)",
		msg.From, msg.Target, msg.SeqNo)

	// Send indirect ping to the target
	indirectPingMsg := &Message{
		Type:       IndirectPingMsg,
		From:       s.config.NodeID,
		FromAddr:   s.config.AdvertiseAddr,
		Target:     msg.Target,
		TargetAddr: msg.TargetAddr,
		SeqNo:      msg.SeqNo,
		Piggyback:  s.gossip.GetUpdatesToGossip(10),
	}

	// Send to target
	if err := s.transport.SendMessage(msg.TargetAddr, indirectPingMsg); err != nil {
		s.config.Logger.Errorf("[SWIM] Error sending indirect ping to %s: %v", msg.Target, err)
		return
	}

	// Set timeout for indirect ping response
	time.AfterFunc(s.config.ProbeTimeout, func() {
		// If we haven't received an ack by now, we won't send anything back
		// The original requester will handle the timeout
	})
}

// handleIndirectPing handles an incoming indirect ping
func (s *SWIM) handleIndirectPing(msg *Message) {
	s.config.Logger.Debugf("[SWIM] Received indirect ping from %s (seq=%d)", msg.From, msg.SeqNo)

	// Update member info
	s.memberList.AddMember(msg.From, msg.FromAddr, Alive, msg.Incarnation)

	// Send indirect ACK back to the original requester (not the intermediary)
	// But we need to know who the original requester is - this should be in the message
	// For now, send back to the intermediary who will forward it
	indirectAckMsg := &Message{
		Type:        IndirectAckMsg,
		From:        s.config.NodeID,
		FromAddr:    s.config.AdvertiseAddr,
		Target:      msg.From,
		TargetAddr:  msg.FromAddr,
		SeqNo:       msg.SeqNo,
		Incarnation: s.memberList.LocalMember().Incarnation,
		Piggyback:   s.gossip.GetUpdatesToGossip(10),
	}

	if err := s.transport.SendMessage(msg.FromAddr, indirectAckMsg); err != nil {
		s.config.Logger.Errorf("[SWIM] Error sending indirect ACK to %s: %v", msg.From, err)
	}
}

// handleSuspectMsg handles a suspect announcement
func (s *SWIM) handleSuspectMsg(msg *Message) {
	s.config.Logger.Debugf("[SWIM] Received suspect message for %s from %s", msg.Target, msg.From)

	// If this is about us, refute it
	if msg.Target == s.config.NodeID {
		s.refuteSuspicion()
		return
	}

	// Update member status
	if s.memberList.UpdateMemberStatus(msg.Target, Suspect, msg.Incarnation) {
		s.handleSuspicion(s.getMember(msg.Target))
	}
}

// handleAliveMsg handles an alive announcement
// Section 4.3: "member voluntarily refutes... by multicasting an Alive message"
func (s *SWIM) handleAliveMsg(msg *Message) {
	s.config.Logger.Debugf("[SWIM] Received alive message for %s", msg.From)

	s.handleAlive(msg.From, msg.Incarnation)
}

// handleConfirmMsg handles a confirmed failure announcement
func (s *SWIM) handleConfirmMsg(msg *Message) {
	s.config.Logger.Infof("[SWIM] Received failure confirmation for %s", msg.Target)

	member := s.getMember(msg.Target)
	if member != nil {
		s.handleFailure(member)
	}
}

// handleLeaveMsg handles a voluntary leave announcement
func (s *SWIM) handleLeaveMsg(msg *Message) {
	s.config.Logger.Infof("[SWIM] Member %s is voluntarily leaving", msg.From)

	s.memberList.UpdateMemberStatus(msg.From, Left, msg.Incarnation)

	member := s.getMember(msg.From)
	if member != nil && s.onMemberLeave != nil {
		s.onMemberLeave(member)
	}
}

// handleJoinMsg handles a join request
func (s *SWIM) handleJoinMsg(msg *Message) {
	s.config.Logger.Infof("[SWIM] Member %s is joining from %s", msg.From, msg.FromAddr)

	// Add the new member
	if s.memberList.AddMember(msg.From, msg.FromAddr, Alive, msg.Incarnation) {
		member := s.getMember(msg.From)
		if member != nil && s.onMemberJoin != nil {
			s.onMemberJoin(member)
		}

		// Gossip this join to others
		update := Update{
			MemberID:    msg.From,
			Address:     msg.FromAddr,
			Status:      Alive,
			Incarnation: msg.Incarnation,
			Timestamp:   time.Now(),
		}
		s.gossip.AddUpdate(update)
	}

	// Send ACK with our info
	ackMsg := &Message{
		Type:        AckMsg,
		From:        s.config.NodeID,
		FromAddr:    s.config.AdvertiseAddr,
		Target:      msg.From,
		SeqNo:       msg.SeqNo,
		Incarnation: s.memberList.LocalMember().Incarnation,
		Piggyback:   s.gossip.GetUpdatesToGossip(10),
	}

	if err := s.transport.SendMessage(msg.FromAddr, ackMsg); err != nil {
		s.config.Logger.Errorf("[SWIM] Error sending ACK to joining member %s: %v", msg.From, err)
	}
}

// handleSyncMsg handles a membership sync request
func (s *SWIM) handleSyncMsg(msg *Message) {
	s.config.Logger.Debugf("[SWIM] Received sync request from %s", msg.From)

	// Send all our membership information
	members := s.memberList.GetMembers()
	updates := make([]Update, 0, len(members))

	for _, member := range members {
		if member.Status == Alive || member.Status == Suspect {
			updates = append(updates, Update{
				MemberID:    member.ID,
				Address:     member.Address,
				Status:      member.Status,
				Incarnation: member.Incarnation,
				Timestamp:   member.LocalTime,
			})
		}
	}

	// Add ourselves
	local := s.memberList.LocalMember()
	updates = append(updates, Update{
		MemberID:    local.ID,
		Address:     local.Address,
		Status:      Alive,
		Incarnation: local.Incarnation,
		Timestamp:   time.Now(),
	})

	// Send response
	syncResponse := &Message{
		Type:      AckMsg,
		From:      s.config.NodeID,
		FromAddr:  s.config.AdvertiseAddr,
		Piggyback: updates,
	}

	if err := s.transport.SendMessage(msg.FromAddr, syncResponse); err != nil {
		s.config.Logger.Errorf("[SWIM] Error sending sync response to %s: %v", msg.From, err)
	}
}

// handleMembershipUpdate handles a membership update from gossip
func (s *SWIM) handleMembershipUpdate(update Update) {
	member := s.getMember(update.MemberID)
	if member == nil {
		return
	}

	oldStatus := member.Status

	switch update.Status {
	case Alive:
		if oldStatus == Suspect {
			s.cancelSuspectTimer(member.ID)
		}
	case Suspect:
		if oldStatus == Alive {
			s.handleSuspicion(member)
		}
	case Failed:
		s.handleFailure(member)
	case Left:
		if s.onMemberLeave != nil {
			s.onMemberLeave(member)
		}
	}

	if s.onMemberUpdate != nil && oldStatus != update.Status {
		s.onMemberUpdate(member)
	}
}

// handleSuspicion handles suspicion of a member
// Section 4.2: "if member Mj suspects Mi... disseminates this suspicion"
func (s *SWIM) handleSuspicion(member *Member) {
	if member == nil {
		return
	}

	s.config.Logger.Warnf("[SWIM] Suspecting member %s", member.ID)

	// Cancel any existing suspect timer
	s.cancelSuspectTimer(member.ID)

	// Calculate suspicion timeout
	// Section 4.2: "timeout should be chosen to be at least log(n)"
	clusterSize := s.memberList.NumMembers()
	suspicionTimeout := s.calculateSuspicionTimeout(clusterSize)

	// Start suspect timer
	timer := time.AfterFunc(suspicionTimeout, func() {
		s.handleSuspicionTimeout(member.ID)
	})

	s.suspectTimers.Store(member.ID, timer)

	// Gossip the suspicion
	update := Update{
		MemberID:    member.ID,
		Address:     member.Address,
		Status:      Suspect,
		Incarnation: member.Incarnation,
		Timestamp:   time.Now(),
	}
	s.gossip.AddUpdate(update)
}

// calculateSuspicionTimeout calculates suspicion timeout based on cluster size
// Section 4.2: "subgroup membership protocol period... is chosen as a small multiple of log(n)"
func (s *SWIM) calculateSuspicionTimeout(clusterSize int) time.Duration {
	if clusterSize < 2 {
		return s.config.SuspicionTimeout
	}

	// Use log(n) scaling factor
	logN := math.Log(float64(clusterSize))
	multiplier := float64(s.config.SuspicionMultiplier) * logN

	timeout := time.Duration(multiplier * float64(s.config.ProtocolPeriod))

	// Apply minimum
	if timeout < s.config.SuspicionTimeout {
		timeout = s.config.SuspicionTimeout
	}

	return timeout
}

// handleSuspicionTimeout handles timeout of suspicion period
func (s *SWIM) handleSuspicionTimeout(memberID string) {
	member := s.getMember(memberID)
	if member == nil || member.Status != Suspect {
		return
	}

	s.config.Logger.Warnf("[SWIM] Suspicion timeout for %s, marking as failed", memberID)
	s.handleFailure(member)
}

// handleAlive handles an alive announcement (usually a refutation)
// Section 4.3: "refutes a suspicion... via disseminating an Alive message"
func (s *SWIM) handleAlive(memberID string, incarnation uint64) {
	member := s.getMember(memberID)
	if member == nil {
		return
	}

	if s.memberList.UpdateMemberStatus(memberID, Alive, incarnation) {
		s.config.Logger.Infof("[SWIM] Member %s is alive (incarnation=%d)", memberID, incarnation)

		// Cancel suspect timer
		s.cancelSuspectTimer(memberID)

		// Gossip the alive status
		update := Update{
			MemberID:    memberID,
			Address:     member.Address,
			Status:      Alive,
			Incarnation: incarnation,
			Timestamp:   time.Now(),
		}
		s.gossip.AddUpdate(update)
	}
}

// handleFailure handles confirmed failure of a member
func (s *SWIM) handleFailure(member *Member) {
	if member == nil {
		return
	}

	s.config.Logger.Warnf("[SWIM] Member %s has failed", member.ID)

	// Cancel suspect timer
	s.cancelSuspectTimer(member.ID)

	// Update status
	s.memberList.UpdateMemberStatus(member.ID, Failed, member.Incarnation)

	// Notify callback
	if s.onMemberFailed != nil {
		s.onMemberFailed(member)
	}

	// Gossip the failure
	update := Update{
		MemberID:    member.ID,
		Address:     member.Address,
		Status:      Failed,
		Incarnation: member.Incarnation,
		Timestamp:   time.Now(),
	}
	s.gossip.AddUpdate(update)
}

// refuteSuspicion refutes a suspicion about the local node
// Section 4.3: "member voluntarily increases its own incarnation number"
func (s *SWIM) refuteSuspicion() {
	incarnation := s.memberList.IncrementIncarnation()

	s.config.Logger.Infof("[SWIM] Refuting suspicion with incarnation %d", incarnation)

	// Broadcast alive message
	local := s.memberList.LocalMember()
	update := Update{
		MemberID:    local.ID,
		Address:     local.Address,
		Status:      Alive,
		Incarnation: incarnation,
		Timestamp:   time.Now(),
	}
	s.gossip.AddUpdate(update)

	// Send direct alive messages to all known members
	members := s.memberList.GetAliveMembers()
	for _, member := range members {
		msg := &Message{
			Type:        AliveMsg,
			From:        s.config.NodeID,
			FromAddr:    s.config.AdvertiseAddr,
			Incarnation: incarnation,
			Piggyback:   []Update{update},
		}
		if err := s.transport.SendMessage(member.Address, msg); err != nil {
			s.config.Logger.Errorf("[SWIM] Error sending alive message to %s: %v", member.ID, err)
		}
	}
}

// cancelSuspectTimer cancels the suspect timer for a member
func (s *SWIM) cancelSuspectTimer(memberID string) {
	if val, ok := s.suspectTimers.LoadAndDelete(memberID); ok {
		if timer, ok := val.(*time.Timer); ok {
			timer.Stop()
		}
	}
}

// getMember retrieves a member by ID
func (s *SWIM) getMember(memberID string) *Member {
	member, _ := s.memberList.GetMember(memberID)
	return member
}

// GetMembers returns a snapshot of all members
func (s *SWIM) GetMembers() []*Member {
	return s.memberList.GetMembers()
}

// GetAliveMembers returns only alive members
func (s *SWIM) GetAliveMembers() []*Member {
	return s.memberList.GetAliveMembers()
}

// NumMembers returns the total number of members
func (s *SWIM) NumMembers() int {
	return s.memberList.NumMembers()
}

// LocalNode returns the local member
func (s *SWIM) LocalNode() *Member {
	return s.memberList.LocalMember()
}

// OnMemberJoin registers a callback for when a member joins
func (s *SWIM) OnMemberJoin(callback func(*Member)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onMemberJoin = callback
}

// OnMemberLeave registers a callback for when a member leaves
func (s *SWIM) OnMemberLeave(callback func(*Member)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onMemberLeave = callback
}

// OnMemberFailed registers a callback for when a member fails
func (s *SWIM) OnMemberFailed(callback func(*Member)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onMemberFailed = callback
}

// OnMemberUpdate registers a callback for when a member is updated
func (s *SWIM) OnMemberUpdate(callback func(*Member)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onMemberUpdate = callback
}
