package swim

import (
	"sync"
	"sync/atomic"
	"time"
)

// ProbeScheduler manages the failure detection protocol
// Section 3: "each member periodically picks some member at random and executes the SWIM failure detector protocol"
type ProbeScheduler struct {
	swim          *SWIM
	ticker        *time.Ticker
	stopCh        chan struct{}
	wg            sync.WaitGroup
	seqNo         uint64   // Sequence number for tracking probe requests
	pendingProbes sync.Map // map[uint64]*probeContext
}

// probeContext tracks the state of an ongoing probe
type probeContext struct {
	targetID      string
	targetAddr    string
	seqNo         uint64
	startTime     time.Time
	ackReceived   bool
	mu            sync.Mutex
	timer         *time.Timer
	indirectNodes []*Member // Nodes we asked to do indirect probes
}

// NewProbeScheduler creates a new probe scheduler
func NewProbeScheduler(swim *SWIM) *ProbeScheduler {
	return &ProbeScheduler{
		swim:   swim,
		stopCh: make(chan struct{}),
	}
}

// Start begins the periodic probing
// Section 3: "each member picks some member at random and sends a ping"
func (ps *ProbeScheduler) Start() {
	ps.ticker = time.NewTicker(ps.swim.config.ProbeInterval)
	ps.wg.Add(1)

	go func() {
		defer ps.wg.Done()
		for {
			select {
			case <-ps.ticker.C:
				ps.probe()
			case <-ps.stopCh:
				return
			}
		}
	}()
}

// Stop stops the probe scheduler
func (ps *ProbeScheduler) Stop() {
	if ps.ticker != nil {
		ps.ticker.Stop()
	}
	close(ps.stopCh)
	ps.wg.Wait()
}

// probe executes one round of the SWIM failure detection protocol
// Section 3: "Mj picks some member Mi at random... sends a ping"
func (ps *ProbeScheduler) probe() {
	// Select a random member to probe (excluding self)
	target := ps.swim.memberList.GetRandomMember(ps.swim.config.NodeID)
	if target == nil {
		// No members to probe
		return
	}

	seqNo := atomic.AddUint64(&ps.seqNo, 1)

	ps.swim.config.Logger.Debugf("[SWIM] Probing member %s (seq=%d)", target.ID, seqNo)

	// Create probe context
	ctx := &probeContext{
		targetID:   target.ID,
		targetAddr: target.Address,
		seqNo:      seqNo,
		startTime:  time.Now(),
	}

	ps.pendingProbes.Store(seqNo, ctx)

	// Send direct ping
	ps.sendPing(target, seqNo)

	// Set timeout for direct probe
	// Section 3: "if Mj does not receive an ack within a timeout period..."
	ctx.timer = time.AfterFunc(ps.swim.config.ProbeTimeout, func() {
		ps.handleProbeTimeout(ctx)
	})
}

// sendPing sends a ping message to a target
func (ps *ProbeScheduler) sendPing(target *Member, seqNo uint64) {
	msg := &Message{
		Type:        PingMsg,
		From:        ps.swim.config.NodeID,
		FromAddr:    ps.swim.config.AdvertiseAddr,
		Target:      target.ID,
		TargetAddr:  target.Address,
		SeqNo:       seqNo,
		Incarnation: ps.swim.memberList.LocalMember().Incarnation,
		Piggyback:   ps.swim.gossip.GetUpdatesToGossip(10),
	}

	if err := ps.swim.transport.SendMessage(target.Address, msg); err != nil {
		ps.swim.config.Logger.Errorf("[Probe] Error sending ping to %s: %v", target.ID, err)
	} else if ps.swim.metrics != nil {
		ps.swim.metrics.RecordPing()
	}
}

// handleProbeTimeout handles timeout of a direct probe
// Section 3: "Mj indirectly probes Mi by selecting k members at random and sending them a ping-req(Mi)"
func (ps *ProbeScheduler) handleProbeTimeout(ctx *probeContext) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	if ctx.ackReceived {
		return
	}

	ps.swim.config.Logger.Debugf("[SWIM] Direct probe timeout for %s, starting indirect probe", ctx.targetID)

	// Select k random members for indirect probing
	// Section 3: "selects k members at random"
	indirectNodes := ps.swim.memberList.GetRandomMembers(
		ps.swim.config.IndirectProbeCount,
		ps.swim.config.NodeID,
		ctx.targetID,
	)

	if len(indirectNodes) == 0 {
		// No nodes available for indirect probing, mark as suspect
		ps.handleProbeFailed(ctx)
		return
	}

	ctx.indirectNodes = indirectNodes

	// Send ping-req to each selected node
	for _, node := range indirectNodes {
		ps.sendPingReq(node, ctx)
	}

	// Set timeout for indirect probe
	// Section 3: "waits for a period less than or equal to the timeout used for direct pings"
	indirectTimeout := ps.swim.config.ProbeTimeout
	time.AfterFunc(indirectTimeout, func() {
		ps.handleIndirectProbeTimeout(ctx)
	})
}

// sendPingReq sends a ping-req message asking another node to probe the target
func (ps *ProbeScheduler) sendPingReq(node *Member, ctx *probeContext) {
	msg := &Message{
		Type:       PingReqMsg,
		From:       ps.swim.config.NodeID,
		FromAddr:   ps.swim.config.AdvertiseAddr,
		Target:     ctx.targetID,
		TargetAddr: ctx.targetAddr,
		SeqNo:      ctx.seqNo,
		Piggyback:  ps.swim.gossip.GetUpdatesToGossip(10),
	}

	if err := ps.swim.transport.SendMessage(node.Address, msg); err != nil {
		ps.swim.config.Logger.Errorf("[Probe] Error sending ping-req to %s: %v", node.ID, err)
	} else if ps.swim.metrics != nil {
		ps.swim.metrics.RecordPingReq()
	}
}

// handleIndirectProbeTimeout handles timeout of indirect probe
// Section 4.2: "declares Mi as suspected"
func (ps *ProbeScheduler) handleIndirectProbeTimeout(ctx *probeContext) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	if ctx.ackReceived {
		return
	}

	ps.handleProbeFailed(ctx)
}

// handleProbeFailed handles a failed probe (both direct and indirect failed)
// Section 4.2: "if no ack messages are received from Mi within the time-out period... suspects Mi"
func (ps *ProbeScheduler) handleProbeFailed(ctx *probeContext) {
	ps.pendingProbes.Delete(ctx.seqNo)

	member, exists := ps.swim.memberList.GetMember(ctx.targetID)
	if !exists {
		return
	}

	if ps.swim.config.EnableSuspicionMechanism {
		// Only mark as suspect if not already suspected or failed
		// This prevents recording multiple suspicions for the same failure
		if member.Status == Alive {
			// Mark as suspect instead of immediately failed
			// Section 4.2: "Suspicion mechanism... allows us to achieve low false positive rate"
			ps.swim.config.Logger.Infof("[SWIM] Marking member %s as SUSPECT (from probe failure)", ctx.targetID)
			ps.swim.handleSuspicion(member)
		} else {
			ps.swim.config.Logger.Debugf("[SWIM] Probe failed for %s but already in status %v, not recording suspicion", ctx.targetID, member.Status)
		}
		// If already Suspect or Failed, ignore the probe failure (already handled)
	} else {
		// Without suspicion, mark directly as failed
		ps.swim.config.Logger.Infof("[SWIM] Marking member %s as FAILED", ctx.targetID)
		ps.swim.handleFailure(member)
	}
}

// HandleAck handles an ACK message received in response to a probe
// Section 3: "Mi sends back an ack to Mj"
func (ps *ProbeScheduler) HandleAck(msg *Message) {
	val, ok := ps.pendingProbes.Load(msg.SeqNo)
	if !ok {
		// Not a probe we're tracking
		return
	}

	ctx := val.(*probeContext)
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	if ctx.ackReceived {
		return
	}

	ctx.ackReceived = true
	if ctx.timer != nil {
		ctx.timer.Stop()
	}

	ps.pendingProbes.Delete(msg.SeqNo)

	latency := time.Since(ctx.startTime)
	ps.swim.config.Logger.Debugf("[SWIM] Received ACK from %s (seq=%d, latency=%v)", msg.From, msg.SeqNo, latency)

	// Record probe latency
	if ps.swim.metrics != nil {
		ps.swim.metrics.RecordProbeLatency(latency)
	}

	// Update member status to alive if it was suspect
	member, exists := ps.swim.memberList.GetMember(msg.From)
	if exists && member.Status == Suspect {
		ps.swim.handleAlive(msg.From, msg.Incarnation)
	}
}

// HandleIndirectAck handles an indirect ACK (forwarded from ping-req)
func (ps *ProbeScheduler) HandleIndirectAck(msg *Message) {
	val, ok := ps.pendingProbes.Load(msg.SeqNo)
	if !ok {
		return
	}

	ctx := val.(*probeContext)
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	if ctx.ackReceived {
		return
	}

	ctx.ackReceived = true
	ps.pendingProbes.Delete(msg.SeqNo)

	ps.swim.config.Logger.Debugf("[SWIM] Received indirect ACK for %s via %s (seq=%d)", ctx.targetID, msg.From, msg.SeqNo)

	// Target is alive
	member, exists := ps.swim.memberList.GetMember(ctx.targetID)
	if exists && member.Status == Suspect {
		ps.swim.handleAlive(ctx.targetID, msg.Incarnation)
	}
}
