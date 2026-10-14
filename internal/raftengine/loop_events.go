package raftengine

import (
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
)

func (g *Group) finishRun() {
	g.cancel()
	g.mu.Lock()
	g.stopped = true
	if g.err == nil {
		g.err = fault.ErrStopped
	}
	g.cached.Lead = 0
	g.cached.RaftState = raft.StateFollower
	g.cachedPending = 0
	g.drainReadQueue(g.err)
	g.failReadRounds(g.err)
	// The loop exclusively owns these maps. Release canceled request data
	// even when a caller retains the stopped Group for inspection.
	clear(g.props)
	clear(g.changes)
	g.publishObservationLocked(regularObservation)
	g.mu.Unlock()
}

type loopState struct {
	lastEntriesWrite time.Time
	readyTimer       *time.Timer
	ticks, metrics   <-chan time.Time
}

func (s *loopState) stopReadyTimer() {
	if s.readyTimer != nil {
		s.readyTimer.Stop()
	}
}

func (g *Group) prepareReady(state *loopState) error {
	if g.pending == nil && state.readyTimer == nil && g.rn.HasReady() {
		// Delay capture, rather than an already frozen Ready: proposals and
		// inbound entries received during this window join the durable
		// batch.
		// The loop keeps handling ticks, cancellation and network traffic.
		if wait := g.opts.Timing.WALWriteWait; wait > 0 && !state.lastEntriesWrite.IsZero() &&
			time.Since(state.lastEntriesWrite) < readyBatchRecencyWindow {
			state.readyTimer = time.NewTimer(wait)
		} else {
			if err := g.beginReady(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *Group) receiveLoopEvent(state *loopState) bool {
	var snapshotC chan *request
	if g.pending == nil && state.readyTimer == nil {
		snapshotC = g.snapshots
	}
	var readyC <-chan time.Time
	controlC := g.controls
	if state.readyTimer != nil {
		readyC = state.readyTimer.C
		// Capture any incoming configuration entries before validating a
		// subsequent control against the local log and membership state.
		controlC = nil
	}
	select {
	case <-g.ctx.Done():
		return false
	case <-state.ticks:
		g.rn.Tick()
		g.refresh()
		g.retryDroppedProposals(time.Now())
		g.resetReadRounds()
		g.retryReadRound(time.Now())
	case now := <-state.metrics:
		g.sampleRaftMetrics(now)
	case <-readyC:
		if err := g.captureReady(state); err != nil {
			g.halt(err)
			return false
		}
	case result := <-g.writeDone:
		return g.completeLoopWrite(state, result)
	case req := <-g.reads:
		g.admitRead(req)
	case req := <-g.proposals:
		g.handleProposal(req)
	case req := <-g.inbound:
		g.handleInbound(req)
	case req := <-controlC:
		g.handleControl(req)
	case req := <-snapshotC:
		if err := g.beginSnapshot(req); err != nil {
			answer(req, Result{}, err)
		}
	case id := <-g.unreachable:
		g.rn.ReportUnreachable(id)
	case report := <-g.reports:
		g.rn.ReportSnapshot(report.id, report.status)
	}
	return true
}

func (g *Group) captureReady(state *loopState) error {
	state.readyTimer.Stop()
	state.readyTimer = nil
	if g.rn.HasReady() {
		if err := g.beginReady(); err != nil {
			return err
		}
	}
	return nil
}

func (g *Group) completeLoopWrite(state *loopState, result writeResult) bool {
	if result.err != nil {
		if g.ctx.Err() == nil {
			g.halt(result.err)
		}
		return false
	}
	entriesWritten := result.kind == writeReady && g.pending != nil && len(g.pending.rd.Entries) > 0
	if err := g.finishWrite(result.kind); err != nil {
		g.halt(err)
		return false
	}
	if entriesWritten {
		state.lastEntriesWrite = time.Now()
	}
	return true
}

func (g *Group) handleInbound(req *request) {
	if req.ctx.Err() != nil {
		answer(req, Result{}, req.ctx.Err())
		return
	}
	var err error
	for _, msg := range req.messages {
		if err = g.stepRemote(msg); err != nil {
			break
		}
		g.recordPeerAck(time.Now(), msg)
	}
	answer(req, Result{}, err)
}

func (g *Group) proposalAdmission(req *request) error {
	if err := req.ctx.Err(); err != nil {
		return err
	}
	if req.expectedTerm != 0 {
		if g.ctx != nil && g.ctx.Err() != nil {
			return fault.ErrStopped
		}
		status := g.rn.BasicStatus()
		if status.RaftState != raft.StateLeader || status.Term != req.expectedTerm ||
			!slices.Contains(g.cs.Voters, g.opts.ReplicaID) && !slices.Contains(g.cs.VotersOutgoing, g.opts.ReplicaID) {
			return fault.ErrNotLeader
		}
	}
	if g.opts.GroupID == topology.MetaGroupID && g.rn.BasicStatus().RaftState != raft.StateLeader {
		return fault.ErrNotLeader
	}
	select {
	case <-g.ready:
	default:
		return fault.ErrReplicaNotReady
	}
	stall := g.WALStall()
	if stall > time.Second || g.props[req.correlation] == nil && len(g.props) >= g.opts.MaxPendingProposals {
		return fault.ErrOverloaded
	}
	return nil
}
