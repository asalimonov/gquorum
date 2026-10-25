package raftengine

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const readyBatchRecencyWindow = 10 * time.Millisecond

func (g *Group) run() {
	defer g.wg.Done()
	defer g.finishRun()
	ticker := time.NewTicker(g.opts.Timing.TickInterval)
	defer ticker.Stop()
	g.sampleRaftMetrics(time.Now())
	metricsTicker := time.NewTicker(g.opts.Metrics.SampleInterval)
	defer metricsTicker.Stop()
	state := loopState{ticks: ticker.C, metrics: metricsTicker.C}
	defer state.stopReadyTimer()
	for {
		g.pruneWaiters()
		g.resetReadRounds()
		g.resumeAuthorizedTransfer()
		if err := g.prepareReady(&state); err != nil {
			g.halt(err)
			return
		}
		if g.pending == nil && g.applied >= g.recoveryCommit {
			g.readyOnce.Do(func() { close(g.ready) })
		}
		g.refresh()
		if g.pending == nil && state.readyTimer == nil {
			g.completeAppliedReads()
		}
		g.issueReadRound(time.Now())
		if g.pending == nil && state.readyTimer == nil && g.rn.HasReady() {
			continue
		}
		if !g.receiveLoopEvent(&state) {
			return
		}
	}
}
func (g *Group) handleProposal(req *request) {
	refuse := func(err error) { delete(g.props, req.correlation); answer(req, Result{}, err) }
	if err := g.proposalAdmission(req); err != nil {
		refuse(err)
		return
	}
	requestID := req.proposal.RequestID
	if requestID == "" {
		requestID = req.correlation
	}
	data, err := raftstate.EncodeEntry(raftstate.Entry{
		Schema:      raftstate.Schema,
		Kind:        raftstate.Application,
		Correlation: req.correlation,
		RequestID:   requestID,
		Data:        req.proposal.Data,
	})
	if err == nil && uint64(len(data)) > g.opts.MaxUncommittedBytes {
		err = fault.ErrOverloaded
	}
	if err == nil {
		err = g.rn.Propose(data)
	}
	if err != nil {
		if g.opts.GroupID != topology.MetaGroupID && !req.retried && errors.Is(err, raft.ErrProposalDropped) {
			req.retried = true
			req.retryAt = time.Now().Add(g.opts.Timing.TickInterval)
			g.props[req.correlation] = req
			return
		}
		if errors.Is(err, raft.ErrProposalDropped) {
			err = fault.New(fault.Unavailable, "Raft did not accept proposal", err)
		}
		refuse(err)
		return
	}
	g.props[req.correlation] = req
}
func (g *Group) handleControl(req *request) {
	if req.admin != nil {
		g.handleAuthority(req)
		return
	}
	if g.opts.ManagedAuthority {
		answer(req, Result{}, fault.ErrUnsupported)
		return
	}
	if err := req.ctx.Err(); err != nil {
		answer(req, Result{}, err)
		return
	}
	if g.rn.BasicStatus().RaftState != raft.StateLeader {
		answer(req, Result{}, fault.ErrNotLeader)
		return
	}
	if req.change != nil {
		if len(g.changes) > 0 || g.pendingConfEntry() {
			answer(req, Result{}, fault.ErrOverloaded)
			return
		}
		cc := *req.change
		data, err := raftstate.EncodeEntry(raftstate.Entry{
			Schema:      raftstate.Schema,
			Kind:        raftstate.StandaloneConfiguration,
			Correlation: req.correlation,
			Data:        cc.Context,
		})
		if err != nil {
			answer(req, Result{}, err)
			return
		}
		cc.Context = data
		if err := g.rn.ProposeConfChange(cc); err != nil {
			answer(req, Result{}, err)
			return
		}
		g.changes[req.correlation] = req
	} else {
		progress, ok := g.rn.Status().Progress[req.transfer]
		if !ok || progress.IsLearner {
			answer(req, Result{}, fault.ErrUnsafeMembership)
			return
		}
		g.rn.TransferLeader(req.transfer)
		answer(req, Result{}, nil)
	}
}

// A configuration entry from an earlier leader may still be unapplied even
// though this process has no local change waiter. Raft can silently turn a
// second change into a no-op; refuse it until the first entry is applied.
func (g *Group) pendingConfEntry() bool {
	last, err := g.ms.LastIndex()
	if err != nil || last <= g.applied {
		return false
	}
	entries, err := g.ms.Entries(g.applied+1, last+1, ^uint64(0))
	if err != nil {
		return true
	}
	for _, entry := range entries {
		if entry.Type == raftpb.EntryConfChange || entry.Type == raftpb.EntryConfChangeV2 {
			return true
		}
	}
	return false
}
func (g *Group) pruneWaiters() {
	for id, req := range g.props {
		if req.ctx.Err() != nil {
			delete(g.props, id)
		}
	}
	// A canceled membership caller does not free the outstanding transition.
	// Keep its slot until apply/Advance so another change cannot be silently
	// dropped.
}
func (g *Group) beginReady() error {
	rd := g.rn.Ready()
	if err := g.classifyOwnedSnapshotReady(&rd); err != nil {
		return err
	}
	rd, jobs, err := g.ownReady(rd)
	if err != nil {
		return err
	}
	retained := false
	defer func() {
		if !retained {
			discardSnapshotJobs(jobs, fault.ErrStopped)
		}
	}()
	if err := g.stageReadySnapshot(&rd); err != nil {
		return err
	}
	if err := g.ms.Append(rd.Entries); err != nil {
		return err
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := g.ms.SetHardState(rd.HardState); err != nil {
			return err
		}
	}
	job := writeJob{kind: writeReady, rd: rd, ingress: g.activeSnapshot, snapshotJobs: jobs}
	retained = true
	g.pending = &job
	if len(rd.Entries) == 0 && raft.IsEmptySnap(rd.Snapshot) && raft.IsEmptyHardState(rd.HardState) {
		return g.finishWrite(writeReady)
	}
	g.writeC <- job
	return nil
}
func (g *Group) finishWrite(kind writeKind) error {
	if g.pending == nil || g.pending.kind != kind {
		return errors.New("unexpected persistence completion")
	}
	job := g.pending
	if kind == writeRecoveryCheckpoint || kind == writeRecoveryFinish {
		return g.finishRecoveryCheckpoint()
	}
	if kind == writeSnapshot {
		if err := g.ms.Compact(job.snapshot.Metadata.Index); err != nil {
			return err
		}
		g.snapshotIndex = job.snapshot.Metadata.Index
		g.pending = nil
		answer(job.request, Result{}, nil)
		return nil
	}
	rd := job.rd
	if !raft.IsEmptyHardState(rd.HardState) {
		g.durableHS = rd.HardState
	}
	if err := g.restoreReadySnapshot(rd.Snapshot); err != nil {
		return err
	}
	if err := g.applyCommittedEntries(rd.CommittedEntries); err != nil {
		return err
	}
	if err := g.observeAppliedReady(rd); err != nil {
		return err
	}
	return g.advance()
}
func (g *Group) apply(entry raftpb.Entry) error {
	if err := validateEntryMode(g.opts, entry); err != nil {
		return err
	}
	switch entry.Type {
	case raftpb.EntryNormal:
		return g.applyNormalEntry(entry)
	case raftpb.EntryConfChangeV2:
		return g.applyConfigurationEntry(entry)
	default:
		return fault.New(fault.Unsupported, "only V2 membership entries are supported", nil)
	}
}
func (g *Group) advance() error {
	// Publish applied membership before the sender routes this Ready. A newly
	// admitted peer must not be mistaken for a retired destination by stale
	// cache.
	g.refresh()
	rd := g.pending.rd
	rd.Messages = append(rd.Messages, g.retirementMessages...)
	g.retirementMessages = nil
	if len(rd.Messages) > 0 {
		select {
		case g.sendC <- rd.Messages:
		default:
			for _, m := range rd.Messages {
				g.rn.ReportUnreachable(m.To)
				if m.Type == raftpb.MsgSnap {
					g.ReportSnapshot(m.To, raft.SnapshotFailure)
				}
			}
		}
	}
	jobs := g.pending.snapshotJobs
	g.pending.snapshotJobs = nil
	g.dispatchSnapshotJobs(jobs)
	g.acceptReadStates(rd.ReadStates)
	g.releaseRemoteReads(rd.Messages)
	g.rn.Advance(rd)
	g.pending = nil
	for _, req := range g.confNotify {
		answer(req, Result{}, nil)
	}
	g.confNotify = nil
	for _, reply := range g.adminNotify {
		answer(reply.req, reply.result, reply.err)
	}
	g.adminNotify = nil
	if g.membershipChanged || g.applied-g.snapshotIndex >= g.opts.SnapshotThreshold {
		if err := g.beginSnapshot(nil); err != nil {
			g.opts.Logger.Warn("automatic snapshot failed", "group", g.opts.GroupID, "error", err)
		} else {
			g.membershipChanged = false
		}
	}
	return nil
}

// Once ApplyConfChange drops a peer, Raft no longer retries its final commit.
// Queue a bounded, prefix-checked append carrying the committed removal itself.
// Advance sends it only after application durability, through the same FIFO as
// ordinary messages. This is best effort for an offline/compacted-away peer;
// it never fabricates membership or commits an unmatched follower log.
func (g *Group) queueRetirement(entry raftpb.Entry, peer uint64) {
	status := g.rn.Status()
	progress, present := status.Progress[peer]
	if status.RaftState != raft.StateLeader || !present {
		return
	}
	first, err := g.ms.FirstIndex()
	if err != nil {
		return
	}
	base := max(first-1, min(progress.Match, entry.Index-1))
	term, err := g.ms.Term(base)
	if err != nil {
		return
	}
	entries, err := g.ms.Entries(base+1, entry.Index+1, g.opts.MaxApplyBatchBytes)
	if err != nil || len(entries) == 0 {
		return
	}
	if entries[len(entries)-1].Index != entry.Index {
		// Keep the handoff bounded if the peer is far behind. A prefix match is
		// still mandatory at the receiver; unavailable old state is not
		// replaced.
		base = entry.Index - 1
		term, err = g.ms.Term(base)
		if err != nil {
			return
		}
		entries = []raftpb.Entry{entry}
	}
	g.retirementMessages = append(g.retirementMessages, raftpb.Message{
		Type:    raftpb.MsgApp,
		From:    g.opts.ReplicaID,
		To:      peer,
		Term:    status.Term,
		Index:   base,
		LogTerm: term,
		Entries: copyEntries(entries),
		Commit:  entry.Index,
	})
}
func (g *Group) beginSnapshot(req *request) error {
	if req != nil && req.recoveryCheckpoint != nil {
		return g.beginRecoveryCheckpoint(req)
	}
	if req != nil && req.ctx.Err() != nil {
		return req.ctx.Err()
	}
	if g.applied <= g.snapshotIndex {
		answer(req, Result{}, nil)
		return nil
	}
	ctx := g.ctx
	if req != nil {
		ctx = req.ctx
	}
	data, err := g.opts.Machine.Snapshot(ctx)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return fault.New(fault.Invalid, "machine snapshot must be nonempty", nil)
	}
	data, err = encodeSnapshot(g.opts, data, g.authority)
	if err != nil {
		return err
	}
	cs := copyConf(g.cs)
	if g.snapshotPublication == math.MaxUint64 {
		return fault.ErrOverloaded
	}
	ss, err := g.ms.CreateSnapshot(g.applied, &cs, bytes.Clone(data))
	if err != nil {
		return err
	}
	g.snapshotPublication++
	job := writeJob{kind: writeSnapshot, snapshot: ss, request: req}
	g.pending = &job
	g.writeC <- job
	return nil
}
func copyEntries(entries []raftpb.Entry) []raftpb.Entry {
	if entries == nil {
		return nil
	}
	result := make([]raftpb.Entry, len(entries))
	for i, e := range entries {
		result[i] = e
		result[i].Data = bytes.Clone(e.Data)
	}
	return result
}
func (g *Group) refresh() {
	status := g.rn.Status()
	g.refreshSnapshotEpochStatus(status)
	bounds := SnapshotBounds{RunID: g.nonce, Term: status.Term, Commit: status.Commit}
	status.Commit = min(status.Commit, g.durableHS.Commit)
	status.Applied = g.applied
	g.mu.Lock()
	old := g.cached
	g.snapshotBounds = bounds
	g.cached = status
	g.cachedPending = len(g.props)
	g.publishObservationLocked(regularObservation)
	g.mu.Unlock()
	if old.Lead != status.Lead {
		g.emit(events.Event{
			Type:      events.LeaderChange,
			OldLeader: fmt.Sprint(old.Lead),
			NewLeader: fmt.Sprint(status.Lead),
			Term:      status.Term,
		})
	}
	if old.RaftState == raft.StateLeader && status.RaftState != raft.StateLeader {
		if g.opts.GroupID == topology.MetaGroupID {
			for id, req := range g.props {
				answer(req, Result{}, fault.ErrNotLeader)
				delete(g.props, id)
			}
		}
		for id, req := range g.changes {
			answer(req, Result{}, fault.ErrNotLeader)
			delete(g.changes, id)
		}
	}
}
func (g *Group) emit(e events.Event) {
	if g.opts.Events != nil {
		e.InstanceID = g.opts.InstanceID
		e.ReplicaID = g.opts.ReplicaID
		if g.opts.GroupID == topology.MetaGroupID {
			e.MemberID = g.opts.ReplicaID
		}
		e.NodeName = g.opts.NodeName
		e.ClusterID = g.opts.ClusterID
		e.GroupID = g.opts.GroupID
		g.opts.Events.Publish(e)
	}
}
func (g *Group) halt(err error) {
	if cause := g.ctx.Err(); cause != nil && errors.Is(err, cause) {
		return
	}
	g.mu.Lock()
	if g.err == nil {
		g.err = err
	}
	g.stopping = true
	g.publishObservationLocked(regularObservation)
	g.mu.Unlock()
	g.emit(events.Event{Type: events.RuntimeError, Code: "raft_halted"})
	cause := err
	for unwrapped := errors.Unwrap(cause); unwrapped != nil; unwrapped = errors.Unwrap(cause) {
		cause = unwrapped
	}
	g.opts.Logger.Error("Raft group halted", "group", g.opts.GroupID, "error", err, "cause", cause)
}
