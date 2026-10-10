package raftengine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func (g *Group) run() {
	defer g.wg.Done()
	defer func() {
		g.cancel()
		g.mu.Lock()
		g.stopped = true
		if g.err == nil {
			g.err = fault.ErrStopped
		}
		g.cached.Lead = 0
		g.cached.RaftState = raft.StateFollower
		g.mu.Unlock()
	}()
	ticker := time.NewTicker(g.opts.Timing.TickInterval)
	defer ticker.Stop()
	for {
		g.pruneWaiters()
		if g.pending == nil && g.rn.HasReady() {
			if err := g.beginReady(); err != nil {
				g.halt(err)
				return
			}
		}
		g.refresh()
		if g.pending == nil && g.applied >= g.recoveryCommit {
			g.readyOnce.Do(func() { close(g.ready) })
		}
		var snapshotC chan *request
		if g.pending == nil {
			snapshotC = g.snapshots
		}
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.rn.Tick()
		case result := <-g.writeDone:
			if result.err != nil {
				if g.ctx.Err() == nil {
					g.halt(result.err)
				}
				return
			}
			if err := g.finishWrite(result.kind); err != nil {
				g.halt(err)
				return
			}
		case req := <-g.proposals:
			g.handleProposal(req)
		case req := <-g.inbound:
			if req.ctx.Err() != nil {
				answer(req, Result{}, req.ctx.Err())
				continue
			}
			answer(req, Result{}, g.rn.Step(*req.message))
		case req := <-g.controls:
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
	}
}
func (g *Group) handleProposal(req *request) {
	if err := req.ctx.Err(); err != nil {
		answer(req, Result{}, err)
		return
	}
	if g.rn.BasicStatus().RaftState != raft.StateLeader {
		answer(req, Result{}, fault.ErrNotLeader)
		return
	}
	if g.WALStall() > time.Second || len(g.props) >= g.opts.MaxPendingProposals {
		answer(req, Result{}, fault.ErrOverloaded)
		return
	}
	requestID := req.proposal.RequestID
	if requestID == "" {
		requestID = req.correlation
	}
	data, err := json.Marshal(envelope{Schema: 1, Correlation: req.correlation, RequestID: requestID, Data: req.proposal.Data})
	if err == nil && uint64(len(data)) > g.opts.MaxUncommittedBytes {
		err = fault.ErrOverloaded
	}
	if err == nil {
		err = g.rn.Propose(data)
	}
	if err != nil {
		answer(req, Result{}, err)
		return
	}
	g.props[req.correlation] = req
}
func (g *Group) handleControl(req *request) {
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
		data, err := json.Marshal(envelope{Schema: 1, Correlation: req.correlation, Data: cc.Context})
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
	// Keep its slot until apply/Advance so another change cannot be silently dropped.
}
func (g *Group) beginReady() error {
	rd := g.rn.Ready()
	rd.Entries = copyEntries(rd.Entries)
	rd.CommittedEntries = copyEntries(rd.CommittedEntries)
	messages := rd.Messages
	rd.Messages = make([]raftpb.Message, len(messages))
	for i, m := range messages {
		owned, err := copyMessage(m)
		if err != nil {
			return err
		}
		rd.Messages[i] = owned
	}
	if !raft.IsEmptySnap(rd.Snapshot) {
		rd.Snapshot.Data = bytes.Clone(rd.Snapshot.Data)
		rd.Snapshot.Metadata.ConfState = copyConf(rd.Snapshot.Metadata.ConfState)
		if err := g.ms.ApplySnapshot(rd.Snapshot); err != nil {
			return err
		}
	}
	if err := g.ms.Append(rd.Entries); err != nil {
		return err
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := g.ms.SetHardState(rd.HardState); err != nil {
			return err
		}
	}
	job := writeJob{kind: writeReady, rd: rd}
	g.pending = &job
	if raft.IsEmptySnap(rd.Snapshot) && len(rd.Entries) == 0 && raft.IsEmptyHardState(rd.HardState) {
		return g.finishWrite(writeReady)
	}
	g.writeC <- job
	return nil
}
func (g *Group) finishWrite(kind writeKind) error {
	if g.pending == nil || g.pending.kind != kind {
		return fmt.Errorf("unexpected persistence completion")
	}
	job := g.pending
	if kind == writeSnapshot {
		if err := g.ms.Compact(job.snapshot.Metadata.Index); err != nil {
			return err
		}
		g.snapshotIndex = job.snapshot.Metadata.Index
		g.pending = nil
		answer(job.request, Result{}, nil)
		return nil
	}
	if kind == writeApplied {
		return g.advance()
	}
	rd := job.rd
	if !raft.IsEmptyHardState(rd.HardState) {
		g.durableHS = rd.HardState
	}
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := g.opts.Machine.Restore(g.ctx, bytes.Clone(rd.Snapshot.Data)); err != nil {
			return fault.New(fault.Unreadable, "restore received snapshot", err)
		}
		g.applied, g.snapshotIndex = rd.Snapshot.Metadata.Index, rd.Snapshot.Metadata.Index
		g.cs = copyConf(rd.Snapshot.Metadata.ConfState)
		g.durableHS.Commit = max(g.durableHS.Commit, g.applied)
	}
	for _, entry := range rd.CommittedEntries {
		if entry.Index <= g.applied {
			continue
		}
		if entry.Index != g.applied+1 {
			return fault.New(fault.Unreadable, "committed application replay gap", nil)
		}
		if err := g.apply(entry); err != nil {
			return err
		}
		g.applied = entry.Index
	}
	if len(rd.CommittedEntries) > 0 || !raft.IsEmptySnap(rd.Snapshot) {
		if observer, ok := g.opts.Machine.(AppliedObserver); ok {
			term, err := g.ms.Term(g.applied)
			if err != nil {
				return err
			}
			if err := observer.AfterApply(g.ctx, g.applied, term, copyConf(g.cs)); err != nil {
				return err
			}
		}
		next := writeJob{kind: writeApplied, rd: rd, applied: g.applied, conf: copyConf(g.cs)}
		g.pending = &next
		g.writeC <- next
		return nil
	}
	return g.advance()
}
func (g *Group) apply(entry raftpb.Entry) error {
	switch entry.Type {
	case raftpb.EntryNormal:
		if len(entry.Data) == 0 {
			return nil
		}
		var e envelope
		if err := json.Unmarshal(entry.Data, &e); err != nil || e.Schema != 1 || e.Correlation == "" {
			return fault.New(fault.Unreadable, "invalid load command envelope", err)
		}
		result, err := g.opts.Machine.Apply(g.ctx, Command{Index: entry.Index, Term: entry.Term, RequestID: e.RequestID, Data: bytes.Clone(e.Data)})
		if err != nil {
			return fault.New(fault.Unavailable, "load machine apply failed", err)
		}
		if req := g.props[e.Correlation]; req != nil {
			delete(g.props, e.Correlation)
			answer(req, result, nil)
		}
	case raftpb.EntryConfChangeV2:
		var cc raftpb.ConfChangeV2
		if err := cc.Unmarshal(entry.Data); err != nil {
			return fault.New(fault.Unreadable, "decode membership entry", err)
		}
		if cc.Transition != raftpb.ConfChangeTransitionAuto || len(cc.Changes) != 1 || cc.Changes[0].NodeID == 0 {
			return fault.ErrUnsafeMembership
		}
		g.cs = copyConf(*g.rn.ApplyConfChange(cc))
		g.membershipChanged = true
		var e envelope
		if json.Unmarshal(cc.Context, &e) == nil && e.Schema == 1 {
			if req := g.changes[e.Correlation]; req != nil {
				g.confNotify = append(g.confNotify, req)
				delete(g.changes, e.Correlation)
			}
		}
	default:
		return fault.New(fault.Unsupported, "only V2 membership entries are supported", nil)
	}
	return nil
}
func (g *Group) advance() error {
	rd := g.pending.rd
	if len(rd.Messages) > 0 {
		select {
		case g.sendC <- rd.Messages:
		default:
			for _, m := range rd.Messages {
				g.rn.ReportUnreachable(m.To)
				if m.Type == raftpb.MsgSnap {
					g.rn.ReportSnapshot(m.To, raft.SnapshotFailure)
				}
			}
		}
	}
	g.rn.Advance(rd)
	g.pending = nil
	for _, req := range g.confNotify {
		answer(req, Result{}, nil)
	}
	g.confNotify = nil
	if g.membershipChanged || g.applied-g.snapshotIndex >= g.opts.SnapshotThreshold {
		if err := g.beginSnapshot(nil); err != nil {
			g.opts.Logger.Warn("automatic snapshot failed", "group", g.opts.GroupID, "error", err)
		} else {
			g.membershipChanged = false
		}
	}
	return nil
}
func (g *Group) beginSnapshot(req *request) error {
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
	cs := copyConf(g.cs)
	ss, err := g.ms.CreateSnapshot(g.applied, &cs, bytes.Clone(data))
	if err != nil {
		return err
	}
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
	status.Commit = min(status.Commit, g.durableHS.Commit)
	status.Applied = g.applied
	g.mu.Lock()
	old := g.cached
	g.cached = status
	g.mu.Unlock()
	if old.Lead != status.Lead {
		g.emit(events.Event{Type: events.LeaderChange, OldLeader: fmt.Sprint(old.Lead), NewLeader: fmt.Sprint(status.Lead), Term: status.Term})
	}
	if old.RaftState == raft.StateLeader && status.RaftState != raft.StateLeader {
		for id, req := range g.props {
			answer(req, Result{}, fault.ErrNotLeader)
			delete(g.props, id)
		}
		for id, req := range g.changes {
			answer(req, Result{}, fault.ErrNotLeader)
			delete(g.changes, id)
		}
		if status.Lead == 0 {
			g.emit(events.Event{Type: events.QuorumLost, Term: status.Term, Voters: len(g.cs.Voters)})
		}
	}
	if old.RaftState != raft.StateLeader && status.RaftState == raft.StateLeader {
		g.emit(events.Event{Type: events.QuorumAchieved, Term: status.Term, Voters: len(g.cs.Voters)})
	}
}
func (g *Group) emit(e events.Event) {
	if g.opts.Events != nil {
		e.NodeName = g.opts.NodeName
		e.ClusterID = g.opts.ClusterID
		e.GroupID = g.opts.GroupID
		g.opts.Events.Publish(e)
	}
}
func (g *Group) halt(err error) {
	g.mu.Lock()
	g.err = err
	g.mu.Unlock()
	g.emit(events.Event{Type: events.RuntimeError, Code: "load_halted"})
	g.opts.Logger.Error("load group halted", "group", g.opts.GroupID, "error", err)
}
