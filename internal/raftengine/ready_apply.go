package raftengine

import (
	"bytes"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func ownReady(rd raft.Ready) (raft.Ready, error) {
	rd.Entries = copyEntries(rd.Entries)
	rd.CommittedEntries = copyEntries(rd.CommittedEntries)
	rd.ReadStates = append([]raft.ReadState(nil), rd.ReadStates...)
	for i := range rd.ReadStates {
		rd.ReadStates[i].RequestCtx = bytes.Clone(rd.ReadStates[i].RequestCtx)
	}
	messages := rd.Messages
	rd.Messages = make([]raftpb.Message, len(messages))
	for i, m := range messages {
		owned, err := copyMessage(m)
		if err != nil {
			return rd, err
		}
		rd.Messages[i] = owned
	}
	return rd, nil
}

func (g *Group) stageReadySnapshot(rd *raft.Ready) error {
	if !raft.IsEmptySnap(rd.Snapshot) {
		if _, err := g.decodeSnapshot(rd.Snapshot); err != nil {
			return err
		}
		if g.opts.GroupID != topology.MetaGroupID {
			for id, req := range g.props {
				delete(g.props, id)
				answer(req, Result{},
					fault.New(fault.Unavailable, "snapshot may cover proposal result; outcome unknown", nil))
			}
		}
		rd.Snapshot.Data = bytes.Clone(rd.Snapshot.Data)
		rd.Snapshot.Metadata.ConfState = copyConf(rd.Snapshot.Metadata.ConfState)
		if err := g.ms.ApplySnapshot(rd.Snapshot); err != nil {
			return err
		}
	}
	return nil
}

func (g *Group) restoreReadySnapshot(snapshot raftpb.Snapshot) error {
	if !raft.IsEmptySnap(snapshot) {
		state, err := g.decodeSnapshot(snapshot)
		if err != nil {
			return err
		}
		g.restoring.Store(true)
		g.refresh()
		err = g.opts.Machine.Restore(g.ctx, bytes.Clone(state.Application))
		g.restoring.Store(false)
		if err != nil {
			return fault.New(fault.Unreadable, "restore received snapshot", err)
		}
		g.authority = state.Authority.Clone()
		g.transferIssuedTerm = 0
		g.applied, g.snapshotIndex = snapshot.Metadata.Index, snapshot.Metadata.Index
		g.cs = copyConf(snapshot.Metadata.ConfState)
		g.durableHS.Commit = max(g.durableHS.Commit, g.applied)
	}
	return nil
}

func (g *Group) applyCommittedEntries(entries []raftpb.Entry) error {
	for _, entry := range entries {
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
	return nil
}

func (g *Group) observeAppliedReady(rd raft.Ready) error {
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
		// The writer persists this hint on its own timer; no hand-off is
		// needed.
		g.hint.record(g.applied, g.cs)
	}
	return nil
}

func (g *Group) applyNormalEntry(entry raftpb.Entry) error {
	if len(entry.Data) == 0 {
		return nil
	}
	e, err := raftstate.DecodeEntry(entry.Data)
	if err != nil {
		return err
	}
	if e.Kind != raftstate.Application {
		return g.applyAuthority(entry, e)
	}
	started := time.Now()
	command := Command{Index: entry.Index, Term: entry.Term, RequestID: e.RequestID, Data: bytes.Clone(e.Data)}
	if g.opts.GroupID == topology.MetaGroupID {
		command.ConfState = copyConf(g.cs)
	}
	result, err := g.opts.Machine.Apply(g.ctx, command)
	g.meter.ObserveApply(time.Since(started))
	if err != nil {
		return fault.New(fault.Unavailable, "load machine apply failed", err)
	}
	if req := g.props[e.Correlation]; req != nil {
		delete(g.props, e.Correlation)
		answer(req, result, nil)
	}
	return nil
}

func (g *Group) applyConfigurationEntry(entry raftpb.Entry) error {
	decoded, err := decodeConfiguration(entry)
	cc, e := decoded.Change, decoded.Context
	if err != nil {
		return err
	}
	if e.Kind == raftstate.ManagedConfiguration {
		return g.applyAuthorizedConfChange(entry, cc, e)
	}
	if e.Kind != raftstate.StandaloneConfiguration || g.opts.ManagedAuthority {
		return fault.New(fault.Unsupported, "managed configuration requires authority execution", nil)
	}
	if change := cc.Changes[0]; change.Type == raftpb.ConfChangeRemoveNode && change.NodeID != g.opts.ReplicaID {
		g.queueRetirement(entry, change.NodeID)
	}
	g.cs = copyConf(*g.rn.ApplyConfChange(cc))
	g.membershipChanged = true
	if req := g.changes[e.Correlation]; req != nil {
		g.confNotify = append(g.confNotify, req)
		delete(g.changes, e.Correlation)
	}
	return nil
}
