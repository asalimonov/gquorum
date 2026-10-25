package raftengine

import (
	"context"
	"io"
	"log/slog"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// ReplayOptions binds committed replay to one exact retained managed replica.
type ReplayOptions struct {
	Identity   storage.Identity
	MemberID   uint64
	InstanceID string
}

// ReplayResult contains actual interpreted positions, native membership and authority.
type ReplayResult struct {
	Index, Term uint64
	ConfState   raftpb.ConfState
	Authority   raftstate.AuthorityState
}

// Replay interprets the complete snapshot and entries through retained commit.
// It starts no workers, elections or transport and writes no persistence store.
func Replay(ctx context.Context, input ReplayOptions, retained storage.Recovery,
	machine Machine) (ReplayResult, error) {
	if err := ctx.Err(); err != nil {
		return ReplayResult{}, err
	}
	if err := validateReplayOptions(input, machine); err != nil {
		return ReplayResult{}, err
	}
	options := Options{NodeName: input.Identity.NodeName, ClusterID: input.Identity.ClusterID,
		GroupID:   input.Identity.GroupID,
		ReplicaID: input.Identity.ReplicaID, MemberID: input.MemberID, InstanceID: input.InstanceID,
		ManagedAuthority: true, Mode: Joining, Machine: machine,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}.defaults()
	state, err := restoreOffline(ctx, options, retained)
	if err != nil {
		return ReplayResult{}, err
	}
	g, err := newRestoredCore(ctx, options, state)
	if err != nil {
		return ReplayResult{}, err
	}
	g.ctx = ctx
	defer g.meter.Stop()
	return replayOffline(ctx, g, retained)
}
func replayOffline(ctx context.Context, g *Group, retained storage.Recovery) (ReplayResult, error) {
	machine := g.opts.Machine
	for _, entry := range retained.Entries {
		if entry.Index > retained.HardState.Commit {
			break
		}
		if err := ctx.Err(); err != nil {
			return ReplayResult{}, err
		}
		if err := g.applyCommittedEntries([]raftpb.Entry{entry}); err != nil {
			return ReplayResult{}, err
		}
	}
	if g.applied != retained.HardState.Commit {
		return ReplayResult{}, fault.ErrUnreadable
	}
	term, err := g.ms.Term(g.applied)
	if err != nil {
		return ReplayResult{}, err
	}
	if observer, ok := machine.(AppliedObserver); ok {
		if err := observer.AfterApply(ctx, g.applied, term, copyConf(g.cs)); err != nil {
			return ReplayResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return ReplayResult{}, err
	}
	return ReplayResult{Index: g.applied, Term: term, ConfState: copyConf(g.cs),
		Authority: g.authority.Clone()}, nil
}
func validateReplayOptions(input ReplayOptions, machine Machine) error {
	id := input.Identity
	if machine == nil || id.ReplicaID == 0 || input.MemberID == 0 || input.InstanceID == "" || id.ClusterID == "" ||
		topology.ValidateID(id.GroupID) != nil || topology.ValidateID(id.NodeName) != nil {
		return fault.ErrInvalid
	}
	if id.GroupID == topology.MetaGroupID && id.ReplicaID != input.MemberID {
		return fault.ErrIdentityMismatch
	}
	return nil
}

func restoreOffline(ctx context.Context, options Options, retained storage.Recovery) (groupRecovery, error) {
	machine := options.Machine
	if err := validateRecovery(retained); err != nil {
		return groupRecovery{}, err
	}
	if retained.Snapshot.Metadata.Index == 0 {
		return groupRecovery{}, fault.New(fault.Unsupported, "offline recovery requires a complete snapshot", nil)
	}

	restored, err := raftstate.DecodeRaftSnapshot(retained.Snapshot, options.ClusterID, options.GroupID)
	if err != nil {
		return groupRecovery{}, err
	}
	if err := validateAuthorityRecovery(options, retained, restored); err != nil {
		return groupRecovery{}, err
	}
	if err := restoreMachine(ctx, machine, restored.Application, retained.Snapshot.Metadata); err != nil {
		return groupRecovery{}, fault.New(fault.Unreadable, "offline snapshot restore failed", err)
	}
	return groupRecovery{recovery: retained, restored: restored}, nil
}
