package raftengine

import (
	"bytes"
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftlog"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func ownGroupOptions(options Options) Options {
	if options.InitialAuthority != nil {
		grant := options.InitialAuthority.Clone()
		options.InitialAuthority = &grant
	}
	peers := make([]raft.Peer, len(options.Peers))
	for i, p := range options.Peers {
		peers[i] = raft.Peer{ID: p.ID, Context: bytes.Clone(p.Context)}
	}
	options.Peers = peers
	learners := make([]raft.Peer, len(options.FoundingLearners))
	for i, p := range options.FoundingLearners {
		learners[i] = raft.Peer{ID: p.ID, Context: bytes.Clone(p.Context)}
	}
	options.FoundingLearners = learners
	return options
}

type groupRecovery struct {
	recovery storage.Recovery
	restored raftstate.Snapshot
}

func loadGroupRecovery(ctx context.Context, options Options) (groupRecovery, error) {
	id := storage.Identity{NodeName: options.NodeName, ClusterID: options.ClusterID, GroupID: options.GroupID,
		ReplicaID: options.ReplicaID}
	if err := options.Store.BindReplica(ctx, id); err != nil {
		return groupRecovery{}, err
	}
	probe, err := options.Store.Probe(ctx, options.GroupID)
	if err != nil {
		return groupRecovery{}, err
	}
	if probe.Exists && probe.Identity != id {
		return groupRecovery{}, fault.ErrIdentityMismatch
	}
	if err := options.Store.Ensure(ctx, options.GroupID); err != nil {
		return groupRecovery{}, err
	}
	recovery, err := options.Store.Load(ctx, options.GroupID)
	if err != nil {
		return groupRecovery{}, err
	}
	if err := validateRecovery(recovery); err != nil {
		return groupRecovery{}, err
	}
	var restored raftstate.Snapshot
	if !raft.IsEmptySnap(recovery.Snapshot) {
		restored, err = raftstate.DecodeRaftSnapshot(recovery.Snapshot, options.ClusterID, options.GroupID)
		if err != nil {
			return groupRecovery{}, err
		}
	}
	if err := validateAuthorityRecovery(options, recovery, restored); err != nil {
		return groupRecovery{}, err
	}
	if err := options.Machine.Restore(ctx, bytes.Clone(restored.Application)); err != nil {
		return groupRecovery{}, fault.New(fault.Unreadable, "restore load machine", err)
	}
	if err := ctx.Err(); err != nil {
		return groupRecovery{}, err
	}
	return groupRecovery{recovery: recovery, restored: restored}, nil
}

func initializeFounding(ctx context.Context, options Options, state *groupRecovery) error {
	recovery, restored := &state.recovery, &state.restored
	if options.Mode == Founding && len(recovery.Entries) == 0 &&
		raft.IsEmptyHardState(recovery.HardState) && raft.IsEmptySnap(recovery.Snapshot) {
		data, err := options.Machine.Snapshot(ctx)
		if err != nil {
			return err
		}
		if len(data) == 0 {
			return fault.New(fault.Invalid, "empty founding machine snapshot", nil)
		}
		cs := raftpb.ConfState{}
		for _, peer := range options.Peers {
			cs.Voters = append(cs.Voters, peer.ID)
		}
		for _, peer := range options.FoundingLearners {
			cs.Learners = append(cs.Learners, peer.ID)
		}
		slices.Sort(cs.Voters)
		slices.Sort(cs.Learners)
		if options.InitialAuthority != nil {
			restored.Authority = raftstate.AuthorityState{Grant: options.InitialAuthority, InstallIndex: 1,
				InstallTerm: 1}
		}
		data, err = encodeSnapshot(options, data, restored.Authority)
		if err != nil {
			return err
		}
		ss := raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: cs}}
		if err := options.Store.SaveSnapshot(ctx, options.GroupID, ss); err != nil {
			return err
		}
		recovery.Snapshot = ss
		recovery.HardState = raftpb.HardState{Term: 1, Commit: 1}
	}
	return nil
}

type initializedRaft struct {
	memory *raft.MemoryStorage
	raw    *raft.RawNode
}

func initializeRaft(options Options, recovery storage.Recovery) (initializedRaft, error) {
	ms := raft.NewMemoryStorage()
	if !raft.IsEmptySnap(recovery.Snapshot) {
		if err := ms.ApplySnapshot(recovery.Snapshot); err != nil {
			return initializedRaft{}, err
		}
	}
	if err := ms.Append(recovery.Entries); err != nil {
		return initializedRaft{}, err
	}
	if err := ms.SetHardState(recovery.HardState); err != nil {
		return initializedRaft{}, err
	}
	rn, err := raft.NewRawNode(&raft.Config{
		ID:                        options.ReplicaID,
		ElectionTick:              options.Timing.ElectionTicks,
		HeartbeatTick:             options.Timing.HeartbeatTicks,
		Storage:                   ms,
		Applied:                   recovery.Snapshot.Metadata.Index,
		MaxSizePerMsg:             64 << 10,
		MaxInflightMsgs:           256,
		MaxCommittedSizePerReady:  options.MaxApplyBatchBytes,
		MaxUncommittedEntriesSize: options.MaxUncommittedBytes,
		PreVote:                   true,
		CheckQuorum:               true,
		ReadOnlyOption:            raft.ReadOnlySafe,
		Logger:                    raftlog.Logger{Log: options.Logger},
	})
	if err != nil {
		return initializedRaft{}, err
	}
	return initializedRaft{memory: ms, raw: rn}, nil
}
