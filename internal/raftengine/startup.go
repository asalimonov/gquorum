package raftengine

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/internal/raftlog"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/internal/replicastate"
	"github.com/asalimonov/gquorum/internal/watchstate"
	"github.com/asalimonov/gquorum/metrics"
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
	if err := restoreMachine(ctx, options.Machine, restored.Application, recovery.Snapshot.Metadata); err != nil {
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
	memory    *raft.MemoryStorage
	snapshots *checkpointMemory
	raw       *raft.RawNode
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
	snapshots := &checkpointMemory{MemoryStorage: ms}
	rn, err := raft.NewRawNode(&raft.Config{
		ID:                        options.ReplicaID,
		ElectionTick:              options.Timing.ElectionTicks,
		HeartbeatTick:             options.Timing.HeartbeatTicks,
		Storage:                   snapshots,
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
	return initializedRaft{memory: ms, snapshots: snapshots, raw: rn}, nil
}

// newRestoredCore initializes the same complete, worker-free engine for normal
// startup and offline interpretation. It neither acquires storage nor restores
// an application; the owner has already completed those steps.
func newRestoredCore(ctx context.Context, options Options, state groupRecovery) (*Group, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	recovery, restored := state.recovery, state.restored
	initialized, err := initializeRaft(options, recovery)
	if err != nil {
		return nil, err
	}
	var seed [16]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil, err
	}
	g := &Group{
		opts:               options,
		rn:                 initialized.raw,
		ms:                 initialized.memory,
		checkpointMemory:   initialized.snapshots,
		cs:                 copyConf(recovery.Snapshot.Metadata.ConfState),
		applied:            recovery.Snapshot.Metadata.Index,
		snapshotIndex:      recovery.Snapshot.Metadata.Index,
		recoveryCommit:     recovery.HardState.Commit,
		durableHS:          recovery.HardState,
		done:               make(chan struct{}),
		ready:              make(chan struct{}),
		proposals:          make(chan *request, proposalQueueCapacity),
		inbound:            make(chan *request, inboundQueueCapacity),
		controls:           make(chan *request, controlQueueCapacity),
		snapshots:          make(chan *request, snapshotQueueCapacity),
		snapshotIngress:    make(chan *request, snapshotQueueCapacity),
		slots:              make(chan struct{}, options.MaxPendingProposals),
		unreachable:        make(chan uint64, unreachableQueueCapacity),
		snapshotReports:    make(map[uint64]*snapshotReportState),
		snapshotReportWake: make(chan struct{}, 1),
		sendC:              make(chan []raftpb.Message, outboundBatchQueueCapacity),
		writeC:             make(chan writeJob, 1),
		writeDone:          make(chan writeResult, 1),
		props:              make(map[string]*request),
		changes:            make(map[string]*request),
		nonce:              hex.EncodeToString(seed[:]),
	}
	if !raft.IsEmptySnap(recovery.Snapshot) {
		g.snapshotPublication = 1
	}
	g.observations = watchstate.New(options.MaxWatchers, replicastate.Observation{},
		func(o replicastate.Observation) replicastate.Observation { return o })
	g.leadershipChanged = make(chan struct{})
	g.authority = restored.Authority.Clone()
	g.reads = make(chan *readRequest, options.Reads.MaxPending)
	g.readSlots = make(chan struct{}, options.Reads.MaxPending)
	g.peerAcks = make(map[uint64]peerAck)
	g.meter, err = internalmetrics.NewMeter(metrics.ReplicaIdentity{
		ClusterID:  options.ClusterID,
		GroupID:    options.GroupID,
		NodeName:   options.NodeName,
		ReplicaID:  options.ReplicaID,
		InstanceID: options.InstanceID,
		RunID:      g.nonce,
	}, options.Metrics)
	if err != nil {
		return nil, err
	}
	g.refresh()
	return g, nil
}
