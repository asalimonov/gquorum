// Package raftengine implements the shared persistence-ordered Raft runtime.
package raftengine

import (
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
)

const (
	minimumTickInterval        = 10 * time.Millisecond
	maximumWALWriteWait        = 10 * time.Millisecond
	defaultMaxWatchers         = 128
	defaultTickInterval        = 100 * time.Millisecond
	defaultElectionTicks       = 10
	defaultMaxPendingProposals = 256
	defaultProposalTimeout     = 30 * time.Second
	defaultMaxUncommittedBytes = 64 << 20
	defaultMaxApplyBatchBytes  = 1 << 20
	defaultSnapshotThreshold   = 10000
)

// Mode selects fresh founding or joining startup for a replica.
type Mode string

type engineMode uint8

const (
	loadEngine engineMode = iota
	metaEngine
)

// Startup modes distinguish founding voters from a joining replica.
const (
	Founding Mode = "founding"
	Joining  Mode = "joining"
)

// Timing configures Raft ticks and persistence stall waits.
type Timing struct {
	TickInterval                  time.Duration
	ElectionTicks, HeartbeatTicks int
	WALWriteWait                  time.Duration
}

// Options supplies exact identity, initial membership, bounded engine settings
// and borrowed dependencies.
type Options struct {
	SnapshotPolicy SnapshotPolicy
	// MaxWatchers bounds local current-state subscriptions; zero selects 128.
	MaxWatchers                  int
	GroupID, NodeName, ClusterID string
	// InstanceID identifies the observer process in local events, when
	// supplied.
	InstanceID string
	Metrics    internalmetrics.Options
	ReplicaID  uint64
	// ManagedAuthority requires grants for administrative effects. MemberID
	// binds
	// group evidence to the host's durable metadata identity.
	ManagedAuthority bool
	MemberID         uint64
	InitialAuthority *topology.AuthorityGrant
	Mode             Mode
	Peers            []raft.Peer
	// FoundingLearners are seeded alongside Peers, which remain the voters.
	// Only fresh founding uses these sets; retained recovery uses durable
	// state.
	FoundingLearners                        []raft.Peer
	Timing                                  Timing
	Reads                                   ReadOptions
	Store                                   storage.Store
	Machine                                 Machine
	Transport                               Transport
	Events                                  *events.Bus
	Logger                                  *slog.Logger
	SnapshotThreshold                       uint64
	MaxPendingProposals                     int
	ProposalTimeout                         time.Duration
	MaxUncommittedBytes, MaxApplyBatchBytes uint64
	// PeerName optionally resolves a replica ID to its node name for leader
	// progress observations. It must honor cancellation and never block on
	// this engine.
	PeerName func(context.Context, uint64) (string, error)
}

// Validate checks load identity, dependencies, authority and engine settings.
func (o Options) Validate() error { return o.validateMode(loadEngine) }
func (o Options) validateMode(mode engineMode) error {
	if mode == metaEngine {
		if o.GroupID != topology.MetaGroupID {
			return fault.New(fault.Invalid, "meta engine requires reserved group ID", nil)
		}
	} else if err := topology.ValidateLoadID(o.GroupID); err != nil {
		return err
	}
	if o.NodeName == "" || o.ClusterID == "" || o.ReplicaID == 0 || o.Store == nil || o.Machine == nil ||
		o.Transport == nil {
		return fault.New(fault.Invalid, "load identity, store, machine and transport are required", nil)
	}
	if err := validateAuthorityOptions(o); err != nil {
		return err
	}
	if o.Mode != Founding && o.Mode != Joining {
		return fault.New(fault.Invalid, "explicit founding or joining mode required", nil)
	}
	if err := o.ValidateSettings(); err != nil {
		return err
	}
	return o.validateFoundingMembers()
}

func (o Options) validateFoundingMembers() error {
	ids := make(map[uint64]bool)
	local, err := validateFoundingPeers(o.Peers, ids, o.ReplicaID, "zero or duplicate founding voter")
	if err != nil {
		return err
	}
	learner, err := validateFoundingPeers(o.FoundingLearners, ids, o.ReplicaID,
		"zero, duplicate or voting founding learner")
	if err != nil {
		return err
	}
	local = local || learner
	if o.Mode == Founding && len(o.Peers) == 0 {
		return fault.New(fault.Invalid, "founding requires at least one voter", nil)
	}
	if o.Mode == Founding && !local {
		return fault.New(fault.Invalid, "founding peers must contain the local replica", nil)
	}
	if o.Mode == Joining && (len(o.Peers) > 0 || len(o.FoundingLearners) > 0) {
		return fault.New(fault.Invalid, "joining cannot supply founding members", nil)
	}
	return nil
}

// ValidateSettings checks tuning without requiring an open store or a machine.
// Node uses this same validation before acquiring any per-replica resources.
func (o Options) ValidateSettings() error {
	if err := o.SnapshotPolicy.validate(); err != nil {
		return err
	}
	if err := o.Reads.Validate(); err != nil {
		return err
	}
	if _, err := o.Metrics.Resolve(); err != nil {
		return err
	}
	if o.Timing.TickInterval < 0 || o.Timing.ElectionTicks < 0 || o.Timing.HeartbeatTicks < 0 ||
		o.Timing.WALWriteWait < 0 || o.MaxWatchers < 0 || o.MaxPendingProposals < 0 || o.ProposalTimeout < 0 {
		return fault.New(fault.Invalid, "negative load timing or proposal bound", nil)
	}
	resolved := o.defaults()
	if int64(resolved.Timing.ElectionTicks) > math.MaxInt64/int64(resolved.Timing.TickInterval) {
		return fault.New(fault.Invalid, "Raft election interval overflows duration", nil)
	}
	if resolved.Timing.TickInterval < minimumTickInterval || resolved.Timing.HeartbeatTicks < 1 ||
		resolved.Timing.ElectionTicks < 2 || resolved.Timing.HeartbeatTicks >= resolved.Timing.ElectionTicks ||
		resolved.Timing.WALWriteWait > maximumWALWriteWait || resolved.Timing.WALWriteWait > 0 &&
		resolved.Timing.WALWriteWait >= resolved.Timing.TickInterval*time.Duration(resolved.Timing.ElectionTicks)/2 {
		return fault.New(fault.Invalid, "invalid Raft timing", nil)
	}
	return nil
}
func (o Options) defaults() Options {
	if o.MaxWatchers == 0 {
		o.MaxWatchers = defaultMaxWatchers
	}
	o.Reads = o.Reads.withDefaults()
	o.Metrics, _ = o.Metrics.Resolve()
	if o.Timing.TickInterval == 0 {
		o.Timing.TickInterval = defaultTickInterval
	}
	if o.Timing.ElectionTicks == 0 {
		o.Timing.ElectionTicks = defaultElectionTicks
	}
	if o.Timing.HeartbeatTicks == 0 {
		o.Timing.HeartbeatTicks = 1
	}
	if o.MaxPendingProposals == 0 {
		o.MaxPendingProposals = defaultMaxPendingProposals
	}
	if o.ProposalTimeout == 0 {
		o.ProposalTimeout = defaultProposalTimeout
	}
	if o.MaxUncommittedBytes == 0 {
		o.MaxUncommittedBytes = defaultMaxUncommittedBytes
	}
	if o.MaxApplyBatchBytes == 0 {
		o.MaxApplyBatchBytes = defaultMaxApplyBatchBytes
	}
	if o.SnapshotThreshold == 0 {
		o.SnapshotThreshold = defaultSnapshotThreshold
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

func validateFoundingPeers(peers []raft.Peer, ids map[uint64]bool, localID uint64, message string) (bool, error) {
	local := false
	for _, peer := range peers {
		if peer.ID == 0 || ids[peer.ID] {
			return false, fault.New(fault.Invalid, message, nil)
		}
		ids[peer.ID] = true
		local = local || peer.ID == localID
	}
	return local, nil
}
