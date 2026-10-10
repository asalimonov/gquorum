// Package raftengine implements the shared persistence-ordered Raft runtime.
package raftengine

import (
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type Command struct {
	Index, Term uint64
	RequestID   string
	Data        []byte
}
type Result struct {
	Code string
	Data []byte
}

// Machine operations are serialized. Apply failures halt the replica; business
// rejection should return a deterministic Result.Code. Restore(nil) initializes
// empty state. Snapshot must return a complete, nonempty representation.
type Machine interface {
	Apply(context.Context, Command) (Result, error)
	Snapshot(context.Context) ([]byte, error)
	Restore(context.Context, []byte) error
}

// AppliedObserver is an internal integration seam for materialized meta state.
// The engine invokes it after the committed batch and before Advance/messages.
type AppliedObserver interface {
	AfterApply(context.Context, uint64, uint64, raftpb.ConfState) error
}

// Transport owns routing and identity validation. Send receives owned copies;
// it must honor cancellation. Asynchronous transports report later failures
// through ReportUnreachable and ReportSnapshot.
type Transport interface {
	Send(context.Context, []raftpb.Message) error
}
type Mode string

const (
	Founding Mode = "founding"
	Joining  Mode = "joining"
)

type Timing struct {
	TickInterval                  time.Duration
	ElectionTicks, HeartbeatTicks int
	WALWriteWait                  time.Duration
}
type Options struct {
	GroupID, NodeName, ClusterID            string
	ReplicaID                               uint64
	Mode                                    Mode
	Peers                                   []raft.Peer
	Timing                                  Timing
	Store                                   storage.Store
	Machine                                 Machine
	Transport                               Transport
	Events                                  *events.Bus
	Logger                                  *slog.Logger
	SnapshotThreshold                       uint64
	MaxPendingProposals                     int
	MaxUncommittedBytes, MaxApplyBatchBytes uint64
}
type Proposal struct {
	RequestID string
	Data      []byte
}

func (o Options) Validate() error { return o.validate(false) }
func (o Options) validate(meta bool) error {
	if meta {
		if o.GroupID != topology.MetaGroupID {
			return fault.New(fault.Invalid, "meta engine requires reserved group ID", nil)
		}
	} else if err := topology.ValidateLoadID(o.GroupID); err != nil {
		return err
	}
	if o.NodeName == "" || o.ClusterID == "" || o.ReplicaID == 0 || o.Store == nil || o.Machine == nil || o.Transport == nil {
		return fault.New(fault.Invalid, "load identity, store, machine and transport are required", nil)
	}
	if o.Mode != Founding && o.Mode != Joining {
		return fault.New(fault.Invalid, "explicit founding or joining mode required", nil)
	}
	if o.Timing.TickInterval < 0 || o.Timing.ElectionTicks < 0 || o.Timing.HeartbeatTicks < 0 || o.Timing.WALWriteWait < 0 || o.MaxPendingProposals < 0 {
		return fault.New(fault.Invalid, "negative load timing or proposal bound", nil)
	}
	o = o.defaults()
	if o.Timing.TickInterval > 0 && int64(o.Timing.ElectionTicks) > math.MaxInt64/int64(o.Timing.TickInterval) {
		return fault.New(fault.Invalid, "Raft election interval overflows duration", nil)
	}
	if o.Timing.TickInterval < 10*time.Millisecond || o.Timing.HeartbeatTicks < 1 || o.Timing.ElectionTicks < 2 || o.Timing.HeartbeatTicks >= o.Timing.ElectionTicks || o.Timing.WALWriteWait > 10*time.Millisecond || o.Timing.WALWriteWait > 0 && o.Timing.WALWriteWait >= o.Timing.TickInterval*time.Duration(o.Timing.ElectionTicks)/2 {
		return fault.New(fault.Invalid, "invalid Raft timing", nil)
	}
	ids := make(map[uint64]bool)
	local := false
	for _, peer := range o.Peers {
		if peer.ID == 0 || ids[peer.ID] {
			return fault.New(fault.Invalid, "zero or duplicate founding voter", nil)
		}
		ids[peer.ID] = true
		local = local || peer.ID == o.ReplicaID
	}
	if o.Mode == Founding && !local {
		return fault.New(fault.Invalid, "founding peers must contain the local replica", nil)
	}
	if o.Mode == Joining && len(o.Peers) > 0 {
		return fault.New(fault.Invalid, "joining cannot supply founding voters", nil)
	}
	return nil
}
func (o Options) defaults() Options {
	if o.Timing.TickInterval == 0 {
		o.Timing.TickInterval = 100 * time.Millisecond
	}
	if o.Timing.ElectionTicks == 0 {
		o.Timing.ElectionTicks = 10
	}
	if o.Timing.HeartbeatTicks == 0 {
		o.Timing.HeartbeatTicks = 1
	}
	if o.MaxPendingProposals == 0 {
		o.MaxPendingProposals = 256
	}
	if o.MaxUncommittedBytes == 0 {
		o.MaxUncommittedBytes = 64 << 20
	}
	if o.MaxApplyBatchBytes == 0 {
		o.MaxApplyBatchBytes = 1 << 20
	}
	if o.SnapshotThreshold == 0 {
		o.SnapshotThreshold = 10000
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}
