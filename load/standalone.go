package load

import (
	"context"
	"log/slog"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/loadhandle"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// RaftTransport supplies standalone routing and identity validation. Successful
// snapshot transfer and asynchronous failures must be reported to the owner.
type RaftTransport interface {
	Send(context.Context, []raftpb.Message) error
}
type StartMode string

const (
	Founding StartMode = "founding"
	Joining  StartMode = "joining"
)

type StandaloneOptions struct {
	Identity     storage.Identity
	InstanceID   string
	Mode         StartMode
	Peers        []raft.Peer
	Engine       EngineOptions
	Store        storage.Store
	StateMachine StateMachine
	Transport    RaftTransport
	Events       *events.Bus
	Logger       *slog.Logger
}

// Standalone is an independent owner, never a member of Node's managed registry.
// Its application supplies membership authority and recovery identity policy.
// Store, StateMachine, Transport and Events remain borrowed and must outlive Stop.
type Standalone struct {
	core       *raftengine.Group
	group      *Group
	membership *membership.Coordinator
}

// NewStandalone restores local durable state without starting workers. The
// caller must Stop even if Start is never called, then close borrowed resources.
func NewStandalone(opts StandaloneOptions) (*Standalone, error) {
	if opts.StateMachine == nil {
		return nil, fault.New(fault.Invalid, "state machine is required", nil)
	}
	core, err := raftengine.New(raftengine.Options{
		GroupID: opts.Identity.GroupID, NodeName: opts.Identity.NodeName, ClusterID: opts.Identity.ClusterID, ReplicaID: opts.Identity.ReplicaID,
		InstanceID: opts.InstanceID, Mode: raftengine.Mode(opts.Mode), Peers: opts.Peers, Timing: raftengine.Timing(opts.Engine.Timing),
		Store: opts.Store, Machine: machineAdapter{opts.StateMachine}, Transport: opts.Transport, Events: opts.Events, Logger: opts.Logger,
		SnapshotThreshold: opts.Engine.SnapshotThreshold, MaxPendingProposals: opts.Engine.MaxPendingProposals,
		ProposalTimeout: opts.Engine.ProposalTimeout, Reads: raftengine.ReadOptions(opts.Engine.Reads),
		MaxUncommittedBytes: opts.Engine.MaxUncommittedBytes, MaxApplyBatchBytes: opts.Engine.MaxApplyBatchBytes,
	})
	if err != nil {
		return nil, err
	}
	return &Standalone{core: core, group: (*Group)(loadhandle.New(core, opts.Identity)), membership: membership.NewCoordinator()}, nil
}
func (s *Standalone) Group() *Group {
	if s == nil {
		return nil
	}
	return s.group
}
func (s *Standalone) Start(ctx context.Context) error {
	if s == nil || s.core == nil {
		return fault.ErrInvalid
	}
	return s.core.Start(ctx)
}
func (s *Standalone) Stop(ctx context.Context) error {
	if s == nil || s.core == nil {
		return fault.ErrInvalid
	}
	return s.core.Stop(ctx)
}
func (s *Standalone) Step(ctx context.Context, msg raftpb.Message) error {
	if s == nil || s.core == nil {
		return fault.ErrInvalid
	}
	return s.core.Step(ctx, msg)
}
func (s *Standalone) ChangeMembership(ctx context.Context, cc raftpb.ConfChangeV2, health membership.HealthSource) error {
	if s == nil || s.core == nil {
		return fault.ErrInvalid
	}
	return s.membership.Apply(ctx, s.core.Membership(), health, cc)
}
func (s *Standalone) TransferLeader(ctx context.Context, id uint64) error {
	if s == nil || s.core == nil {
		return fault.ErrInvalid
	}
	return s.core.TransferLeader(ctx, id)
}
func (s *Standalone) ReportUnreachable(id uint64) {
	if s != nil && s.core != nil {
		s.core.ReportUnreachable(id)
	}
}
func (s *Standalone) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	if s != nil && s.core != nil {
		s.core.ReportSnapshot(id, status)
	}
}

type machineAdapter struct{ StateMachine }

func (m machineAdapter) Apply(ctx context.Context, c raftengine.Command) (raftengine.Result, error) {
	result, err := m.StateMachine.Apply(ctx, Command(c))
	return raftengine.Result(result), err
}

func (m machineAdapter) SupportsQuery() bool {
	_, ok := m.StateMachine.(QueryMachine)
	return ok
}
func (m machineAdapter) Query(ctx context.Context, query []byte) (raftengine.Result, error) {
	machine, ok := m.StateMachine.(QueryMachine)
	if !ok {
		return raftengine.Result{}, fault.ErrUnsupported
	}
	result, err := machine.Query(ctx, query)
	return raftengine.Result(result), err
}
