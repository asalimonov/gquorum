package load

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/loadhandle"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/membership"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// RaftTransport supplies standalone routing and identity validation. Successful
// snapshot transfer and asynchronous failures must be reported to the owner.
type RaftTransport interface {
	Send(context.Context, []raftpb.Message) error
}

// StartMode selects fresh founding or joining for a standalone replica.
type StartMode string

// Standalone startup modes distinguish initial voters from joining replicas.
const (
	Founding StartMode = "founding"
	Joining  StartMode = "joining"
)

// Standalone is an independent owner, never a member of Node's managed
// registry. Its application supplies membership authority and recovery identity
// lifecycle. Store, StateMachine, Transport and Events remain borrowed and must
// outlive Stop.
type Standalone struct {
	core       *raftengine.Group
	group      *Group
	membership *membership.Coordinator
}

// NewStandalone restores local durable state without starting workers. The
// caller must Stop even if Start is never called, then close borrowed
// resources.
func NewStandalone(opts StandaloneOptions) (*Standalone, error) {
	if opts.StateMachine == nil {
		return nil, fault.New(fault.Invalid, "state machine is required", nil)
	}
	core, err := raftengine.New(raftengine.Options{
		GroupID: opts.Identity.GroupID, NodeName: opts.Identity.NodeName, ClusterID: opts.Identity.ClusterID,
		ReplicaID:  opts.Identity.ReplicaID,
		InstanceID: opts.InstanceID, Mode: raftengine.Mode(opts.Mode), Peers: opts.Peers,
		Timing: raftengine.Timing(opts.Engine.Timing),
		Store:  opts.Store, Machine: machineAdapter{
			opts.StateMachine}, Transport: opts.Transport, Events: opts.Events, Logger: opts.Logger,
		SnapshotThreshold: opts.Engine.SnapshotThreshold, MaxPendingProposals: opts.Engine.MaxPendingProposals,
		MaxWatchers: opts.Engine.MaxWatchers, ProposalTimeout: opts.Engine.ProposalTimeout,
		Reads: raftengine.ReadOptions(
			opts.Engine.Reads),
		MaxUncommittedBytes: opts.Engine.MaxUncommittedBytes, MaxApplyBatchBytes: opts.Engine.MaxApplyBatchBytes,
	})
	if err != nil {
		return nil, err
	}
	return &Standalone{
		core:       core,
		group:      (*Group)(loadhandle.New(core, opts.Identity)),
		membership: membership.NewCoordinator(),
	}, nil
}

// Group returns the stable consensus handle owned by this standalone replica.
func (s *Standalone) Group() *Group {
	if s == nil {
		return nil
	}
	return s.group
}

// Start starts the standalone engine under the supplied lifetime context.
func (s *Standalone) Start(ctx context.Context) error {
	if s == nil || s.core == nil {
		return fault.ErrInvalid
	}
	return s.core.Start(ctx)
}

// Stop requests engine teardown and waits for its owned work to join.
func (s *Standalone) Stop(ctx context.Context) error {
	if s == nil || s.core == nil {
		return fault.ErrInvalid
	}
	return s.core.Stop(ctx)
}

// Step delivers an authenticated Raft message to the standalone engine.
func (s *Standalone) Step(ctx context.Context, msg raftpb.Message) error {
	if s == nil || s.core == nil {
		return fault.ErrInvalid
	}
	return s.core.Step(ctx, msg)
}

// ChangeMembership serializes and safety-checks a membership proposal.
func (s *Standalone) ChangeMembership(ctx context.Context, cc raftpb.ConfChangeV2,
	health membership.HealthSource) error {
	if s == nil || s.core == nil {
		return fault.ErrInvalid
	}
	return s.membership.Apply(ctx, s.core.Membership(), health, cc)
}

// TransferLeader requests leadership transfer to the specified Raft replica.
func (s *Standalone) TransferLeader(ctx context.Context, id uint64) error {
	if s == nil || s.core == nil {
		return fault.ErrInvalid
	}
	return s.core.TransferLeader(ctx, id)
}

// ReportUnreachable reports a transport failure for a destination replica.
func (s *Standalone) ReportUnreachable(id uint64) {
	if s != nil && s.core != nil {
		s.core.ReportUnreachable(id)
	}
}

// ReportSnapshot reports the completed outcome of a snapshot transfer.
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
