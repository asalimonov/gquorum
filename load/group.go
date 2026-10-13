package load

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/loadhandle"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Group is a stable view of one exact replica. It never redirects into a
// successor. Obtain it from Node or Standalone; its zero value cannot operate.
// Lifecycle, transport ingress and membership belong to the owning node.
type Group loadhandle.Handle

func (g *Group) Identity() storage.Identity { return (*loadhandle.Handle)(g).Identity() }
func (g *Group) GroupID() string            { return g.Identity().GroupID }
func (g *Group) ReplicaID() uint64          { return g.Identity().ReplicaID }
func (g *Group) core() *raftengine.Group    { return (*loadhandle.Handle)(g).Core() }

// Propose cancellation ends the caller's wait; it cannot roll back a command
// already committed by Raft. Preserve the request's application identity on retry.
func (g *Group) Propose(ctx context.Context, p Proposal) (Result, error) {
	core := g.core()
	if core == nil {
		return Result{}, fault.ErrInvalid
	}
	result, err := core.Propose(ctx, raftengine.Proposal(p))
	return Result(result), err
}

// ReadBarrier confirms a quorum after invocation and waits for local application.
// It does not serialize subsequent caller access to the state machine; use Read
// for a query serialized with Apply and Restore.
func (g *Group) ReadBarrier(ctx context.Context) error {
	if core := g.core(); core != nil {
		return core.ReadBarrier(ctx)
	}
	return fault.ErrInvalid
}

// Read runs optional Query after a safe quorum read and local application, on
// any ready local replica, including followers and learners. Query errors do
// not halt the replica. Cancellation ends the wait but retains callback ownership.
func (g *Group) Read(ctx context.Context, query []byte) (Result, error) {
	if core := g.core(); core != nil {
		r, err := core.Read(ctx, query)
		return Result(r), err
	}
	return Result{}, fault.ErrInvalid
}
func (g *Group) WaitReady(ctx context.Context) error {
	core := g.core()
	if core == nil {
		return fault.ErrInvalid
	}
	return core.WaitReady(ctx)
}
func (g *Group) IsLeader() bool { core := g.core(); return core != nil && core.IsLeader() }

// Status returns a local Raft observation. LeaderID is a numeric replica ID;
// LeaderNodeName is empty because this handle does not resolve topology names.
// Use Cluster.Status for group observations with resolved node names.
func (g *Group) Status() topology.GroupStatus {
	core := g.core()
	if core == nil {
		return topology.GroupStatus{ErrorCode: string(fault.Invalid)}
	}
	return core.Status()
}
func (g *Group) ProposalStats() ProposalStats {
	core := g.core()
	if core == nil {
		return ProposalStats{}
	}
	return ProposalStats(core.ProposalStats())
}
func (g *Group) WALStall() time.Duration {
	core := g.core()
	if core == nil {
		return 0
	}
	return core.WALStall()
}
func (g *Group) Snapshot(ctx context.Context) error {
	core := g.core()
	if core == nil {
		return fault.ErrInvalid
	}
	return core.Snapshot(ctx)
}
func (g *Group) Error() error {
	core := g.core()
	if core == nil {
		return fault.ErrInvalid
	}
	return core.Error()
}
func (g *Group) Running() bool { core := g.core(); return core != nil && core.Running() }

// Metrics is an owned local observation, with explicit source ages. Reading it
// performs no I/O and cannot turn cached samples into fresh evidence.
func (g *Group) Metrics() metrics.ReplicaObservation {
	if core := g.core(); core != nil {
		return core.Metrics()
	}
	return (metrics.ReplicaObservation{State: metrics.Unknown, Reachability: "unknown"}).Clone()
}

// MetricsHistory reads bounded memory belonging to this exact engine run. A
// stopped handle never redirects into a successor's history.
func (g *Group) MetricsHistory(q metrics.HistoryQuery) (metrics.HistoryPage, error) {
	if core := g.core(); core != nil {
		return core.MetricsHistory(q)
	}
	return metrics.HistoryPage{}, fault.ErrInvalid
}
