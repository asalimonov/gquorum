package gquorum

import (
	"context"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

// Cluster is a stable view of a Node's topology service. It owns no separate
// connection or lifetime. Latest/Target/Active are owned local observations.
// Explicit Read methods confirm quorum and wait for local application.
type Cluster interface {
	// ReadBarrier confirms quorum and local application on this participant.
	ReadBarrier(context.Context) error
	// ReadTopology captures all three documents atomically after a safe barrier.
	ReadTopology(context.Context) (topology.ReadView, error)
	// ReadAuthority confirms the current durable revocation obligations.
	ReadAuthority(context.Context, topology.OperatorID) (topology.AuthorityStatus, error)
	// WaitRevoked waits for a requested revocation to complete at its resources.
	WaitRevoked(context.Context, topology.OperatorID) (topology.AuthorityStatus, error)
	// ReadLockHolder reports committed ownership; TTL is configured duration,
	// not remaining lifetime or proof of a currently valid lease.
	ReadLockHolder(context.Context, string) (topology.Session, bool, error)
	Rules() topology.Rules
	IsLeader() bool
	Status(context.Context) (topology.ClusterStatus, error)
	HealthObservation() topology.HealthObservation
	Metrics() metrics.ClusterSnapshot
	MetricsHistory(metrics.HistoryQuery) (metrics.HistoryPage, error)
	ParkedJoins(context.Context) ([]topology.ParkedJoin, error)
	Latest(context.Context) (topology.Document, error)
	Target(context.Context) (topology.Document, error)
	Active(context.Context) (topology.Document, error)
	Get(context.Context, uint64) (topology.Document, error)
	History(context.Context, topology.Page) (topology.HistoryResult, error)
	Watch(context.Context, topology.View) (<-chan topology.Update, error)
	Stage(context.Context, topology.Document, uint64) (topology.Document, error)
	Plan(context.Context, uint64) (topology.Plan, error)
	PlanFor(context.Context, topology.Document) (topology.Plan, error)
	Approve(context.Context, uint64) (topology.OperatorID, error)
	Operator(context.Context, topology.OperatorID) (topology.OperatorStatus, error)
	WaitOperator(context.Context, topology.OperatorID) (topology.OperatorStatus, error)
	AddNode(context.Context, topology.Node, MutationOptions) (MutationResult, error)
	ReadmitNode(context.Context, string, MutationOptions) (MutationResult, error)
	DrainNode(context.Context, string, MutationOptions) (MutationResult, error)
	ActivateNode(context.Context, string, MutationOptions) (MutationResult, error)
	ExcludeNode(context.Context, string, MutationOptions) (MutationResult, error)
	ForceRetireNode(context.Context, string, RetireOptions) (MutationResult, error)
	SetPlacement(context.Context, topology.PlacementRule, MutationOptions) (MutationResult, error)
	SetGroupPlacement(context.Context, string, topology.PlacementRule, MutationOptions) (MutationResult, error)
	SetGroupVoterPolicy(context.Context, string, topology.VoterPolicy, MutationOptions) (MutationResult, error)
	SetGroupHosting(context.Context, string, topology.HostingRule, MutationOptions) (MutationResult, error)
	SetReplicaPinnedLearner(context.Context, string, topology.ReplicaKey, bool, MutationOptions) (MutationResult, error)
	CreateGroup(context.Context, GroupSpec, MutationOptions) (MutationResult, error)
	DeleteGroup(context.Context, string, MutationOptions) (MutationResult, error)
	ChangeReplica(context.Context, string, string, topology.ReplicaRole, MutationOptions) (MutationResult, error)
	ReseatReplica(context.Context, string, string, MutationOptions) (MutationResult, error)
	RollbackTo(context.Context, uint64, MutationOptions) (MutationResult, error)
	TransferLeader(context.Context, string, string) error
	AcquireLock(context.Context, string, string) error
	LockHolder(context.Context, string) (topology.Session, bool, error)
}
type clusterView struct{ node *node }

var _ Cluster = (*clusterView)(nil)
