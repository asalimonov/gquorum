package gquorum

import (
	"context"

	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

// Cluster is a stable view of a Node's topology service. It owns no separate
// connection or lifetime. Latest/Target are owned local observations.
// Explicit Read methods confirm quorum and wait for local application.
type Cluster interface {
	// Quorum-confirmed reads. These wait for local application before
	// returning.

	// ReadBarrier confirms quorum and local application on this participant.
	ReadBarrier(context.Context) error
	// ReadTopology captures the coherent topology view after a safe barrier.
	ReadTopology(context.Context) (topology.ReadView, error)
	// ReadProgress confirms metadata and attaches only matching cached observations.
	ReadProgress(context.Context, topology.ProgressQuery) (topology.ProgressPage, error)
	// ReadAuthority confirms the current durable revocation obligations.
	ReadAuthority(context.Context, topology.OperationID) (topology.AuthorityStatus, error)
	// WaitRevoked waits for a requested revocation to complete at its
	// resources.
	WaitRevoked(context.Context, topology.OperationID) (topology.AuthorityStatus, error)
	// ReadLockHolder reports committed ownership; TTL is configured duration,
	// not remaining lifetime or proof of a currently valid lease.
	ReadLockHolder(context.Context, string) (topology.LockHolder, error)

	// Local configuration, leadership and status observations.

	// Rules returns the effective topology rules for this participant.
	Rules() topology.Rules
	// IsLeader reports this participant's current _meta leadership observation.
	IsLeader() bool
	// Status returns a cluster status snapshot, including placement reports.
	Status(context.Context) (topology.ClusterStatus, error)
	// HealthObservation returns the latest local health snapshot.
	HealthObservation() topology.HealthObservation
	// Metrics returns the latest local cluster metrics snapshot.
	Metrics() metrics.ClusterSnapshot
	// MetricsHistory pages through retained local metrics samples.
	MetricsHistory(metrics.HistoryQuery) (metrics.HistoryPage, error)
	// ParkedJoins reports join requests waiting for operator action.
	ParkedJoins(context.Context) ([]topology.ParkedJoin, error)

	// Document observations. Latest may be unapproved; Target is approved
	// intent. Per-group and per-node convergence is available through progress.

	// Latest returns the most recently staged document.
	Latest(context.Context) (topology.Document, error)
	// Target returns the approved desired document.
	Target(context.Context) (topology.Document, error)
	// Get returns a retained document by version.
	Get(context.Context, uint64) (topology.Document, error)
	// History pages through retained document versions.
	History(context.Context, topology.Page) (topology.HistoryResult, error)
	// Watch streams local updates for one document view.
	Watch(context.Context, topology.View) (<-chan topology.Update, error)

	// Explicit topology workflow. Staging alone authorizes no effects.

	// Stage compares the expected Latest version and records a complete
	// document.
	Stage(context.Context, topology.Document, uint64) (topology.Document, error)
	// Plan previews execution of a staged version.
	Plan(context.Context, uint64) (topology.Plan, error)
	// PlanFor previews execution of an unstaged document.
	PlanFor(context.Context, topology.Document) (topology.Plan, error)
	// Approve commits an executable plan for the current staged version.
	Approve(context.Context, uint64) (topology.OperationID, error)
	// Operation reports progress of an approved plan.
	Operation(context.Context, topology.OperationID) (topology.OperationStatus, error)
	// WaitOperation waits for that plan to reach a terminal state.
	WaitOperation(context.Context, topology.OperationID) (topology.OperationStatus, error)
	// RetryOperation retries the exact failed attempt's immutable plan.
	RetryOperation(context.Context, topology.OperationID) (topology.OperationID, error)

	// Node membership and lifecycle edits. Each uses MutationOptions for CAS,
	// preview and optional approval.

	// AddNode stages a new node and its _meta admission.
	AddNode(context.Context, topology.Node, MutationOptions) (MutationResult, error)
	// ReadmitNode replaces a node whose current member ID matches the nonzero predecessor.
	ReadmitNode(context.Context, string, uint64, MutationOptions) (MutationResult, error)
	// DrainNode moves load replicas off a node through approved topology.
	DrainNode(context.Context, string, MutationOptions) (MutationResult, error)
	// ActivateNode returns a drained node to active placement.
	ActivateNode(context.Context, string, MutationOptions) (MutationResult, error)
	// ExcludeNode stages removal of a node from cluster participation.
	ExcludeNode(context.Context, string, MutationOptions) (MutationResult, error)
	// ForceRetireNode stages explicitly justified retirement and cleanup.
	ForceRetireNode(context.Context, string, RetireOptions) (MutationResult, error)

	// Placement, voting and group edits. Group labels are edited through
	// complete documents; placement rules explicitly select eligible hosts.

	// SetPlacement changes the document-level load placement rule.
	SetPlacement(context.Context, topology.PlacementRule, MutationOptions) (MutationResult, error)
	// SetGroupPlacement sets a complete rule override for one load group.
	SetGroupPlacement(context.Context, string, topology.PlacementRule, MutationOptions) (MutationResult, error)
	// SetGroupVoterConfig changes a group's target voter configuration.
	SetGroupVoterConfig(context.Context, string, topology.VoterConfig, MutationOptions) (MutationResult, error)
	// SetGroupHosting changes a group's explicit hosting declaration.
	SetGroupHosting(context.Context, string, topology.HostingRule, MutationOptions) (MutationResult, error)
	// SetReplicaPinnedLearner controls promotion eligibility of an exact
	// replica.
	SetReplicaPinnedLearner(context.Context, string, topology.LearnerPin, MutationOptions) (MutationResult, error)
	// CreateGroup stages a new managed load group.
	CreateGroup(context.Context, GroupSpec, MutationOptions) (MutationResult, error)
	// EnsureGroup idempotently creates and approves a group until converged.
	// A different existing group or foreign staged intent returns ErrConflict.
	EnsureGroup(context.Context, GroupSpec, EnsureOptions) (topology.Group, error)
	// DeleteGroup stages removal of a managed load group.
	DeleteGroup(context.Context, string, MutationOptions) (MutationResult, error)
	// ChangeReplica changes a named replica's desired voting role.
	ChangeReplica(context.Context, string, topology.ReplicaRoleChange, MutationOptions) (MutationResult, error)
	// ReseatReplica replaces the exact nonzero replica key with a fresh identity.
	ReseatReplica(context.Context, string, topology.ReplicaKey, MutationOptions) (MutationResult, error)
	// RollbackTo stages an earlier document as new desired topology.
	RollbackTo(context.Context, uint64, MutationOptions) (MutationResult, error)

	// Separate leadership and lease operations.

	// TransferLeader requests leadership transfer to a named node in a group.
	TransferLeader(context.Context, string, string) error
	// AcquireLock binds a lock key to a live session.
	AcquireLock(context.Context, string, string) error
	// LockHolder returns the locally observed lock holder.
	LockHolder(context.Context, string) (topology.LockHolder, error)
}
type clusterView struct{ node *node }

var _ Cluster = (*clusterView)(nil)

func (c *clusterView) ReadProgress(ctx context.Context, query topology.ProgressQuery) (topology.ProgressPage, error) {
	if err := query.Validate(); err != nil {
		return topology.ProgressPage{}, err
	}
	service, err := c.node.service(ctx, workService)
	if err != nil {
		return topology.ProgressPage{}, err
	}
	return service.ReadProgress(ctx, query)
}

func (c *clusterView) ReadBarrier(ctx context.Context) error {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return err
	}
	return m.ReadBarrier(ctx)
}

func (c *clusterView) ReadTopology(ctx context.Context) (topology.ReadView, error) {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return topology.ReadView{}, err
	}
	return m.ReadTopology(ctx)
}

func (c *clusterView) ReadLockHolder(ctx context.Context, key string) (topology.LockHolder, error) {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return topology.LockHolder{}, err
	}
	return m.ReadLockHolder(ctx, key)
}

func (c *clusterView) ReadAuthority(ctx context.Context, id topology.OperationID) (topology.AuthorityStatus, error) {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return topology.AuthorityStatus{}, err
	}
	return m.ReadAuthority(ctx, topology.OperationID(id))
}

func (c *clusterView) WaitRevoked(ctx context.Context, id topology.OperationID) (topology.AuthorityStatus, error) {
	m, err := c.node.service(ctx, workService)
	if err != nil {
		return topology.AuthorityStatus{}, err
	}
	return m.WaitRevoked(ctx, topology.OperationID(id))
}

func (c *clusterView) Rules() topology.Rules {
	return topology.Rules{SchemaVersion: 1, Limits: c.node.opts.Limits}
}
func (c *clusterView) IsLeader() bool {
	m, err := c.node.service(context.Background(), workService)
	return err == nil && m.IsLeader()
}
func (c *clusterView) HealthObservation() topology.HealthObservation {
	m, err := c.node.service(context.Background(), readService)
	if err != nil {
		return topology.HealthObservation{}
	}
	return m.HealthObservation()
}

func (c *clusterView) Status(parent context.Context) (topology.ClusterStatus, error) {
	m, err := c.node.service(parent, readService)
	if err != nil {
		return topology.ClusterStatus{}, err
	}
	n := c.node
	n.mu.RLock()
	cached := n.stopping || n.fenced
	n.mu.RUnlock()
	if cached {
		return m.CachedStatus(parent)
	}
	timeout := n.opts.Loads.EffectTimeout
	if timeout == 0 {
		timeout = defaultEffectTimeout
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return m.Status(ctx)
}

// RetireOptions supplies the retirement reason and topology mutation controls.
type RetireOptions struct {
	Reason   string          `json:"reason"`
	Mutation MutationOptions `json:"mutation"`
}
