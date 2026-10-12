package gquorum

import (
	"context"
	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/topology"
)

// MutationOptions controls a CAS edit. Zero ExpectedVersion pins local Latest.
// Preview never proposes and cannot combine with Approve.
type MutationOptions struct {
	ExpectedVersion uint64 `json:"expected_version,omitempty"`
	Actor           string `json:"actor,omitempty"`
	Comment         string `json:"comment,omitempty"`
	Approve         bool   `json:"approve"`
	Preview         bool   `json:"preview"`
}

// MutationResult preserves the staged Document if subsequent approval fails.
type MutationResult struct {
	Document   topology.Document   `json:"document"`
	OperatorID topology.OperatorID `json:"operator_id,omitempty"`
}
type GroupSpec struct {
	ID          string                  `json:"id"`
	Placement   *topology.PlacementRule `json:"placement,omitempty"`
	Nodes       []string                `json:"nodes,omitempty"`
	VoterPolicy *topology.VoterPolicy   `json:"voter_policy,omitempty"`
	Hosting     *topology.HostingRule   `json:"hosting,omitempty"`
}
type RetireOptions struct {
	Reason   string          `json:"reason"`
	Mutation MutationOptions `json:"mutation"`
}

func (o MutationOptions) internal() metaservice.MutationOptions {
	return metaservice.MutationOptions{ExpectedVersion: o.ExpectedVersion, Actor: o.Actor, Comment: o.Comment, Apply: o.Approve, Preview: o.Preview}
}
func (c *clusterView) AddNode(ctx context.Context, node topology.Node, opts MutationOptions) (MutationResult, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return MutationResult{}, err
	}
	result, err := m.AddNode(ctx, node, opts.internal())
	return MutationResult(result), err
}
func (c *clusterView) ReadmitNode(ctx context.Context, name string, opts MutationOptions) (MutationResult, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return MutationResult{}, err
	}
	result, err := m.ReadmitNode(ctx, name, opts.internal())
	return MutationResult(result), err
}
func (c *clusterView) DrainNode(ctx context.Context, name string, opts MutationOptions) (MutationResult, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return MutationResult{}, err
	}
	result, err := m.DrainNode(ctx, name, opts.internal())
	return MutationResult(result), err
}
func (c *clusterView) ActivateNode(ctx context.Context, name string, opts MutationOptions) (MutationResult, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return MutationResult{}, err
	}
	result, err := m.ActivateNode(ctx, name, opts.internal())
	return MutationResult(result), err
}
func (c *clusterView) ExcludeNode(ctx context.Context, name string, opts MutationOptions) (MutationResult, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return MutationResult{}, err
	}
	result, err := m.ExcludeNode(ctx, name, opts.internal())
	return MutationResult(result), err
}
func (c *clusterView) SetPlacement(ctx context.Context, rule topology.PlacementRule, opts MutationOptions) (MutationResult, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return MutationResult{}, err
	}
	result, err := m.SetPlacement(ctx, rule, opts.internal())
	return MutationResult(result), err
}
func (c *clusterView) SetGroupPlacement(ctx context.Context, id string, rule topology.PlacementRule, opts MutationOptions) (MutationResult, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return MutationResult{}, err
	}
	result, err := m.SetGroupPlacement(ctx, id, rule, opts.internal())
	return MutationResult(result), err
}
func (c *clusterView) CreateGroup(ctx context.Context, spec GroupSpec, opts MutationOptions) (MutationResult, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return MutationResult{}, err
	}
	result, err := m.CreateLoadGroup(ctx, metaservice.GroupSpec(spec), opts.internal())
	return MutationResult(result), err
}

// SetGroupVoterPolicy stages replicated voter intent. Approval authorizes later
// scheduler convergence; changing the policy does not itself change any roles.
func (c *clusterView) SetGroupVoterPolicy(ctx context.Context, id string, p topology.VoterPolicy, opts MutationOptions) (MutationResult, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return MutationResult{}, err
	}
	r, err := m.SetGroupVoterPolicy(ctx, id, p, opts.internal())
	return MutationResult(r), err
}

func (c *clusterView) SetGroupHosting(ctx context.Context, id string, h topology.HostingRule, opts MutationOptions) (MutationResult, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return MutationResult{}, err
	}
	r, err := m.SetGroupHosting(ctx, id, h, opts.internal())
	return MutationResult(r), err
}

// SetReplicaPinnedLearner pins an exact replica against automatic promotion.
// Pinning a voter requests guarded demotion; unpinning changes only eligibility.
func (c *clusterView) SetReplicaPinnedLearner(ctx context.Context, id string, replica topology.ReplicaKey, pinned bool, opts MutationOptions) (MutationResult, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return MutationResult{}, err
	}
	r, err := m.SetReplicaPinnedLearner(ctx, id, replica, pinned, opts.internal())
	return MutationResult(r), err
}
func (c *clusterView) DeleteGroup(ctx context.Context, id string, opts MutationOptions) (MutationResult, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return MutationResult{}, err
	}
	result, err := m.DeleteLoadGroup(ctx, id, opts.internal())
	return MutationResult(result), err
}
func (c *clusterView) ChangeReplica(ctx context.Context, id, name string, role topology.ReplicaRole, opts MutationOptions) (MutationResult, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return MutationResult{}, err
	}
	result, err := m.ChangeReplica(ctx, id, name, role, opts.internal())
	return MutationResult(result), err
}
func (c *clusterView) ReseatReplica(ctx context.Context, id, name string, opts MutationOptions) (MutationResult, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return MutationResult{}, err
	}
	result, err := m.ReseatReplica(ctx, id, name, opts.internal())
	return MutationResult(result), err
}
func (c *clusterView) RollbackTo(ctx context.Context, version uint64, opts MutationOptions) (MutationResult, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return MutationResult{}, err
	}
	result, err := m.RollbackTo(ctx, version, opts.internal())
	return MutationResult(result), err
}
func (c *clusterView) ForceRetireNode(ctx context.Context, name string, opts RetireOptions) (MutationResult, error) {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return MutationResult{}, err
	}
	result, err := m.ForceRetireNode(ctx, name, metaservice.RetireOptions{Reason: opts.Reason, Mutation: opts.Mutation.internal()})
	return MutationResult(result), err
}
func (c *clusterView) TransferLeader(ctx context.Context, group, name string) error {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return err
	}
	return m.TransferLeader(ctx, group, name)
}
func (c *clusterView) AcquireLock(ctx context.Context, key, session string) error {
	m, err := c.node.service(ctx, true)
	if err != nil {
		return err
	}
	return m.AcquireLock(ctx, key, session)
}
func (c *clusterView) LockHolder(ctx context.Context, key string) (topology.Session, bool, error) {
	m, err := c.node.service(ctx, false)
	if err != nil {
		return topology.Session{}, false, err
	}
	return m.LockHolder(ctx, key)
}
