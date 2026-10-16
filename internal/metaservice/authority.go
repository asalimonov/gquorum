package metaservice

import (
	"context"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

// BeginEffect requests immutable replicated authority for an exact approved
// step.
func (g *Group) BeginEffect(ctx context.Context, action topology.AuthorizedStep) (topology.AuthorityGrant, error) {
	result, err := g.propose(ctx, metaengine.BeginEffectProposal{Action: action})
	if err != nil {
		return topology.AuthorityGrant{}, err
	}
	if result.Grant == nil || result.Grant.Action == nil || *result.Grant.Action != action ||
		result.Grant.Validate() != nil {
		return topology.AuthorityGrant{}, fault.ErrUnreadable
	}
	// Election can precede activation after a retained restart. Wait until the
	// replicated routing identity names this process before sending effects:
	// receivers must reject a courier still bound to the former instance. The
	// grant remains durable and the same operator step can safely retry it.
	id := g.Identity()
	proof, err := g.readAuthorityProof(ctx, result.Grant.Key, id.NodeName)
	if err != nil {
		return topology.AuthorityGrant{}, err
	}
	if proof.Grant == nil || !proof.Grant.Equal(*result.Grant) {
		return topology.AuthorityGrant{}, fault.ErrOperatorAuthorization
	}
	if proof.Courier != id {
		return topology.AuthorityGrant{}, fault.ErrReplicaNotReady
	}
	return result.Grant.Clone(), nil
}

// ValidateGrant accepts retained committed records, including closed grants.
// Resource owners decide whether execution is still allowed at their installed
// fence. A local miss takes a safe read; missing/future grants never authorize.
func (g *Group) ValidateGrant(parent context.Context, grant topology.AuthorityGrant) error {
	if err := grant.Validate(); err != nil {
		return err
	}
	ctxHandle, err := g.beginRPC(parent)
	ctx, release := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return err
	}
	defer release()
	if cached, found := g.state().AuthorityGrant(grant.Key); found {
		if !cached.Equal(grant) {
			return fault.ErrIdentityMismatch
		}
		return nil
	}
	proof, err := g.readAuthorityProof(ctx, grant.Key, "")
	if err != nil {
		return err
	}
	if proof.Grant == nil {
		return fault.ErrOperatorAuthorization
	}
	if !proof.Grant.Equal(grant) {
		return fault.ErrIdentityMismatch
	}
	return nil
}

// ReadGrant confirms and validates a retained grant for an exact authority key.
func (g *Group) ReadGrant(ctx context.Context, key topology.AuthorityKey) (topology.AuthorityGrantLookup, error) {
	var grant *topology.AuthorityGrant
	err := g.readControl(ctx, metaengine.QueryRequest{Kind: "authority_grant", AuthorityKey: &key}, &grant)
	if err != nil || grant == nil {
		return topology.AuthorityGrantLookup{}, err
	}
	if grant.Key != key || grant.Validate() != nil {
		return topology.AuthorityGrantLookup{}, fault.ErrUnreadable
	}
	return topology.AuthorityGrantLookup{Grant: grant.Clone(), Found: true}, nil
}

// RecordFence proposes durable resource fence installation evidence to meta
// consensus.
func (g *Group) RecordFence(ctx context.Context, evidence topology.FenceEvidence) error {
	_, err := g.propose(ctx, metaengine.RecordFenceProposal{Evidence: evidence})
	return err
}

// RecordEffect proposes exact durable effect consumption evidence to meta
// consensus.
func (g *Group) RecordEffect(ctx context.Context, receipt topology.EffectReceipt) error {
	_, err := g.propose(ctx, metaengine.RecordEffectProposal{Receipt: receipt.Clone(), At: time.Now().UTC()})
	return err
}

// EffectReceipts reads retained completion receipts for an exact grant.
func (g *Group) EffectReceipts(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	if err := g.ensureRunning(ctx); err != nil {
		return nil, err
	}
	return g.state().EffectReceipts(ctx, grant)
}

// ResolveAuthorityPeer uses the retained grant as durable identity evidence.
// Cleanup can outlive the current topology catalog, but may never be redirected
// to a successor member sharing the old node name.
func (g *Group) ResolveAuthorityPeer(ctx context.Context, grant topology.AuthorityGrant,
	target topology.AuthorityTarget) (load.Peer, error) {
	if err := g.ValidateGrant(ctx, grant); err != nil {
		return load.Peer{}, err
	}
	if !slices.Contains(grant.Targets, target) {
		return load.Peer{}, fault.ErrOperatorAuthorization
	}
	id := g.Identity()
	if target.NodeName != id.NodeName {
		var err error
		id, err = g.authorityPeerIdentity(ctx, grant, target)
		if err != nil {
			return load.Peer{}, err
		}
	}
	if id.ClusterID != grant.Key.ClusterID || id.NodeName != target.NodeName || id.MemberID != target.MemberID ||
		id.InstanceID == "" {
		return load.Peer{}, fault.ErrIdentityMismatch
	}
	return load.Peer{Node: id, Replica: load.ReplicaRef{GroupID: grant.Key.GroupID, ReplicaID: target.ReplicaID}}, nil
}

// ReadAuthorityFloors reads current host-resource grant floors for a named
// node.
func (g *Group) ReadAuthorityFloors(ctx context.Context, node string) ([]topology.AuthorityGrant, error) {
	var floors []topology.AuthorityGrant
	err := g.readControl(ctx, metaengine.QueryRequest{Kind: "authority_floors", Key: node}, &floors)
	return floors, err
}

// ReadAuthority performs a confirmed query of an operator's revocation
// obligations.
func (g *Group) ReadAuthority(ctx context.Context, id topology.OperatorID) (topology.AuthorityStatus, error) {
	var result topology.AuthorityStatus
	err := g.read(ctx, metaengine.QueryRequest{Kind: "authority", OperatorID: id}, &result)
	return result, err
}

// WaitRevoked returns only a durably completed requested revocation. A
// currently
// empty prerequisite list is not proof that revocation was ever requested.
func (g *Group) WaitRevoked(ctx context.Context, id topology.OperatorID) (topology.AuthorityStatus, error) {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		status, err := g.ReadAuthority(ctx, id)
		if err != nil {
			return topology.AuthorityStatus{}, err
		}
		if status.RevocationRequested && status.RevocationComplete {
			return status, nil
		}
		select {
		case <-ctx.Done():
			return topology.AuthorityStatus{}, ctx.Err()
		case <-tick.C:
		}
	}
}

// InstallFence is also the generic meta-protocol receiver. Host requests need
// no
// running load handle; group requests still name one exact local replica.
func (g *Group) InstallFence(parent context.Context, request load.FenceRequest) (load.FenceResult, error) {
	ctxHandle, err := g.beginRPC(parent)
	ctx, release := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return load.FenceResult{}, err
	}
	defer release()
	request.Grant = request.Grant.Clone()
	id := g.Identity()
	if request.Resource.Kind == topology.AuthorityHost && request.Grant.Purpose == topology.AuthorityFence {
		id, err = g.authorityIdentity()
		if err != nil {
			return load.FenceResult{}, err
		}
	}
	if request.To != id || request.From.ClusterID != request.To.ClusterID ||
		request.Grant.Key.ClusterID != request.To.ClusterID {
		return load.FenceResult{}, fault.ErrIdentityMismatch
	}
	if err := g.ValidateAuthorityCourier(ctx, request.Grant, request.From); err != nil {
		return load.FenceResult{}, err
	}
	if request.Resource.GroupID != request.Grant.Key.GroupID ||
		!slices.Contains(request.Grant.Resources, request.Resource) {
		return load.FenceResult{}, fault.ErrOperatorAuthorization
	}
	if request.Resource.Kind == topology.AuthorityGroup && request.Resource.GroupID == topology.MetaGroupID {
		return g.installMetaFence(ctx, request)
	}
	if g.opts.InstallLocalFence == nil {
		return load.FenceResult{}, fault.ErrNoController
	}
	return g.opts.InstallLocalFence(ctx, request)
}

func (g *Group) installMetaFence(parent context.Context, request load.FenceRequest) (load.FenceResult, error) {
	if request.ReplicaID != request.To.MemberID {
		return load.FenceResult{}, fault.ErrIdentityMismatch
	}
	ctxHandle, err := g.acquireEffectContext(parent)
	ctx, done := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return load.FenceResult{}, err
	}
	defer done()
	engine := g.engine.Load()
	if engine == nil {
		return load.FenceResult{}, fault.ErrReplicaNotReady
	}
	evidence, err := engine.InstallAuthority(ctx, request.Grant)
	if err != nil {
		return load.FenceResult{}, err
	}
	return load.FenceResult{From: request.To, Evidence: evidence}, nil
}
