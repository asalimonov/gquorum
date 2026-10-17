package metaservice

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (c groupControllers) DispatchEffect(parent context.Context, incoming topology.AuthorityGrant,
	resource topology.AuthorityResource, target topology.AuthorityTarget) (topology.EffectReceipt, error) {
	grant := incoming.Clone()
	if err := operator.ValidateEffectTarget(grant, resource, target); err != nil {
		return topology.EffectReceipt{}, err
	}
	handle, err := c.g.acquireEffectContext(parent)
	if err != nil {
		return topology.EffectReceipt{}, err
	}
	defer handle.Release()
	ctx := handle.Context
	if err := c.g.ValidateGrant(ctx, grant); err != nil {
		return topology.EffectReceipt{}, err
	}
	if grant.Key.GroupID == topology.MetaGroupID {
		return c.g.dispatchMetaEffect(ctx, grant, resource, target)
	}
	if c.g.opts.LoadGroups == nil {
		return topology.EffectReceipt{}, fault.ErrNoController
	}
	return c.g.opts.LoadGroups.DispatchEffect(ctx, grant, resource, target)
}

func (g *Group) dispatchMetaEffect(ctx context.Context, grant topology.AuthorityGrant,
	resource topology.AuthorityResource, target topology.AuthorityTarget) (topology.EffectReceipt, error) {
	peer, err := g.ResolveAuthorityPeer(ctx, grant, target)
	if err != nil {
		return topology.EffectReceipt{}, err
	}
	if err := g.installEffectFence(ctx, grant, resource, peer); err != nil {
		return topology.EffectReceipt{}, err
	}
	if err := g.ValidateGrant(ctx, grant); err != nil {
		return topology.EffectReceipt{}, err
	}
	request := load.ActionRequest{From: g.Identity(), To: peer, Grant: grant, Authorization: *grant.Action}
	if resource.Kind == topology.AuthorityHost {
		request.LocalTarget = storage.Identity{ClusterID: grant.Key.ClusterID, GroupID: grant.Key.GroupID,
			NodeName: target.NodeName, ReplicaID: target.ReplicaID}
	}
	// sendMetaAction addresses this exact peer. applyMetaConsensusAction refuses
	// followers; do not use metaEffect's legacy leader-forwarding entry here.
	receipts, err := g.sendMetaAction(ctx, request)
	if err != nil {
		return topology.EffectReceipt{}, err
	}
	return receipts[0], nil // sendMetaAction requires exactly one valid receipt.
}

func (g *Group) installEffectFence(ctx context.Context, grant topology.AuthorityGrant,
	resource topology.AuthorityResource, peer load.Peer) error {
	request := load.FenceRequest{From: g.Identity(), To: peer.Node, Resource: resource, Grant: grant}
	if resource.Kind == topology.AuthorityGroup {
		request.ReplicaID = peer.Replica.ReplicaID
	}
	proof, err := g.sendFence(ctx, request)
	if err != nil {
		return err
	}
	if proof.From != request.To || proof.Evidence.Receiver != request.To || proof.Evidence.Resource != resource ||
		proof.Evidence.ReplicaID != request.ReplicaID || proof.Evidence.Validate(grant) != nil {
		return fault.ErrOperationAuthorization
	}
	return g.RecordFence(ctx, proof.Evidence)
}
