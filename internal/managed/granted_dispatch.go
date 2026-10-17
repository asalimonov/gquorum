package managed

import (
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (h *Host) grantedPeer(ctx context.Context, grant topology.AuthorityGrant,
	target topology.AuthorityTarget) (load.Peer, error) {
	peer, err := h.cfg.ResolveAuthorityPeer(ctx, grant, target)
	if err != nil {
		return load.Peer{}, err
	}
	if peer.Node.ClusterID != grant.Key.ClusterID || peer.Node.NodeName != target.NodeName ||
		peer.Node.MemberID != target.MemberID ||
		peer.Replica != (load.ReplicaRef{GroupID: grant.Key.GroupID, ReplicaID: target.ReplicaID}) ||
		!completePeer(peer) {
		return load.Peer{}, fault.ErrIdentityMismatch
	}
	return peer, nil
}

func (h *Host) installGrantedFence(ctx context.Context, grant topology.AuthorityGrant,
	resource topology.AuthorityResource, peer load.Peer) error {
	if !slices.Contains(grant.Resources, resource) {
		return fault.ErrOperationAuthorization
	}
	replicaID := peer.Replica.ReplicaID
	if resource.Kind == topology.AuthorityHost {
		replicaID = 0
	}
	request := load.FenceRequest{
		From:      h.cfg.Identity(),
		To:        peer.Node,
		ReplicaID: replicaID,
		Resource:  resource,
		Grant:     grant.Clone(),
	}
	result, err := h.cfg.Transport.InstallFence(ctx, request)
	if err != nil {
		return err
	}
	if result.From != request.To || result.Evidence.Receiver != request.To || result.Evidence.Resource != resource ||
		result.Evidence.ReplicaID != replicaID || result.Evidence.Validate(grant) != nil {
		return fault.ErrOperationAuthorization
	}
	return h.cfg.RecordFence(ctx, result.Evidence)
}

func (h *Host) sendGrantedAction(ctx context.Context, request load.ActionRequest) (topology.EffectReceipt, error) {
	result, err := h.cfg.Transport.ApplyAction(ctx, request)
	if err != nil {
		return topology.EffectReceipt{}, err
	}
	if result.From != request.To || result.Authorization != request.Authorization ||
		result.LocalTarget != request.LocalTarget || len(result.Receipts) != 1 {
		return topology.EffectReceipt{}, fault.ErrOperationAuthorization
	}
	receipt := result.Receipts[0]
	if receipt.Validate(request.Grant) != nil {
		return topology.EffectReceipt{}, fault.ErrOperationAuthorization
	}
	resource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: request.Grant.Key.GroupID}
	if request.LocalTarget != (storage.Identity{}) {
		resource.Kind, resource.NodeName = topology.AuthorityHost, request.To.Node.NodeName
		if receipt.Target != actionTarget(request) {
			return topology.EffectReceipt{}, fault.ErrOperationAuthorization
		}
	}
	if receipt.Resource != resource {
		return topology.EffectReceipt{}, fault.ErrOperationAuthorization
	}
	return receipt.Clone(), nil
}
