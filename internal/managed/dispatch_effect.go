package managed

import (
	"context"
	"fmt"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// DispatchEffect installs authority and delivers one exact target. Cancellation
// releases this courier; durable actionWorks still owns any running local work.
func (h *Host) DispatchEffect(parent context.Context, incoming topology.AuthorityGrant,
	resource topology.AuthorityResource, target topology.AuthorityTarget) (topology.EffectReceipt, error) {
	ctx, cancel := context.WithTimeout(parent, h.cfg.EffectTimeout)
	defer cancel()
	handle, err := h.beginWork(ctx)
	if err != nil {
		return topology.EffectReceipt{}, err
	}
	defer handle.Release()
	ctx = handle.Context
	grant := incoming.Clone()
	if err := topology.ValidateLoadID(grant.Key.GroupID); err != nil {
		return topology.EffectReceipt{}, err
	}
	if err := operator.ValidateEffectTarget(grant, resource, target); err != nil {
		return topology.EffectReceipt{}, err
	}
	if h.cfg.ResolveAuthorityPeer == nil || h.cfg.RecordFence == nil {
		return topology.EffectReceipt{}, fault.ErrUnsupported
	}
	if err := h.cfg.ValidateGrant(ctx, grant); err != nil {
		return topology.EffectReceipt{}, err
	}
	peer, err := h.grantedPeer(ctx, grant, target)
	if err != nil {
		return topology.EffectReceipt{}, fmt.Errorf("resolve effect recipient: %w", err)
	}
	if err := h.installGrantedFence(ctx, grant, resource, peer); err != nil {
		return topology.EffectReceipt{}, fmt.Errorf("install effect fence: %w", err)
	}
	// Resolving the peer and committing the fence can wait. Recheck the full
	// immutable grant before submitting the effect; the receiver checks it too.
	if err := h.cfg.ValidateGrant(ctx, grant); err != nil {
		return topology.EffectReceipt{}, err
	}
	request := load.ActionRequest{From: h.cfg.Identity(), To: peer, Grant: grant, Authorization: *grant.Action}
	if resource.Kind == topology.AuthorityHost {
		request.LocalTarget = storage.Identity{ClusterID: grant.Key.ClusterID, GroupID: grant.Key.GroupID,
			NodeName: target.NodeName, ReplicaID: target.ReplicaID}
	}
	receipt, err := h.sendGrantedAction(ctx, request)
	if err != nil {
		return topology.EffectReceipt{}, fmt.Errorf("apply granted action: %w", err)
	}
	return receipt, nil
}
