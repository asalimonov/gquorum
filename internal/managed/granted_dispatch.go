package managed

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// DispatchGrant installs resource authority before delivering an immutable
// action. Partial receipts remain useful when another host is unavailable: the
// controller must persist them even when the overall attempt returns an error.
func (h *Host) DispatchGrant(parent context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	ctx, cancel := context.WithTimeout(parent, h.cfg.EffectTimeout)
	defer cancel()
	ctx, release, err := h.beginWork(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	grant = grant.Clone()
	if err := topology.ValidateLoadID(grant.Key.GroupID); err != nil {
		return nil, err
	}
	if grant.Action == nil || grant.Validate() != nil {
		return nil, fault.ErrOperatorAuthorization
	}
	if h.cfg.ResolveAuthorityPeer == nil || h.cfg.RecordFence == nil {
		return nil, fault.ErrUnsupported
	}
	if err := h.cfg.ValidateGrant(ctx, grant); err != nil {
		return nil, err
	}
	var receipts []topology.EffectReceipt
	if localAction(grant.Action.Kind) {
		var failures []error
		var mu sync.Mutex
		var workers sync.WaitGroup
		for _, resource := range grant.Resources {
			if resource.Kind != topology.AuthorityHost {
				continue
			}
			// Different hosts progress independently. Historical replicas on the
			// same host share a group gate and must be reconciled in order.
			workers.Go(func() {
				got, err := h.dispatchGrantedHost(ctx, grant, resource)
				mu.Lock()
				receipts = append(receipts, got...)
				if err != nil {
					failures = append(failures, err)
				}
				mu.Unlock()
			})
		}
		workers.Wait()
		sortReceipts(receipts)
		if err := errors.Join(failures...); err != nil {
			return receipts, err
		}
		if grant.Action.Kind != topology.StepCreateFoundingGroup {
			return receipts, nil
		}
	}
	// Status locates a possible leader; only an applied group receipt proves
	// the effect. Founding reaches this point after all host creation receipts.
	status, err := h.ObserveGroup(ctx, grant.Key.GroupID)
	if err != nil {
		return receipts, err
	}
	if status.LeaderID == 0 {
		return receipts, fault.ErrReplicaNotReady
	}
	var peer load.Peer
	for _, target := range grant.Targets {
		if target.ReplicaID != status.LeaderID {
			continue
		}
		peer, err = h.grantedPeer(ctx, grant, target)
		if err == nil {
			break
		}
	}
	if err != nil {
		return receipts, err
	}
	if !completePeer(peer) {
		return receipts, fault.ErrIdentityMismatch
	}
	resource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: grant.Key.GroupID}
	if err := h.installGrantedFence(ctx, grant, resource, peer); err != nil {
		return receipts, err
	}
	receipt, err := h.sendGrantedAction(ctx, load.ActionRequest{From: h.cfg.Identity(), To: peer, Grant: grant, Authorization: *grant.Action})
	if err == nil {
		receipts = append(receipts, receipt)
		sortReceipts(receipts)
	}
	return receipts, err
}

func sortReceipts(receipts []topology.EffectReceipt) {
	slices.SortFunc(receipts, func(a, b topology.EffectReceipt) int {
		if order := a.Resource.Compare(b.Resource); order != 0 {
			return order
		}
		return a.Target.Compare(b.Target)
	})
}

func (h *Host) grantedPeer(ctx context.Context, grant topology.AuthorityGrant, target topology.AuthorityTarget) (load.Peer, error) {
	peer, err := h.cfg.ResolveAuthorityPeer(ctx, grant, target)
	if err != nil {
		return load.Peer{}, err
	}
	if !completePeer(peer) || peer.Node.ClusterID != grant.Key.ClusterID || peer.Node.NodeName != target.NodeName || peer.Node.MemberID != target.MemberID ||
		peer.Replica != (load.ReplicaRef{GroupID: grant.Key.GroupID, ReplicaID: target.ReplicaID}) {
		return load.Peer{}, fault.ErrIdentityMismatch
	}
	return peer, nil
}

func (h *Host) dispatchGrantedHost(ctx context.Context, grant topology.AuthorityGrant, resource topology.AuthorityResource) ([]topology.EffectReceipt, error) {
	var receipts []topology.EffectReceipt
	for _, target := range grant.Targets {
		if target.NodeName != resource.NodeName {
			continue
		}
		peer, err := h.grantedPeer(ctx, grant, target)
		if err != nil {
			return receipts, err
		}
		if err := h.installGrantedFence(ctx, grant, resource, peer); err != nil {
			return receipts, err
		}
		local := storage.Identity{ClusterID: grant.Key.ClusterID, GroupID: grant.Key.GroupID, NodeName: target.NodeName, ReplicaID: target.ReplicaID}
		receipt, err := h.sendGrantedAction(ctx, load.ActionRequest{From: h.cfg.Identity(), To: peer, Grant: grant, Authorization: *grant.Action, LocalTarget: local})
		if err != nil {
			return receipts, err
		}
		receipts = append(receipts, receipt)
	}
	return receipts, nil
}

func (h *Host) installGrantedFence(ctx context.Context, grant topology.AuthorityGrant, resource topology.AuthorityResource, peer load.Peer) error {
	if !slices.Contains(grant.Resources, resource) {
		return fault.ErrOperatorAuthorization
	}
	replicaID := peer.Replica.ReplicaID
	if resource.Kind == topology.AuthorityHost {
		replicaID = 0
	}
	request := load.FenceRequest{From: h.cfg.Identity(), To: peer.Node, ReplicaID: replicaID, Resource: resource, Grant: grant.Clone()}
	result, err := h.cfg.Transport.InstallFence(ctx, request)
	if err != nil {
		return err
	}
	if result.From != request.To || result.Evidence.Receiver != request.To || result.Evidence.Resource != resource || result.Evidence.ReplicaID != replicaID || result.Evidence.Validate(grant) != nil {
		return fault.ErrOperatorAuthorization
	}
	return h.cfg.RecordFence(ctx, result.Evidence)
}

func (h *Host) sendGrantedAction(ctx context.Context, request load.ActionRequest) (topology.EffectReceipt, error) {
	result, err := h.cfg.Transport.ApplyAction(ctx, request)
	if err != nil {
		return topology.EffectReceipt{}, err
	}
	if result.From != request.To || result.Authorization != request.Authorization || result.LocalTarget != request.LocalTarget || len(result.Receipts) != 1 {
		return topology.EffectReceipt{}, fault.ErrOperatorAuthorization
	}
	receipt := result.Receipts[0]
	if receipt.Validate(request.Grant) != nil {
		return topology.EffectReceipt{}, fault.ErrOperatorAuthorization
	}
	resource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: request.Grant.Key.GroupID}
	if request.LocalTarget != (storage.Identity{}) {
		resource.Kind, resource.NodeName = topology.AuthorityHost, request.To.Node.NodeName
		if receipt.Target != actionTarget(request) {
			return topology.EffectReceipt{}, fault.ErrOperatorAuthorization
		}
	}
	if receipt.Resource != resource {
		return topology.EffectReceipt{}, fault.ErrOperatorAuthorization
	}
	return receipt.Clone(), nil
}
