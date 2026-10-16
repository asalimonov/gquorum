package operator

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type dispatchRequest struct {
	key      topology.AuthorityKey
	resource topology.AuthorityResource
	target   topology.AuthorityTarget
}

type effectDispatch struct {
	grant    topology.AuthorityGrant
	resource topology.AuthorityResource
	target   topology.AuthorityTarget
	cleanup  bool
}

func (r effectDispatch) key() dispatchRequest {
	key := dispatchRequest{key: r.grant.Key, resource: r.resource, target: r.target}
	if r.resource.Kind == topology.AuthorityGroup {
		key.target = topology.AuthorityTarget{} // One native request, even across a leader change.
	}
	return key
}

func (r effectDispatch) host() dispatchHost {
	return dispatchHost{group: r.grant.Key.GroupID, node: r.target.NodeName}
}

// DispatchEffect reserves one exact ordinary RPC. It shares the host/group gate
// with cleanup, and records valid late evidence before reporting lost leadership.
func (e *Controller) DispatchEffect(ctx context.Context, grant topology.AuthorityGrant,
	resource topology.AuthorityResource, target topology.AuthorityTarget) (topology.EffectReceipt, error) {
	return e.dispatchRequest(ctx, effectDispatch{grant: grant, resource: resource, target: target})
}

func (e *Controller) dispatchRequest(ctx context.Context, request effectDispatch) (topology.EffectReceipt, error) {
	request.grant = request.grant.Clone()
	if err := ValidateEffectTarget(request.grant, request.resource, request.target); err != nil {
		return topology.EffectReceipt{}, err
	}
	if !e.ownsGroup(request.grant.Key.GroupID) {
		return topology.EffectReceipt{}, fault.ErrOperationAuthorization
	}
	e.mu.Lock()
	run := e.run
	e.mu.Unlock()
	permit, err := e.reserveDispatch(ctx, dispatchPermit{run: run, host: request.host(), cleanup: request.cleanup})
	if err != nil {
		return topology.EffectReceipt{}, err
	}
	receipt, err := e.dispatchReserved(ctx, permit, request)
	e.releaseDispatch(permit, err)
	return receipt, err
}

func (e *Controller) dispatchReserved(ctx context.Context, permit *dispatchPermit,
	request effectDispatch) (topology.EffectReceipt, error) {
	if err := e.bindRequest(ctx, permit, request); err != nil {
		return topology.EffectReceipt{}, err
	}
	run := permit.run
	bounded, cancel := context.WithTimeout(ctx, e.proposalTimeout)
	stop := context.AfterFunc(run.ctx, cancel)
	defer func() { stop(); cancel() }()
	receipt, err := e.groups.DispatchEffect(bounded, request.grant, request.resource, request.target)
	err = e.finishRequest(run.ctx, request, receipt, err)
	if err == nil {
		err = bounded.Err()
	}
	if err == nil && !e.scoped.IsMetaLeader() {
		err = fault.ErrNotLeader
	}
	return receipt, err
}

func (e *Controller) finishRequest(ctx context.Context, request effectDispatch,
	receipt topology.EffectReceipt, callErr error) error {
	if receipt.Key == (topology.AuthorityKey{}) && callErr != nil {
		return callErr
	}
	if receipt.Resource != request.resource ||
		request.resource.Kind == topology.AuthorityHost && receipt.Target != request.target ||
		receipt.Validate(request.grant) != nil {
		return fault.ErrOperationAuthorization
	}
	// The courier's deadline or ownership may have changed while the recipient
	// completed. Recording exact proof is useful even when advancing is forbidden.
	bounded, cancel := context.WithTimeout(ctx, e.proposalTimeout)
	defer cancel()
	if err := e.authority.RecordEffect(bounded, receipt.Clone()); err != nil {
		return err
	}
	return callErr
}
