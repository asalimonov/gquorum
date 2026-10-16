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
	if run == nil || run.dispatch == nil {
		return topology.EffectReceipt{}, fault.ErrStopped
	}
	if err := e.reserveRequest(ctx, run.dispatch, request); err != nil {
		return topology.EffectReceipt{}, err
	}
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
	e.releaseRequest(run.dispatch, request, err)
	return receipt, err
}

func (e *Controller) reserveRequest(ctx context.Context, d *controllerDispatch, request effectDispatch) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || ctx.Err() != nil {
		return fault.ErrStopped
	}
	if !e.scoped.IsMetaLeader() {
		return fault.ErrNotLeader
	}
	limits := e.rules.Limits.WithDefaults()
	if request.cleanup && d.cleanup >= limits.MaxConcurrentCleanup ||
		!request.cleanup && d.ordinary >= limits.MaxConcurrentGroups {
		return fault.ErrUnavailable
	}
	host, key := request.host(), request.key()
	now := e.clock.Now()
	d.pruneBackoff(now, e.maxRetry)
	if d.hosts[host] || d.requests[key] || now.Before(d.backoff[host].until) {
		return fault.ErrUnavailable
	}
	if d.hosts == nil {
		d.hosts = make(map[dispatchHost]bool)
		d.requests = make(map[dispatchRequest]bool)
	}
	d.hosts[host], d.requests[key] = true, true
	if request.cleanup {
		d.cleanup++
	} else {
		d.ordinary++
	}
	d.workers.Add(1) // Admission and close share mu; never Add after shutdown Wait starts.
	return nil
}

func (e *Controller) releaseRequest(d *controllerDispatch, request effectDispatch, err error) {
	d.mu.Lock()
	delete(d.hosts, request.host())
	delete(d.requests, request.key())
	if request.cleanup {
		d.cleanup--
	} else {
		d.ordinary--
	}
	if err != nil && !d.closed {
		e.backoffDispatch(d, request.host())
	} else {
		delete(d.backoff, request.host())
	}
	d.mu.Unlock()
	d.workers.Done()
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
