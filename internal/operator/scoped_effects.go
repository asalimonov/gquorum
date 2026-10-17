package operator

import (
	"context"
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (e *Controller) performScopedEffect(ctx context.Context, work WorkItem, grant topology.AuthorityGrant) error {
	receipts, err := e.authority.EffectReceipts(ctx, grant)
	if err != nil {
		return fmt.Errorf("read receipts: %w", err)
	}
	complete, err := receiptSetComplete(grant, receipts)
	if err != nil {
		return fmt.Errorf("validate receipts: %w", err)
	}
	if complete && grant.Action.Kind != topology.StepTransferLeader {
		return nil
	}
	request, err := e.nextScopedRequest(ctx, grant, receipts)
	if err != nil {
		return fmt.Errorf("select next target: %w", err)
	}
	if err := e.checkScopedWork(ctx, work); err != nil {
		return fmt.Errorf("revalidate scope: %w", err)
	}
	_, err = e.dispatchRequest(ctx, request)
	e.rememberScopedRequest(request)
	if err != nil {
		return fmt.Errorf("dispatch target %s: %w", request.target.NodeName, err)
	}
	complete, err = e.effectComplete(ctx, grant)
	if err != nil {
		return fmt.Errorf("verify effect receipt: %w", err)
	}
	if !complete {
		return fault.ErrAuthorityPending // Next worker takes the next missing target/phase.
	}
	return nil
}

func (e *Controller) rememberScopedRequest(request effectDispatch) {
	if request.resource.Kind != topology.AuthorityHost {
		return
	}
	e.mu.Lock()
	run := e.run
	e.mu.Unlock()
	if run == nil || run.dispatch == nil {
		return
	}
	d := run.dispatch
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.closed {
		// RPC backoff can evict this row when capacity is one. Restore the
		// logical block cursor after release so the first failed founding host
		// cannot monopolize the next bounded worker.
		e.rememberDispatch(&dispatchPermit{run: run,
			host: dispatchHost{group: request.grant.Key.GroupID, node: request.grant.Action.NodeName}}, request.key())
	}
}

func (e *Controller) nextScopedRequest(ctx context.Context, grant topology.AuthorityGrant,
	receipts []topology.EffectReceipt) (effectDispatch, error) {
	for _, resource := range grant.Resources {
		if resource.Kind == topology.AuthorityHost && !resourceReceiptsComplete(resource, grant.Targets, receipts) {
			// Founding must finish every recorded host receipt before asking a
			// native leader to attest the voter set. Never repeat the old fan-out.
			return e.nextScopedHost(grant, receipts)
		}
	}
	resource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: grant.Key.GroupID}
	if !slices.Contains(grant.Resources, resource) {
		return effectDispatch{}, fault.ErrOperationAuthorization
	}
	status, err := e.groups.Status(ctx, grant.Key.GroupID)
	if err != nil {
		return effectDispatch{}, err
	}
	if status.GroupID != grant.Key.GroupID {
		return effectDispatch{}, fault.ErrOperationAuthorization
	}
	for _, target := range grant.Targets {
		if status.LeaderID != 0 && target.ReplicaID == status.LeaderID {
			return effectDispatch{grant: grant, resource: resource, target: target}, nil
		}
	}
	return effectDispatch{}, fault.New(fault.ReplicaNotReady,
		fmt.Sprintf("no granted target matches observed leader %d for %s (targets=%v)", status.LeaderID,
			grant.Key.GroupID, grant.Targets), nil)
}

func (e *Controller) nextScopedHost(grant topology.AuthorityGrant,
	receipts []topology.EffectReceipt) (effectDispatch, error) {
	e.mu.Lock()
	run := e.run
	e.mu.Unlock()
	if run == nil || run.dispatch == nil {
		return effectDispatch{}, fault.ErrStopped
	}
	d := run.dispatch
	d.mu.Lock()
	defer d.mu.Unlock()
	host := dispatchHost{group: grant.Key.GroupID, node: grant.Action.NodeName}
	last := d.backoff[host].last.target
	targets := slices.Clone(grant.Targets)
	slices.SortFunc(targets, func(a, b topology.AuthorityTarget) int { return a.Compare(b) })
	start := slices.IndexFunc(targets, func(t topology.AuthorityTarget) bool { return t.Compare(last) > 0 })
	for offset := range targets {
		target := targets[(max(0, start)+offset)%len(targets)]
		request := effectDispatch{grant: grant, target: target, resource: topology.AuthorityResource{
			Kind: topology.AuthorityHost, GroupID: grant.Key.GroupID, NodeName: target.NodeName}}
		if !scopedHostMissing(request, receipts) || d.hosts[request.host()] ||
			e.clock.Now().Before(d.backoff[request.host()].until) {
			continue
		}
		e.rememberDispatch(&dispatchPermit{run: run, host: host}, request.key())
		return request, nil
	}
	return effectDispatch{}, fault.ErrUnavailable
}

func scopedHostMissing(request effectDispatch, receipts []topology.EffectReceipt) bool {
	return slices.Contains(request.grant.Resources, request.resource) &&
		!slices.ContainsFunc(receipts, func(r topology.EffectReceipt) bool {
			return r.Resource == request.resource && r.Target == request.target
		})
}
