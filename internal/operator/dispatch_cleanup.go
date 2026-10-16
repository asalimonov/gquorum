package operator

import (
	"cmp"
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (e *Controller) runCleanupTick(ctx context.Context, run *controllerRun) {
	available := e.cleanupAvailable(run)
	if available < 1 || !e.scoped.IsMetaLeader() {
		return
	}
	bounded, cancel := context.WithTimeout(ctx, e.proposalTimeout)
	defer cancel()
	work, err := e.scoped.ReadyCleanup(bounded, available)
	if err != nil || bounded.Err() != nil {
		return // Failure in this lane never suppresses required group work.
	}
	orderCleanupTargets(run.dispatch, work)
	for _, item := range work {
		e.dispatchCleanup(ctx, run, item)
	}
}

func (e *Controller) dispatchCleanup(ctx context.Context, run *controllerRun, work CleanupWork) {
	if work.Identity.GroupID != work.Action.GroupID || !e.ownsGroup(work.Identity.GroupID) {
		return
	}
	host := dispatchHost{group: work.Identity.GroupID, node: work.Identity.NodeName}
	permit, err := e.reserveDispatch(ctx, dispatchPermit{run: run, host: host, cleanup: true})
	if err != nil {
		return
	}
	run.dispatch.mu.Lock()
	e.rememberDispatch(permit, dispatchRequest{target: cleanupTarget(work)})
	run.dispatch.mu.Unlock()
	go func() {
		bounded, cancel := context.WithTimeout(ctx, e.proposalTimeout)
		defer cancel()
		err := e.runCleanupWork(bounded, permit, work)
		e.releaseDispatch(permit, err)
		if err == nil {
			e.Signal()
		}
	}()
}

func (e *Controller) runCleanupWork(ctx context.Context, permit *dispatchPermit, work CleanupWork) error {
	grant, err := e.scoped.BeginCleanupEffect(ctx, work.Identity, work.Action)
	if err != nil {
		return err
	}
	if grant.Key.ClusterID != work.Identity.ClusterID || grant.Key.GroupID != work.Identity.GroupID ||
		grant.Action == nil || *grant.Action != work.Action {
		return fault.ErrOperationAuthorization
	}
	request := effectDispatch{grant: grant, cleanup: true,
		resource: topology.AuthorityResource{Kind: topology.AuthorityHost,
			GroupID: work.Identity.GroupID, NodeName: work.Identity.NodeName},
		target: cleanupTarget(work)}
	if err := ValidateEffectTarget(grant, request.resource, request.target); err != nil {
		return err
	}
	// Each selected identity is dispatched independently even when the retained
	// action/grant spans several hosts. Other targets' failures cannot discard it.
	_, err = e.dispatchReserved(ctx, permit, request)
	return err
}

func orderCleanupTargets(d *controllerDispatch, work []CleanupWork) {
	d.mu.Lock()
	defer d.mu.Unlock()
	slices.SortStableFunc(work, func(a, b CleanupWork) int {
		order := cmp.Or(cmp.Compare(a.Identity.GroupID, b.Identity.GroupID),
			cmp.Compare(a.Identity.NodeName, b.Identity.NodeName))
		if order != 0 {
			return order
		}
		last := d.backoff[dispatchHost{group: a.Identity.GroupID, node: a.Identity.NodeName}].last.target
		left, right := cleanupTarget(a), cleanupTarget(b)
		afterLeft, afterRight := left.Compare(last) > 0, right.Compare(last) > 0
		if afterLeft != afterRight {
			if afterLeft {
				return -1
			}
			return 1
		}
		return left.Compare(right)
	})
}
