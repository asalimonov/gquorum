package operator

import (
	"context"

	"github.com/asalimonov/gquorum/topology"
)

func (e *Controller) cleanupAvailable(run *controllerRun) int {
	d := run.dispatch
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return 0
	}
	return max(0, e.rules.Limits.WithDefaults().MaxConcurrentCleanup-d.cleanup)
}

func (e *Controller) runDetachedTick(ctx context.Context, run *controllerRun) {
	d := run.dispatch
	d.mu.Lock()
	first := d.fencesFirst
	d.fencesFirst = !first
	d.mu.Unlock()
	if first {
		e.runFenceTick(ctx, run)
		e.runCleanupTick(ctx, run)
	} else {
		e.runCleanupTick(ctx, run)
		e.runFenceTick(ctx, run)
	}
}

func (e *Controller) runFenceTick(ctx context.Context, run *controllerRun) {
	available := e.cleanupAvailable(run)
	if available == 0 || !e.scoped.IsMetaLeader() {
		return
	}
	bounded, cancel := context.WithTimeout(ctx, e.proposalTimeout)
	defer cancel()
	work, err := e.scoped.ReadyFences(bounded, available)
	if err != nil || bounded.Err() != nil {
		return
	}
	for _, item := range work {
		e.dispatchFence(ctx, run, item)
	}
}

func (e *Controller) dispatchFence(ctx context.Context, run *controllerRun, work FenceWork) {
	if !e.ownsGroup(work.Resource.GroupID) {
		return
	}
	host := dispatchHost{group: work.Resource.GroupID, node: work.Resource.NodeName}
	permit, err := e.reserveDispatch(ctx, dispatchPermit{run: run, host: host, cleanup: true})
	if err != nil {
		return
	}
	go func() {
		bounded, cancel := context.WithTimeout(ctx, e.proposalTimeout)
		defer cancel()
		err := e.runFenceWork(bounded, permit, work)
		e.releaseDispatch(permit, err)
		if err == nil {
			e.Signal()
		}
	}()
}

func (e *Controller) runFenceWork(ctx context.Context, permit *dispatchPermit, work FenceWork) error {
	target, err := e.scoped.ResolveFenceTarget(ctx, work)
	if err != nil {
		return err
	}
	if err := ValidateFenceTarget(work.Grant, work.Resource, target); err != nil {
		return err
	}
	request := effectDispatch{grant: work.Grant, resource: work.Resource, target: target, cleanup: true}
	if err := e.bindRequest(ctx, permit, request); err != nil {
		return err
	}
	return e.scoped.DispatchFence(ctx, work.Grant, work.Resource, target)
}

func cleanupTarget(work CleanupWork) topology.AuthorityTarget {
	return topology.AuthorityTarget{NodeName: work.Identity.NodeName,
		MemberID: work.MemberID, ReplicaID: work.Identity.ReplicaID}
}
