package main

import (
	"context"
	"errors"
	"net"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/util"
)

// launchLocked owns one worker, independent of any request waiter. Cancellation
// queues its durable intent behind the joined Run; it never overlaps two owners.
func (s *recoveryJobs) launchLocked() {
	if s.done != nil {
		s.wake = true
		return
	}
	if s.job == nil || !s.job.Maintenance {
		return
	}
	s.wake = false
	ctx, cancel := context.WithCancel(s.app.workCtx)
	s.cancel, s.done = cancel, make(chan struct{})
	done := s.done
	s.workers.Go(func() {
		err := s.execute(ctx)
		cancel()
		if err != nil && s.app.workCtx.Err() == nil && !errors.Is(err, context.Canceled) {
			_ = s.update(func(job *recoveryJob) { job.Error = err.Error() })
		}
		s.mu.Lock()
		close(done)
		s.done, s.cancel = nil, nil
		// A new confirm/retry/cancel may arrive after progress is saved but before
		// this owner returns. Join its call and callbacks before consuming that wake.
		if s.wake && s.app.workCtx.Err() == nil {
			s.launchLocked()
		}
		s.mu.Unlock()
	})
}
func (s *recoveryJobs) snapshot() *recoveryJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneRecoveryJob(s.job)
}
func (s *recoveryJobs) progress(phase string, change func(*recoveryJob)) error {
	return s.update(func(job *recoveryJob) {
		if job.Cancellation == nil {
			job.Phase = phase
		}
		change(job)
	})
}
func (s *recoveryJobs) execute(ctx context.Context) error {
	if err := s.app.stopNodeGeneration(ctx); err != nil {
		return err
	}
	handle, err := s.handle()
	if err != nil {
		return err
	}
	job := s.snapshot()
	if job.Cancellation != nil {
		return s.executeCancel(ctx, handle, job)
	}
	if job.Acceptance == nil {
		plan, inspectErr := handle.Inspect(ctx, job.Request)
		saveErr := s.progress("prepared", func(next *recoveryJob) {
			next.Plan = &plan
			if plan.Request.RecoveryID != "" {
				next.Request = plan.Request
			}
		})
		if saveErr == nil && s.afterInspect != nil {
			saveErr = s.afterInspect(ctx)
		}
		return errors.Join(inspectErr, saveErr)
	}
	return s.executeAccepted(ctx, handle, job)
}
func (s *recoveryJobs) executeAccepted(ctx context.Context, handle gquorum.Recovery, job *recoveryJob) error {
	// Resolve a lost approval response from public durable status before repeating
	// the exact saved Request/Acceptance. Never replace a generated request ID.
	status, err := handle.Status(ctx, job.Request.RecoveryID)
	if errors.Is(err, fault.ErrHistoryUnavailable) {
		operation, approveErr := handle.Approve(ctx, job.Request, *job.Acceptance)
		if approveErr != nil {
			return approveErr
		}
		if err := s.progress("running", func(next *recoveryJob) { next.Operation = &operation }); err != nil {
			return err
		}
		status, err = handle.Status(ctx, job.Request.RecoveryID)
	}
	if err != nil {
		return err
	}
	if job.Operation == nil {
		if err := s.progress("running", func(next *recoveryJob) {
			next.Operation = &recovery.Operation{RecoveryID: status.RecoveryID, PlanDigest: status.PlanDigest,
				BackupName: status.BackupName, AcceptedAt: status.AcceptedAt, Mode: status.Mode}
		}); err != nil {
			return err
		}
	}
	if !startableRecovery(status) {
		status, err = handle.Run(ctx, job.Request.RecoveryID)
		if err != nil {
			return err
		}
	}
	if err := s.progress("starting", func(next *recoveryJob) { next.Status = &status }); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.app.startRecoveredGeneration(ctx, status); err != nil {
		return err
	}
	if err := s.app.currentNode().WaitReady(ctx); err != nil {
		return err
	}
	if err := s.app.persistEstablished(); err != nil {
		return err
	}
	status, err = handle.Status(ctx, job.Request.RecoveryID)
	if err != nil {
		return err
	}
	if status.Phase != recovery.Complete && status.Phase != recovery.Joined {
		return fault.ErrReplicaNotReady
	}
	return s.update(func(next *recoveryJob) {
		next.Status = &status
		if next.Cancellation == nil {
			next.Phase, next.Maintenance, next.Error = "complete", false, ""
		}
	})
}
func startableRecovery(status recovery.Status) bool {
	switch status.Phase {
	case recovery.AwaitingStart, recovery.Importing, recovery.Validating, recovery.Complete, recovery.JoinReady, recovery.Joined:
		return true
	default:
		return false
	}
}
func (s *recoveryJobs) executeCancel(ctx context.Context, handle gquorum.Recovery, job *recoveryJob) error {
	status, err := recovery.Status{}, error(fault.ErrHistoryUnavailable)
	if job.Acceptance != nil {
		status, err = handle.Status(ctx, job.Request.RecoveryID)
	}
	if !errors.Is(err, fault.ErrHistoryUnavailable) {
		if err != nil {
			return err
		}
		audit := job.Cancellation
		status, err = handle.Cancel(ctx, job.Request.RecoveryID, audit.Actor, audit.Reason)
		if err != nil {
			return err
		}
		if status.Phase != recovery.Cancelled {
			return fault.ErrUnavailable
		}
	}
	if err := s.update(func(next *recoveryJob) {
		if status.RecoveryID != "" {
			next.Status = &status
		}
		next.Phase, next.Maintenance, next.Error = "cancelled", false, ""
	}); err != nil {
		return err
	}
	return s.app.restartNodeGeneration(ctx)
}
func (a *application) startRecoveredGeneration(ctx context.Context, status recovery.Status) error {
	a.nodeOpMu.Lock()
	defer a.nodeOpMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	opts := a.nodeOptions
	opts.Identity.InstanceID = util.NewInstanceID()
	opts.Bootstrap.JoinSeeds = nil
	for _, peer := range a.cfg.Peers {
		if peer.Name != opts.Identity.NodeName {
			opts.Bootstrap.JoinSeeds = append(opts.Bootstrap.JoinSeeds, peer.MetaAddress)
		}
	}
	var err error
	if status.Phase == recovery.Complete || status.Phase == recovery.Joined {
		opts.Identity.ClusterID, opts.Identity.MemberID = status.Identity.ClusterID, status.Identity.MemberID
		opts.Bootstrap = gquorum.BootstrapOptions{}
	} else {
		opts, err = gquorum.RecoveryStartOptions(status, opts)
		if err != nil {
			return err
		}
	}
	listener, err := net.Listen("tcp", a.nodeOptions.Meta.Network.Listener.Addr().String())
	if err != nil {
		return err
	}
	opts.Meta.Network.Listener = listener
	binding, err := a.prepareLoadBinding(&opts)
	if err != nil {
		_ = listener.Close()
		return err
	}
	node, err := gquorum.NewNode(opts)
	if err != nil {
		_ = listener.Close()
		return err
	}
	if err = binding.bind(a, node); err != nil {
		_ = node.Stop(ctx)
		_ = binding.transport.Close(ctx)
		_ = listener.Close()
		return err
	}
	a.installGeneration(node, listener, binding)
	if a.metaProxy != nil {
		a.metaProxy.internal = listener
	}
	// The recovery worker owns readiness and affiliation. A failure must leave
	// management available, rather than trigger the ordinary process-fatal path.
	if err := node.Start(a.lifeCtx); err != nil {
		return err
	}
	generation := a.generation
	generation.workers.Go(func() { a.observeServingContext(generation.ctx) })
	return nil
}
func (a *application) installGeneration(node gquorum.Node, listener net.Listener, binding *fixtureLoadBinding) {
	next := a.makeNodeGeneration(node, listener)
	next.production = binding
	a.mu.Lock()
	a.serving = make(map[string]topology.GroupProgress)
	a.startupObserved, a.startupMissing = false, ""
	a.mu.Unlock()
	a.generationMu.Lock()
	a.generation = next
	a.node, a.cluster = nil, nil
	a.generationMu.Unlock()
}
