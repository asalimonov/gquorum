package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	mathrand "math/rand/v2"
	"time"

	"github.com/asalimonov/gquorum/examples/scheduler/task"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
)

var retry = fault.RetryOptions{}
var proposalRetry = load.ProposalRetryOptions{}

type reconcileTrigger uint8

const (
	regularTick reconcileTrigger = iota
	burstTick
)

const taskSeedBytes = 32

// runScheduler runs only for one leadership term; losing leadership cancels
// ctx.
// Each tick revokes failed work, assigns pending work, or creates a burst.
func (s *replicaService) runScheduler(ctx context.Context, leader *load.Leadership) error {
	s.logger.Info("scheduler_started", "term", leader.Term())
	defer s.logger.Info("scheduler_stopped", "term", leader.Term())
	defer s.replicas.Store(nil)
	session := newSchedulerSession()
	tick := time.NewTicker(schedulerTick)
	defer tick.Stop()
	burst := time.NewTicker(task.BurstInterval)
	defer burst.Stop()
	for {
		trigger := regularTick
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		case <-burst.C:
			trigger = burstTick
		}
		if err := s.reconcile(ctx, leader, session, trigger); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !fault.Retryable(err) {
				return err
			}
			s.logger.Info("scheduler_waiting", "error", err)
		}
	}
}

func (s *replicaService) reconcile(ctx context.Context, leader *load.Leadership, session *schedulerSession,
	trigger reconcileTrigger) error {
	view, err := fault.Retry(ctx, retry, func(call context.Context) (task.State, error) {
		return task.ReadState(call, s.group)
	})
	if err != nil {
		return err
	}
	replicas, err := leader.Replicas(ctx)
	if err != nil {
		return err
	}
	s.replicas.Store(&replicas)
	self, now := s.self.ReplicaID, time.Now()
	if revoke := session.revocations(now, self, view, replicas); len(revoke.Tasks) > 0 {
		return s.propose(ctx, leader, "revoke", revoke)
	}
	slots := freeSlots(self, view, replicas, session)
	assign, pending := pendingAssignments(view, slots, replicas)
	if len(assign.Tasks) > 0 {
		return s.propose(ctx, leader, "assign", assign)
	}
	if trigger != burstTick || pending {
		return nil
	}
	burst := task.Create{ExpectedLastTaskID: view.Meta.LastTaskID}
	for range task.BurstSize {
		executor, ok := pickExecutor(slots, replicas, mathrand.IntN)
		if !ok {
			break
		}
		seed := make([]byte, taskSeedBytes)
		_, _ = rand.Read(seed)
		//nolint:gosec // Random duration only spreads synthetic load.
		duration := task.MinDuration + time.Duration(mathrand.Int64N(int64(task.MaxDuration-task.MinDuration)+1))
		burst.Tasks = append(burst.Tasks,
			task.NewTask{Seed: hex.EncodeToString(seed), DurationMS: duration.Milliseconds(), Executor: executor})
	}
	if len(burst.Tasks) == 0 {
		s.logger.Info("capacity_paused", "unfinished", len(view.Outstanding))
		return nil
	}
	return s.propose(ctx, leader, "create", burst)
}

// propose submits a scheduler command in this leadership term. A refusal such
// as stale or capacity_full is a normal outcome: the next tick re-reads.
func (s *replicaService) propose(ctx context.Context, leader *load.Leadership, kind string, body any) error {
	p, err := task.Encode(kind, body)
	if err != nil {
		return err
	}
	out, err := load.ProposeRetry(ctx, leader.Propose, p, proposalRetry)
	if err != nil {
		s.logger.Info("schedule_outcome", "operation", kind, "disposition", out.Disposition, "error", err)
		return waitProposalReconcile(ctx, out, err)
	}
	s.logger.Info("schedule_committed", "operation", kind, "term", leader.Term(), "outcome", out.Result.Code)
	if out.Result.Code == "invalid_command" {
		return &task.Error{Code: out.Result.Code}
	}
	return nil
}

func pendingAssignments(view task.State, slots map[uint64]int, replicas []load.ReplicaProgress) (task.Assign, bool) {
	assign := task.Assign{}
	pending := false
	for _, t := range view.Outstanding {
		if t.State != "pending" {
			continue
		}
		pending = true
		if len(assign.Tasks) == task.MaxBatch {
			break
		}
		executor, ok := pickExecutor(slots, replicas, mathrand.IntN)
		if !ok {
			break
		}
		assign.Tasks = append(assign.Tasks, task.Assignment{TaskID: t.TaskID, ExpectedAttempt: t.Attempt,
			Executor: executor})
	}
	return assign, pending
}

// waitProposalReconcile yields before a new state read. It never resubmits the
// prior command; the application's next CAS decision is based on that fresh view.
func waitProposalReconcile(ctx context.Context, s load.Submission, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for _, terminal := range []error{fault.ErrStopped, fault.ErrInvalid, fault.ErrUnreadable,
		fault.ErrIdentityMismatch, fault.ErrUnsupported, context.Canceled} {
		if errors.Is(err, terminal) {
			return err
		}
	}
	appliedWithoutResult := s.Disposition == load.SubmissionApplied && !s.ResultAvailable
	uncertain := s.Disposition == load.SubmissionIndeterminate && fault.Retryable(err)
	refused := s.CanResubmit(err)
	if !appliedWithoutResult && !uncertain && !refused {
		return err
	}
	timer := time.NewTimer(schedulerTick)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
