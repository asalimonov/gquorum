package main

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/examples/scheduler/task"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
)

// executorStatus is an observational snapshot for /status.
type executorStatus struct {
	Active *task.Key  `json:"active"`
	Phase  string     `json:"phase"` // idle, starting, calculating, finishing
	Queued []task.Key `json:"queued"`
}

// executor pulls its own committed assignments and runs one at a time. Its
// queue is the committed assigned rows, so losing the process loses nothing.
type executor struct {
	self   task.Executor
	logger *slog.Logger
	mu     sync.Mutex
	status executorStatus
	seen   map[task.Key]bool // attempts already logged as received
}

func newExecutor(self task.Executor, logger *slog.Logger) *executor {
	return &executor{self: self, logger: logger, status: executorStatus{Phase: "idle",
		Queued: []task.Key{}}, seen: map[task.Key]bool{}}
}

func (e *executor) Status() executorStatus {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := e.status
	out.Queued = slices.Clone(e.status.Queued)
	if out.Active != nil {
		key := *out.Active
		out.Active = &key
	}
	return out
}

func (e *executor) setPhase(active *task.Key, phase string) {
	e.mu.Lock()
	e.status.Active, e.status.Phase = active, phase
	e.mu.Unlock()
}

func (e *executor) setQueued(queued []task.Key) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, key := range queued {
		if !e.seen[key] {
			e.seen[key] = true
			e.logger.Info("received", "task", key.TaskID, "attempt", key.Attempt)
		}
	}
	e.status.Queued = queued
}

// nextTask picks this replica's own running attempt first (resuming after a
// restart), then its lowest assigned task. It also returns the keys still
// waiting behind it.
func nextTask(view task.State, self uint64) (*task.Task, []task.Key) {
	var next *task.Task
	var assigned []int
	for i, t := range view.Outstanding {
		if t.Owner().ReplicaID != self {
			continue
		}
		switch t.State {
		case "running":
			if next == nil || next.State != "running" {
				next = &view.Outstanding[i]
			}
		case "assigned":
			assigned = append(assigned, i)
		default: // Other states do not represent executable work.
		}
	}
	queued := []task.Key{}
	for _, i := range assigned {
		if next == nil {
			next = &view.Outstanding[i]
			continue
		}
		queued = append(queued, view.Outstanding[i].Key())
	}
	return next, queued
}

// Run wakes on every newly applied index and drains this replica's committed
// work while it is a ready voting follower. A promoted executor starts nothing
// new; a task already running continues under the replica's Run context.
//
// TODO(potential-improvement): a local, non-linearizable read would avoid one
// quorum round per wake-up; Group.Read is used because it is the public API.
func (e *executor) Run(ctx context.Context, group *load.Group) error {
	watch, err := group.Watch(ctx)
	if err != nil {
		return err
	}
	var seen uint64
	for {
		var o load.Observation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case next, open := <-watch:
			if !open {
				return ctx.Err()
			}
			o = next
		}
		if !o.Ready || !o.IsVoter || o.IsLeader {
			e.setQueued([]task.Key{})
			continue
		}
		if o.AppliedIndex == seen {
			continue
		}
		seen = o.AppliedIndex
		if err := e.drain(ctx, group); err != nil {
			return err
		}
	}
}

func (e *executor) drain(ctx context.Context, group *load.Group) error {
	for {
		view, err := fault.Retry(ctx, retry, func(call context.Context) (task.State, error) {
			return task.ReadState(call, group)
		})
		if err != nil {
			return err
		}
		next, queued := nextTask(view, e.self.ReplicaID)
		e.setQueued(queued)
		if next == nil || group.IsLeader() {
			return nil
		}
		if err := e.runTask(ctx, group, *next); err != nil {
			if ctx.Err() != nil || !fault.Retryable(err) {
				return err
			}
			// Re-read committed state before choosing another guarded command.
			e.logger.Info("executor_waiting", "error", err)
		}
	}
}

func (e *executor) runTask(ctx context.Context, group *load.Group, t task.Task) error {
	key := t.Key()
	logger := e.logger.With("task", key.TaskID, "attempt", key.Attempt)
	defer e.setPhase(nil, "idle")
	e.setPhase(&key, "starting")
	out, err := submit(ctx, group, "start", task.Start{Key: key, Executor: e.self})
	if err != nil {
		return err
	}
	switch out.Code {
	case "ok", "already_running":
	case "succeeded", "failed":
		logger.Info("finished", "outcome", out.Code, "recovered", true)
		return nil
	case "stale", "task_retired", "not_found":
		logger.Info("attempt_abandoned", "reason", out.Code)
		return nil
	default:
		return &task.Error{Code: out.Code}
	}
	e.setPhase(&key, "calculating")
	logger.Info("started", "term", group.Status().Term)
	value, err := calculate(ctx, t.Seed, time.Duration(t.DurationMS)*time.Millisecond)
	if err != nil {
		return err
	}
	logger.Info("executed", "result", value)
	e.setPhase(&key, "finishing")
	out, err = submit(ctx, group, "finish", task.Finish{Key: key, Executor: e.self, Value: value})
	if err != nil {
		return err
	}
	switch out.Code {
	case "succeeded", "failed":
		logger.Info("quorum_committed", "outcome", out.Code)
		logger.Info("finished", "outcome", out.Code)
	case "stale", "task_retired", "not_found":
		logger.Info("attempt_abandoned", "reason", out.Code)
	default:
		return &task.Error{Code: out.Code}
	}
	return nil
}

// submit retries proven refusals; task state owns the start/finish guards.
func submit(ctx context.Context, group *load.Group, kind string, body any) (load.Result, error) {
	call, cancel := context.WithTimeout(ctx, proposalTimeout)
	defer cancel()
	p, err := task.Encode(kind, body)
	if err != nil {
		return load.Result{}, err
	}
	submission, err := load.ProposeRetry(call, group.Propose, p, proposalRetry)
	return submission.Result, err
}
