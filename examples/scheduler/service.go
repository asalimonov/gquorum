package main

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/asalimonov/gquorum/examples/scheduler/task"
	"github.com/asalimonov/gquorum/load"
)

// replicaService is the business side of one ready local replica: a leader
// scheduler and a follower executor. Node starts Run after replay and
// authorization, and joins it before closing SQLite.
type replicaService struct {
	runtime  *runtime
	self     task.Executor
	group    *load.Group
	executor *executor
	logger   *slog.Logger
	replicas atomic.Pointer[[]load.ReplicaProgress] // latest leader observation
}

func newReplicaService(r *runtime, spec load.ReplicaSpec) *replicaService {
	self := task.Executor{ReplicaID: spec.Identity.ReplicaID, NodeName: spec.Identity.NodeName}
	logger := r.logger.With("node", self.NodeName, "group", groupID, "replica", self.ReplicaID)
	return &replicaService{runtime: r, self: self, logger: logger, executor: newExecutor(self, logger)}
}

// Run executes on followers and schedules while leader until the replica stops.
func (s *replicaService) Run(parent context.Context, group *load.Group) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	s.group = group
	s.runtime.service.Store(s)
	defer s.runtime.service.CompareAndSwap(s, nil)
	failures := make(chan error, 2)
	var workers []func() error
	workers = append(workers, func() error { return s.executor.Run(ctx, group) })
	workers = append(workers, func() error { return group.RunWhileLeader(ctx, s.runScheduler) })
	done := make(chan struct{}, len(workers))
	for _, work := range workers {
		go func() {
			defer func() { done <- struct{}{} }()
			if err := work(); err != nil && ctx.Err() == nil {
				failures <- err
			}
		}()
	}
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-failures:
	}
	cancel()
	for range workers {
		<-done
	}
	return err
}

// serviceStatus is the observational /status payload for this replica.
type serviceStatus struct {
	Observation load.Observation       `json:"observation"`
	Executor    executorStatus         `json:"executor"`
	Replicas    []load.ReplicaProgress `json:"replicas,omitempty"`
}

func (s *replicaService) Status(ctx context.Context) serviceStatus {
	out := serviceStatus{Executor: s.executor.Status()}
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if watch, err := s.group.Watch(watchCtx); err == nil {
		select {
		case out.Observation = <-watch:
		case <-ctx.Done():
		}
	}
	if replicas := s.replicas.Load(); replicas != nil {
		out.Replicas = *replicas
	}
	return out
}
