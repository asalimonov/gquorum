//go:build functional

package operator

import (
	"context"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

type dispatchCoordinatorTest struct {
	CleanupCoordinator
	mu     sync.Mutex
	leader bool
	work   []WorkItem
	after  string
	done   chan string
}

func (c *dispatchCoordinatorTest) IsMetaLeader() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}

func (c *dispatchCoordinatorTest) ReadyCleanup(ctx context.Context, limit int) ([]CleanupWork, error) {
	if c.CleanupCoordinator == nil {
		return nil, nil
	}
	return c.CleanupCoordinator.ReadyCleanup(ctx, limit)
}

func (c *dispatchCoordinatorTest) ReadyFences(ctx context.Context, limit int) ([]FenceWork, error) {
	if c.CleanupCoordinator == nil {
		return nil, nil
	}
	return c.CleanupCoordinator.ReadyFences(ctx, limit)
}

func (c *dispatchCoordinatorTest) ReadyBlocks(_ context.Context, limit int) ([]WorkItem, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var result []WorkItem
	start := slices.IndexFunc(c.work, func(w WorkItem) bool { return w.GroupID > c.after })
	if start < 0 {
		start = 0
	}
	for offset := 0; offset < len(c.work) && len(result) < limit; offset++ {
		w := c.work[(start+offset)%len(c.work)]
		w.Operation = w.Operation.Clone()
		result = append(result, w)
	}
	if len(result) != 0 {
		c.after = result[len(result)-1].GroupID
	}
	return result, nil
}

func (c *dispatchCoordinatorTest) RecordOperationTransition(_ context.Context, t topology.OperationTransition) error {
	c.mu.Lock()
	if t.State.Terminal() {
		c.work = slices.DeleteFunc(c.work, func(w WorkItem) bool { return w.GroupID == t.GroupID })
	}
	c.mu.Unlock()
	if t.State.Terminal() {
		c.done <- t.GroupID
	}
	return nil
}

func dispatchFixtureWork(ids ...string) []WorkItem {
	var work []WorkItem
	for _, id := range ids {
		op := topology.Operation{Status: topology.OperationStatus{ID: "exec-v2-a1", Version: 2, Attempt: 1,
			State: topology.OperationRunning, TotalBlocks: 1,
			Blocks: []topology.BlockStatus{{GroupID: id, State: topology.OperationRunning}}},
			Plan: topology.Plan{Version: 2, Blocks: []topology.PlanBlock{{GroupID: id}}}}
		work = append(work, WorkItem{Operation: op, GroupID: id})
	}
	return work
}

func TestScopedDispatchTimeoutReleasesSlotForNextGroup(t *testing.T) {
	t.Parallel()
	coord := &dispatchCoordinatorTest{leader: true, work: dispatchFixtureWork("a", "b"), done: make(chan string, 2)}
	cfg := scopedControllerConfig(coord)
	cfg.Rules.Limits.MaxConcurrentGroups = 1
	cfg.Interval, cfg.ProposalTimeout = 5*time.Millisecond, 50*time.Millisecond
	entered := make(chan struct{}, 1)
	cfg.ScopedPlanValidator = func(ctx context.Context, _ topology.Operation, group string) error {
		if group == "a" {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	e.Start(ctx)
	defer e.Stop()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case id := <-coord.done:
		if id != "b" {
			t.Fatal("unavailable group falsely completed", id)
		}
	case <-ctx.Done():
		t.Fatal("timed out group monopolized sole slot", ctx.Err())
	}
}

func TestScopedDispatchStopClosesAdmissionAndJoinsLateWorker(t *testing.T) {
	t.Parallel()
	coord := &dispatchCoordinatorTest{leader: true, work: dispatchFixtureWork("a", "b"), done: make(chan string, 2)}
	cfg := scopedControllerConfig(coord)
	cfg.Rules.Limits.MaxConcurrentGroups = 2
	cfg.Interval, cfg.ProposalTimeout = time.Millisecond, 25*time.Millisecond
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	cfg.ScopedPlanValidator = func(_ context.Context, _ topology.Operation, group string) error {
		if group == "a" {
			once.Do(func() { close(entered) })
			<-release
		}
		return nil
	}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	e.Start(ctx)
	defer e.Stop()
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case id := <-coord.done:
		if id != "b" {
			t.Fatal(id)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	e.mu.Lock()
	run := e.run
	e.mu.Unlock()
	// Closing admission uses the same mutex as worker Add. Try concurrent late
	// ticks while Stop is joining an intentionally uncooperative local callback.
	run.dispatch.close()
	stopped := make(chan struct{})
	go func() { e.Stop(); close(stopped) }()
	var callers sync.WaitGroup
	for range 20 {
		callers.Go(func() { e.Tick(ctx); e.Start(ctx) })
	}
	callers.Wait()
	select {
	case <-stopped:
		t.Fatal("Stop released a still-running worker")
	default:
	}
	run.dispatch.mu.Lock()
	if !run.dispatch.closed || len(run.dispatch.groups) != 1 {
		t.Error("late admission", run.dispatch.groups)
	}
	run.dispatch.mu.Unlock()
	releaseOnce.Do(func() { close(release) })
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestScopedDispatchHugeLimitAllocatesOnlyActualWork(t *testing.T) {
	t.Parallel()
	coord := &dispatchCoordinatorTest{leader: true, work: dispatchFixtureWork("a"), done: make(chan string, 1)}
	cfg := scopedControllerConfig(coord)
	cfg.Rules.Limits.MaxConcurrentGroups = math.MaxInt
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	e.Start(ctx)
	defer e.Stop()
	select {
	case <-coord.done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
