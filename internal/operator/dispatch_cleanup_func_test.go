//go:build functional

package operator

import (
	"context"
	"errors"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type cleanupCoordinatorTest struct {
	AuthorityCoordinator
	mu       sync.Mutex
	grant    topology.AuthorityGrant
	pending  []CleanupWork
	issued   []topology.AuthorityKey
	receipts chan topology.EffectReceipt
	fences   []FenceWork
	fenced   chan topology.AuthorityResource
}

func newCleanupCoordinatorTest(t *testing.T, sameHost bool) *cleanupCoordinatorTest {
	t.Helper()
	grant := dispatchTestGrant(t, "retired")
	grant.Action.Kind = topology.StepDeleteGroupState
	grant.Resources = grant.Resources[1:]
	if sameHost {
		grant.Targets[1].NodeName, grant.Targets[1].MemberID = "a", 1
		grant.Resources = grant.Resources[:1]
	}
	if err := grant.Validate(); err != nil {
		t.Fatal(err)
	}
	c := &cleanupCoordinatorTest{grant: grant, receipts: make(chan topology.EffectReceipt, 8), fenced: make(chan topology.AuthorityResource, 8)}
	for _, target := range grant.Targets {
		c.pending = append(c.pending, CleanupWork{Identity: storage.Identity{ClusterID: grant.Key.ClusterID, GroupID: grant.Key.GroupID, NodeName: target.NodeName, ReplicaID: target.ReplicaID}, MemberID: target.MemberID, Action: *grant.Action})
	}
	return c
}

func (c *cleanupCoordinatorTest) ReadyCleanup(_ context.Context, limit int) ([]CleanupWork, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.pending[:min(limit, len(c.pending))]), nil
}

func (c *cleanupCoordinatorTest) BeginCleanupEffect(ctx context.Context, id storage.Identity, action topology.AuthorizedStep) (topology.AuthorityGrant, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx.Err() != nil {
		return topology.AuthorityGrant{}, ctx.Err()
	}
	if action != *c.grant.Action || !slices.ContainsFunc(c.pending, func(w CleanupWork) bool { return w.Identity == id }) {
		return topology.AuthorityGrant{}, fault.ErrOperationAuthorization
	}
	c.issued = append(c.issued, c.grant.Key)
	return c.grant.Clone(), nil
}

func (c *cleanupCoordinatorTest) RecordEffect(_ context.Context, receipt topology.EffectReceipt) error {
	c.mu.Lock()
	c.pending = slices.DeleteFunc(c.pending, func(w CleanupWork) bool { return cleanupTarget(w) == receipt.Target })
	c.mu.Unlock()
	c.receipts <- receipt.Clone()
	return nil
}

func (c *cleanupCoordinatorTest) ReadyFences(_ context.Context, limit int) ([]FenceWork, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var work []FenceWork
	for _, item := range c.fences[:min(limit, len(c.fences))] {
		item.Grant = item.Grant.Clone()
		work = append(work, item)
	}
	return work, nil
}

func (*cleanupCoordinatorTest) ResolveFenceTarget(_ context.Context, work FenceWork) (topology.AuthorityTarget, error) {
	return work.Grant.Targets[0], nil
}

func (c *cleanupCoordinatorTest) DispatchFence(_ context.Context, _ topology.AuthorityGrant, resource topology.AuthorityResource, _ topology.AuthorityTarget) error {
	c.mu.Lock()
	c.fences = slices.DeleteFunc(c.fences, func(w FenceWork) bool { return w.Resource == resource })
	c.mu.Unlock()
	c.fenced <- resource
	return nil
}

func TestCleanupDispatchDoesNotBlockOrdinaryOrIndependentHost(t *testing.T) {
	t.Parallel()
	cleanup := newCleanupCoordinatorTest(t, false)
	coord := &dispatchCoordinatorTest{CleanupCoordinator: cleanup, leader: true, work: dispatchFixtureWork("healthy"), done: make(chan string, 1)}
	cfg := scopedControllerConfig(coord)
	cfg.Authority = cleanup
	cfg.Rules.Limits.MaxConcurrentCleanup, cfg.Rules.Limits.MaxConcurrentGroups = math.MaxInt, 1
	cfg.Interval, cfg.ProposalTimeout = 5*time.Millisecond, 50*time.Millisecond
	cfg.Groups = requestEffectsTest{call: func(ctx context.Context, grant topology.AuthorityGrant, resource topology.AuthorityResource, target topology.AuthorityTarget) (topology.EffectReceipt, error) {
		if target.NodeName == "a" {
			<-ctx.Done()
			return topology.EffectReceipt{}, ctx.Err()
		}
		return dispatchTestReceipt(grant, resource, target), nil
	}}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.Start(t.Context())
	defer e.Stop()
	select {
	case id := <-coord.done:
		if id != "healthy" {
			t.Fatal(id)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup blocked ordinary work")
	}
	select {
	case r := <-cleanup.receipts:
		if r.Target.NodeName != "b" {
			t.Fatal(r)
		}
	case <-time.After(time.Second):
		t.Fatal("dead host suppressed independent receipt")
	}
}

func waitCleanupIdle(t *testing.T, e *Controller) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		e.mu.Lock()
		run := e.run
		e.mu.Unlock()
		run.dispatch.mu.Lock()
		active := run.dispatch.cleanup
		run.dispatch.mu.Unlock()
		if active == 0 {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("cleanup worker did not release its slot")
		}
	}
}

func TestCleanupDispatchRotatesSameHostAfterTimeoutAndReusesGrant(t *testing.T) {
	t.Parallel()
	cleanup := newCleanupCoordinatorTest(t, true)
	coord := &dispatchCoordinatorTest{CleanupCoordinator: cleanup, leader: true}
	cfg := scopedControllerConfig(coord)
	clock := newFakeControllerClock()
	cfg.clock, cfg.Authority = clock, cleanup
	cfg.Rules.Limits.MaxConcurrentCleanup = 2
	cfg.Interval, cfg.MaxRetryInterval, cfg.ProposalTimeout = time.Second, time.Second, 25*time.Millisecond
	attempts := make(chan uint64, 4)
	var returning atomic.Bool
	secondReleased := make(chan struct{})
	var secondOnce sync.Once
	cfg.Groups = requestEffectsTest{call: func(ctx context.Context, grant topology.AuthorityGrant, resource topology.AuthorityResource, target topology.AuthorityTarget) (topology.EffectReceipt, error) {
		attempts <- target.ReplicaID
		if target.ReplicaID == 1 && !returning.Load() {
			<-ctx.Done()
			return topology.EffectReceipt{}, ctx.Err()
		}
		if target.ReplicaID == 2 {
			<-secondReleased
		}
		return dispatchTestReceipt(grant, resource, target), nil
	}}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.Start(t.Context())
	defer e.Stop()
	defer secondOnce.Do(func() { close(secondReleased) })
	e.Tick(t.Context())
	select {
	case id := <-attempts:
		if id != 1 {
			t.Fatal(id)
		}
	case <-time.After(time.Second):
		t.Fatal("first target not attempted")
	}
	waitCleanupIdle(t, e)
	clock.mu.Lock()
	clock.now = clock.now.Add(time.Second)
	clock.mu.Unlock()
	e.Tick(t.Context())
	select {
	case id := <-attempts:
		if id != 2 {
			t.Fatal("first failing target monopolized shared gate", id)
		}
	case <-time.After(time.Second):
		t.Fatal("second target skipped")
	}
	// Successful receipt recording wakes the background loop immediately.
	// Make the first target reachable before releasing that wake; otherwise a
	// third timeout can install backoff while the test's fake clock is frozen.
	returning.Store(true)
	secondOnce.Do(func() { close(secondReleased) })
	select {
	case receipt := <-cleanup.receipts:
		if receipt.Target.ReplicaID != 2 {
			t.Fatal(receipt)
		}
	case <-time.After(time.Second):
		t.Fatal("successful exact target not recorded")
	}
	cleanup.mu.Lock()
	issued := slices.Clone(cleanup.issued)
	cleanup.mu.Unlock()
	if len(issued) < 2 || issued[0] != issued[1] {
		t.Fatal("retry minted another grant", issued)
	}
	waitCleanupIdle(t, e)
	clock.mu.Lock()
	clock.now = clock.now.Add(time.Second)
	clock.mu.Unlock()
	e.Tick(t.Context())
	select {
	case receipt := <-cleanup.receipts:
		if receipt.Target.ReplicaID != 1 {
			t.Fatal(receipt)
		}
	case <-time.After(time.Second):
		t.Fatal("returning identity failed to resume")
	}

}

func TestCleanupDispatchAlternatesFencesAtCapacityOne(t *testing.T) {
	t.Parallel()
	cleanup := newCleanupCoordinatorTest(t, false)
	cleanup.pending = cleanup.pending[:1]
	fence := cleanup.grant.Clone()
	fence.Key.GroupID, fence.Key.Revision = "fenced", 2
	fence.Action, fence.OperatorID, fence.Purpose = nil, "", topology.AuthorityFence
	fence.Resources = []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: "fenced", NodeName: "a"}}
	fence.Targets = fence.Targets[:1]
	if err := fence.Validate(); err != nil {
		t.Fatal(err)
	}
	cleanup.fences = []FenceWork{{Grant: fence, Resource: fence.Resources[0]}}
	coord := &dispatchCoordinatorTest{CleanupCoordinator: cleanup, leader: true}
	cfg := scopedControllerConfig(coord)
	cfg.Authority = cleanup
	cfg.Rules.Limits.MaxConcurrentCleanup = 1
	cfg.Interval, cfg.ProposalTimeout = 5*time.Millisecond, 25*time.Millisecond
	cfg.Groups = requestEffectsTest{call: func(ctx context.Context, _ topology.AuthorityGrant, _ topology.AuthorityResource, _ topology.AuthorityTarget) (topology.EffectReceipt, error) {
		<-ctx.Done()
		return topology.EffectReceipt{}, ctx.Err()
	}}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.Start(t.Context())
	defer e.Stop()
	select {
	case resource := <-cleanup.fenced:
		if resource.GroupID != "fenced" {
			t.Fatal(resource)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup monopolized shared fence budget")
	}
	if _, err := e.DispatchEffect(t.Context(), fence, fence.Resources[0], fence.Targets[0]); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("fence accepted as effect", err)
	}
}
