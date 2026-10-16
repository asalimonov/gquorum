//go:build functional

package operator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type requestEffectsTest struct {
	GroupEffects
	call func(context.Context, topology.AuthorityGrant, topology.AuthorityResource, topology.AuthorityTarget) (topology.EffectReceipt, error)
}

func (f requestEffectsTest) DispatchEffect(ctx context.Context, g topology.AuthorityGrant, r topology.AuthorityResource, target topology.AuthorityTarget) (topology.EffectReceipt, error) {
	return f.call(ctx, g, r, target)
}

type requestAuthorityTest struct {
	AuthorityCoordinator
	recorded chan topology.EffectReceipt
}

func (a requestAuthorityTest) RecordEffect(_ context.Context, receipt topology.EffectReceipt) error {
	a.recorded <- receipt
	return nil
}

func dispatchTestGrant(t *testing.T, group string) topology.AuthorityGrant {
	t.Helper()
	action := topology.AuthorizedStep{OperatorID: topology.MakeOperatorID(2, 1), Version: 2, Attempt: 1,
		StepSeq: 1, Kind: topology.StepCreateFoundingGroup, GroupID: group}
	g := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "cluster", GroupID: group, Revision: 1},
		Purpose: topology.AuthorityEffect, OperatorID: action.OperatorID, Action: &action,
		Resources: []topology.AuthorityResource{{Kind: topology.AuthorityGroup, GroupID: group},
			{Kind: topology.AuthorityHost, GroupID: group, NodeName: "a"},
			{Kind: topology.AuthorityHost, GroupID: group, NodeName: "b"}},
		Targets: []topology.AuthorityTarget{{NodeName: "a", MemberID: 1, ReplicaID: 1},
			{NodeName: "b", MemberID: 2, ReplicaID: 2}}}
	if err := g.Validate(); err != nil {
		t.Fatal(err)
	}
	return g
}

func dispatchTestReceipt(g topology.AuthorityGrant, r topology.AuthorityResource, target topology.AuthorityTarget) topology.EffectReceipt {
	for _, receipt := range controllerReceipts(g) {
		if receipt.Resource == r && (r.Kind == topology.AuthorityGroup || receipt.Target == target) {
			return receipt
		}
	}
	panic("missing test receipt")
}

func TestScopedRequestSerializesHostAndNativeKeysAcrossBudgets(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "host", true: "native"}[native], func(t *testing.T) {
			t.Parallel()
			coord := &dispatchCoordinatorTest{leader: true}
			cfg := scopedControllerConfig(coord)
			cfg.Rules.Limits.MaxConcurrentGroups, cfg.Rules.Limits.MaxConcurrentCleanup = 2, 1
			cfg.ProposalTimeout = time.Second
			recorded := make(chan topology.EffectReceipt, 4)
			cfg.Authority = requestAuthorityTest{recorded: recorded}
			entered, release := make(chan struct{}), make(chan struct{})
			cfg.Groups = requestEffectsTest{call: func(ctx context.Context, g topology.AuthorityGrant, r topology.AuthorityResource, target topology.AuthorityTarget) (topology.EffectReceipt, error) {
				if g.Key.GroupID == "work" {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return topology.EffectReceipt{}, ctx.Err()
					}
				}
				return dispatchTestReceipt(g, r, target), nil
			}}
			e, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			e.Start(t.Context())
			defer e.Stop()
			defer close(release)
			grant := dispatchTestGrant(t, "work")
			resource := grant.Resources[1]
			if native {
				resource = grant.Resources[0]
			}
			done := make(chan error, 1)
			go func() { _, err := e.DispatchEffect(t.Context(), grant, resource, grant.Targets[0]); done <- err }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("no request")
			}
			// Both another grant on this host and the same grant on a new native
			// leader must be refused before calling the effect seam.
			request := effectDispatch{grant: grant, resource: resource, target: grant.Targets[0], cleanup: true}
			if native {
				request.target = grant.Targets[1]
			} else {
				request.grant = grant.Clone()
				request.grant.Key.Revision++
			}
			if _, err := e.dispatchRequest(t.Context(), request); !errors.Is(err, fault.ErrUnavailable) {
				t.Fatal(err)
			}
			// Another group on the same node is independent and may use cleanup's budget.
			other := dispatchTestGrant(t, "other")
			if _, err := e.dispatchRequest(t.Context(), effectDispatch{grant: other, resource: other.Resources[1], target: other.Targets[0], cleanup: true}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestScopedRequestTimeoutReleasesRPCAndBacksOffHost(t *testing.T) {
	t.Parallel()
	coord := &dispatchCoordinatorTest{leader: true}
	cfg := scopedControllerConfig(coord)
	cfg.Rules.Limits.MaxConcurrentGroups = 1
	cfg.Interval, cfg.MaxRetryInterval, cfg.ProposalTimeout = time.Second, time.Second, 25*time.Millisecond
	cfg.Authority = requestAuthorityTest{recorded: make(chan topology.EffectReceipt, 2)}
	cfg.Groups = requestEffectsTest{call: func(ctx context.Context, g topology.AuthorityGrant, r topology.AuthorityResource, target topology.AuthorityTarget) (topology.EffectReceipt, error) {
		if target.NodeName == "a" {
			<-ctx.Done()
			return topology.EffectReceipt{}, ctx.Err()
		}
		return dispatchTestReceipt(g, r, target), nil
	}}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.Start(t.Context())
	defer e.Stop()
	grant := dispatchTestGrant(t, "work")
	if _, err := e.DispatchEffect(t.Context(), grant, grant.Resources[1], grant.Targets[0]); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if _, err := e.DispatchEffect(t.Context(), grant, grant.Resources[1], grant.Targets[0]); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("missing backoff", err)
	}
	if _, err := e.DispatchEffect(t.Context(), grant, grant.Resources[2], grant.Targets[1]); err != nil {
		t.Fatal("healthy host blocked", err)
	}
	e.mu.Lock()
	d := e.run.dispatch
	e.mu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.requests) != 0 || len(d.hosts) != 0 || d.ordinary != 0 || len(d.backoff) != 1 {
		t.Fatal("leaked RPC accounting", d)
	}
}

func TestScopedRequestRecordsLateEvidenceButRefusesLeadershipLoss(t *testing.T) {
	t.Parallel()
	coord := &dispatchCoordinatorTest{leader: true}
	cfg := scopedControllerConfig(coord)
	recorded := make(chan topology.EffectReceipt, 1)
	cfg.Authority = requestAuthorityTest{recorded: recorded}
	cfg.Groups = requestEffectsTest{call: func(_ context.Context, g topology.AuthorityGrant, r topology.AuthorityResource, target topology.AuthorityTarget) (topology.EffectReceipt, error) {
		coord.mu.Lock()
		coord.leader = false
		coord.mu.Unlock()
		return dispatchTestReceipt(g, r, target), nil
	}}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.Start(t.Context())
	defer e.Stop()
	grant := dispatchTestGrant(t, "work")
	if _, err := e.DispatchEffect(t.Context(), grant, grant.Resources[1], grant.Targets[0]); !errors.Is(err, fault.ErrNotLeader) {
		t.Fatal(err)
	}
	select {
	case got := <-recorded:
		if err := got.Validate(grant); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("late proof discarded")
	}
}

func TestScopedRequestStopJoinsAdmittedCallAndSealsNewRequests(t *testing.T) {
	t.Parallel()
	coord := &dispatchCoordinatorTest{leader: true}
	cfg := scopedControllerConfig(coord)
	entered, release := make(chan struct{}), make(chan struct{})
	cfg.Groups = requestEffectsTest{call: func(context.Context, topology.AuthorityGrant, topology.AuthorityResource, topology.AuthorityTarget) (topology.EffectReceipt, error) {
		close(entered)
		<-release
		return topology.EffectReceipt{}, context.Canceled
	}}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.Start(t.Context())
	defer e.Stop()
	var once sync.Once
	defer once.Do(func() { close(release) })
	grant := dispatchTestGrant(t, "work")
	done := make(chan error, 1)
	go func() {
		_, err := e.DispatchEffect(t.Context(), grant, grant.Resources[1], grant.Targets[0])
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no admitted request")
	}
	e.mu.Lock()
	run := e.run
	e.mu.Unlock()
	run.dispatch.close() // Barrier: Stop cannot admit new work past this point.
	stopped := make(chan struct{})
	go func() { e.Stop(); close(stopped) }()
	var attempts sync.WaitGroup
	for range 20 {
		attempts.Go(func() {
			e.Start(t.Context())
			if _, err := e.DispatchEffect(t.Context(), grant, grant.Resources[2], grant.Targets[1]); !errors.Is(err, fault.ErrStopped) {
				t.Errorf("request crossed closed admission: %v", err)
			}
		})
	}
	attempts.Wait()
	select {
	case <-stopped:
		t.Fatal("Stop released admitted owner before return")
	default:
	}
	once.Do(func() { close(release) })
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not join")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestScopedDispatchBackoffIsBoundedAndPrunesIdleHosts(t *testing.T) {
	t.Parallel()
	clock := newFakeControllerClock()
	cfg := scopedControllerConfig(&dispatchCoordinatorTest{leader: true})
	cfg.clock = clock
	cfg.Rules.Limits.MaxConcurrentGroups, cfg.Rules.Limits.MaxConcurrentCleanup = 2, 3
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	d := &controllerDispatch{}
	for i := range 100 {
		host := dispatchHost{group: "work", node: fmt.Sprint(i)}
		e.backoffDispatch(d, host)
		clock.mu.Lock()
		clock.now = clock.now.Add(time.Millisecond)
		clock.mu.Unlock()
		if len(d.backoff) > 3 {
			t.Fatal("unbounded retry memory", len(d.backoff))
		}
	}
	if _, found := d.backoff[dispatchHost{group: "work", node: "99"}]; !found {
		t.Fatal("newest backoff evicted")
	}
	d.pruneBackoff(clock.Now().Add(e.maxRetry), e.maxRetry)
	if len(d.backoff) != 0 {
		t.Fatal("idle host entries retained", d.backoff)
	}
}
