package gquorum

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/policy"
	"github.com/asalimonov/gquorum/topology"
)

type testLifecycle struct {
	readiness func(context.Context, policy.ReadinessInput) (policy.ReadinessDecision, error)
	retention func(context.Context, policy.RetentionInput) (policy.RetentionDecision, error)
	removal   func(context.Context, policy.RemovalInput) (policy.RemovalDecision, error)
}

func (p testLifecycle) Readiness(ctx context.Context, in policy.ReadinessInput) (policy.ReadinessDecision, error) {
	return p.readiness(ctx, in)
}
func (p testLifecycle) Retention(ctx context.Context, in policy.RetentionInput) (policy.RetentionDecision, error) {
	return p.retention(ctx, in)
}
func (p testLifecycle) Removal(ctx context.Context, in policy.RemovalInput) (policy.RemovalDecision, error) {
	return p.removal(ctx, in)
}

func removalPolicyInput() policy.RemovalInput {
	return policy.RemovalInput{
		Identity: topology.Identity{NodeName: "node", ClusterID: "cluster", InstanceID: "process", MemberID: 1},
		Removal:  topology.SelfRemovalStatus{Fenced: true, MemberID: 1, Version: 2, AppliedIndex: 10, Reason: "approved exclusion"}, Fenced: true,
	}
}

func TestLifecycleSerializesAllDecisionKinds(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan string, 3), make(chan struct{})
		var active atomic.Int32
		var overlapped atomic.Bool
		call := func(kind string) {
			if active.Add(1) != 1 {
				overlapped.Store(true)
			}
			entered <- kind
			<-release
			active.Add(-1)
		}
		p := testLifecycle{
			readiness: func(context.Context, policy.ReadinessInput) (policy.ReadinessDecision, error) {
				call("readiness")
				return policy.ReadinessDecision{State: policy.Ready}, nil
			},
			retention: func(context.Context, policy.RetentionInput) (policy.RetentionDecision, error) {
				call("retention")
				return policy.RetentionDecision{Collect: true}, nil
			},
			removal: func(context.Context, policy.RemovalInput) (policy.RemovalDecision, error) {
				call("removal")
				return policy.RemovalDecision{Action: policy.KeepDiagnostics}, nil
			},
		}
		r := newLifecycleRunner(context.Background(), p, time.Minute)
		done := make(chan error, 3)
		go func() { _, err := r.Readiness(context.Background(), policy.ReadinessInput{}); done <- err }()
		if <-entered != "readiness" {
			t.Fatal("wrong first callback")
		}
		go func() { _, err := r.Retention(context.Background(), policy.RetentionInput{}); done <- err }()
		go func() { _, err := r.Removal(context.Background(), removalPolicyInput()); done <- err }()
		synctest.Wait()
		if len(entered) != 0 || active.Load() != 1 {
			t.Fatal("callbacks overlapped before release")
		}
		for i := 0; i < 3; i++ {
			release <- struct{}{}
			if i < 2 {
				<-entered
			}
		}
		for i := 0; i < 3; i++ {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		if overlapped.Load() {
			t.Fatal("policy methods overlapped")
		}
		if err := r.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestLifecycleTimeoutRetainsCallbackUntilReturn(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan context.Context, 1), make(chan struct{})
		p := testLifecycle{readiness: func(ctx context.Context, _ policy.ReadinessInput) (policy.ReadinessDecision, error) {
			entered <- ctx
			<-release // Deliberately noncooperative application; ownership must remain.
			return policy.ReadinessDecision{State: policy.Ready}, nil
		}}
		r := newLifecycleRunner(context.Background(), p, 10*time.Second)
		done := make(chan error, 1)
		go func() { _, err := r.Readiness(context.Background(), policy.ReadinessInput{}); done <- err }()
		ctx := <-entered
		time.Sleep(11 * time.Second)
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatal("callback deadline missing", ctx.Err())
		}
		if len(done) != 0 {
			t.Fatal("callback ownership released at deadline")
		}
		// Retention cannot invoke its nil callback while Readiness is still held.
		if _, err := r.Retention(context.Background(), policy.RetentionInput{}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("queued callback did not expire", err)
		}
		bounded, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.Stop(bounded); !errors.Is(err, fault.ErrShutdownTimeout) {
			t.Fatal("Stop forgot live callback", err)
		}
		if _, err := r.Readiness(context.Background(), policy.ReadinessInput{}); !errors.Is(err, fault.ErrStopped) {
			t.Fatal("callback admitted after Stop", err)
		}
		close(release)
		if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("late ready decision accepted", err)
		}
		if err := r.Stop(context.Background()); err != nil {
			t.Fatal("retry did not join original callback", err)
		}
	})
}

func TestLifecycleOwnerCancellationReachesCallbackAndJoins(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		parent, cancel := context.WithCancel(context.Background())
		p := testLifecycle{retention: func(ctx context.Context, _ policy.RetentionInput) (policy.RetentionDecision, error) {
			close(started)
			<-ctx.Done()
			return policy.RetentionDecision{Collect: true}, nil
		}}
		r := newLifecycleRunner(parent, p, time.Minute)
		done := make(chan error, 1)
		go func() { _, err := r.Retention(context.Background(), policy.RetentionInput{}); done <- err }()
		<-started
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal("canceled retention was accepted", err)
		}
		if err := r.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestLifecycleValidatesDecisionsAndOwnsInputs(t *testing.T) {
	t.Parallel()
	p := testLifecycle{
		readiness: func(_ context.Context, in policy.ReadinessInput) (policy.ReadinessDecision, error) {
			in.MissingFounders[0].MemberID = 99
			return policy.ReadinessDecision{State: "unknown"}, nil
		},
		removal: func(context.Context, policy.RemovalInput) (policy.RemovalDecision, error) {
			return policy.RemovalDecision{Action: "reactivate"}, nil
		},
	}
	r := newLifecycleRunner(context.Background(), p, time.Second)
	t.Cleanup(func() { _ = r.Stop(context.Background()) })
	in := policy.ReadinessInput{MissingFounders: []topology.Identity{{MemberID: 1}}}
	if result, err := r.Readiness(context.Background(), in); !errors.Is(err, fault.ErrInvalid) || result != (policy.ReadinessDecision{}) {
		t.Fatal("invalid readiness accepted", result, err)
	}
	if in.MissingFounders[0].MemberID != 1 {
		t.Fatal("caller policy mutated owned founder snapshot")
	}
	if result, err := r.Removal(context.Background(), removalPolicyInput()); !errors.Is(err, fault.ErrInvalid) || result != (policy.RemovalDecision{}) {
		t.Fatal("invalid removal accepted", result, err)
	}
	bad := removalPolicyInput()
	bad.Removal.MemberID++
	if _, err := r.Removal(context.Background(), bad); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("foreign removal reached custom policy", err)
	}
	bad = removalPolicyInput()
	bad.Fenced = false
	if _, err := r.Removal(context.Background(), bad); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("unfenced removal reached custom policy", err)
	}
}
