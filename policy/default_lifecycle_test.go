//go:build !functional

package policy_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/policy"
	"github.com/asalimonov/gquorum/policy/policytest"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestDefaultLifecycleReadiness(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	for _, all := range []bool{false, true} {
		for _, fresh := range []bool{false, true} {
			for _, missing := range []bool{false, true} {
				for _, late := range []bool{false, true} {
					for _, admitted := range []bool{false, true} {
						t.Run(fmt.Sprintf("all=%v/fresh=%v/missing=%v/late=%v/admitted=%v", all, fresh, missing, late, admitted), func(t *testing.T) {
							t.Parallel()
							opts := policy.DefaultLifecycleOptions{}
							if all {
								opts.Startup, opts.FoundersTimeout = policy.AllFounders, time.Minute
							}
							p, err := policy.NewDefaultLifecycle(opts)
							if err != nil {
								t.Fatal(err)
							}
							in := policy.ReadinessInput{Now: now, StartedAt: now.Add(-time.Second), FirstBootstrap: fresh, Admitted: admitted, Recovered: admitted, SessionLive: admitted}
							if late {
								in.StartedAt = now.Add(-time.Minute)
							}
							if missing {
								in.MissingFounders = []topology.Identity{{NodeName: "late-founder", MemberID: 2}}
							}
							want := policy.Ready
							if !admitted || all && fresh && missing {
								want = policy.Waiting
							}
							if all && fresh && late && want != policy.Ready {
								want = policy.Failed
							}
							got, err := p.Readiness(context.Background(), in)
							if err != nil || got.State != want {
								t.Fatalf("readiness = %+v/%v, want %s", got, err, want)
							}
						})
					}
				}
			}
		}
	}
}

func TestLifecycleRetentionAndRemoval(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	old := storage.RetiredReplica{Identity: storage.Identity{NodeName: "n", ClusterID: "c", GroupID: "g", ReplicaID: 1}, Token: "opaque", QuarantinedAt: now.Add(-7 * 24 * time.Hour)}
	id := topology.Identity{NodeName: "n", ClusterID: "c", InstanceID: "i", MemberID: 7}
	removed := topology.SelfRemovalStatus{Fenced: true, MemberID: 7, Version: 2, AppliedIndex: 10, Reason: "approved_removal"}
	factory := func() policy.LifecyclePolicy {
		p, err := policy.NewDefaultLifecycle(policy.DefaultLifecycleOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	policytest.CheckLifecycle(t, factory, policytest.LifecycleCases{
		Retention: []policytest.RetentionCase{
			{Name: "exact deadline", Input: policy.RetentionInput{Now: now, Retired: old}, Want: policy.RetentionDecision{Collect: true, NotBefore: now}},
			{Name: "before deadline", Input: policy.RetentionInput{Now: now.Add(-time.Nanosecond), Retired: old}, Want: policy.RetentionDecision{NotBefore: now}},
			{Name: "missing retirement time", Input: policy.RetentionInput{Now: now}, ErrorCode: fault.Invalid},
		},
		Removal: []policytest.RemovalCase{
			{Name: "exact fence", Input: policy.RemovalInput{Identity: id, Removal: removed, Fenced: true}, Want: policy.RemovalDecision{Action: policy.StopNode, Reason: removed.Reason}},
			{Name: "not fenced", Input: policy.RemovalInput{Identity: id, Removal: removed}, ErrorCode: fault.IdentityMismatch},
			{Name: "different member", Input: policy.RemovalInput{Identity: topology.Identity{NodeName: "n", ClusterID: "c", InstanceID: "i", MemberID: 8}, Removal: removed, Fenced: true}, ErrorCode: fault.IdentityMismatch},
		},
	})
	forever, err := policy.NewDefaultLifecycle(policy.DefaultLifecycleOptions{RetainForever: true, Removal: policy.RemovalKeepDiagnostics})
	if err != nil {
		t.Fatal(err)
	}
	decision, err := forever.Retention(context.Background(), policy.RetentionInput{Now: now.Add(100 * 365 * 24 * time.Hour), Retired: old})
	if err != nil || decision.Collect || decision.Reason != "retain_forever" {
		t.Fatalf("forever: %+v/%v", decision, err)
	}
	removal, err := forever.Removal(context.Background(), policy.RemovalInput{Identity: id, Removal: removed, Fenced: true})
	if err != nil || removal.Action != policy.KeepDiagnostics {
		t.Fatalf("removal: %+v/%v", removal, err)
	}
}

func TestPolicyInvalidOptionsAndCancellation(t *testing.T) {
	t.Parallel()
	for _, opts := range []policy.DefaultLifecycleOptions{
		{Startup: "unknown"}, {Removal: "unknown"}, {FoundersTimeout: -1}, {Retention: -1}, {RetainForever: true, Retention: time.Second}, {FoundersTimeout: time.Second},
	} {
		if _, err := policy.NewDefaultLifecycle(opts); !errors.Is(err, fault.ErrInvalid) {
			t.Fatalf("options %+v: %v", opts, err)
		}
	}
	if _, err := policy.NewRestoreDegraded(policy.RestoreDegradedOptions{StableFor: -1}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("negative stability: %v", err)
	}
	p, _ := policy.NewDefaultLifecycle(policy.DefaultLifecycleOptions{})
	s, _ := policy.NewRestoreDegraded(policy.RestoreDegradedOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, a := p.Readiness(ctx, policy.ReadinessInput{})
	_, b := p.Retention(ctx, policy.RetentionInput{})
	_, c := p.Removal(ctx, policy.RemovalInput{})
	_, d := s.Schedule(ctx, policy.ScheduleInput{})
	for _, err := range []error{a, b, c, d} {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation: %v", err)
		}
	}
}

func TestExternalSchedulerContract(t *testing.T) {
	t.Parallel()
	factory := func() policy.Scheduler {
		p, err := policy.NewRestoreDegraded(policy.RestoreDegradedOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	policytest.CheckScheduler(t, factory, []policytest.SchedulerCase{
		{Name: "empty observations", Input: policy.ScheduleInput{}, Want: policy.ScheduleDecision{Reason: "converged"}},
		{Name: "missing observation is not a repair", Input: policy.ScheduleInput{Active: topology.Document{ClusterID: "c", Groups: []topology.Group{{ID: "g"}}}}, Want: policy.ScheduleDecision{Reason: "converged"}},
	})
}

type retainApplicationReplicas struct{ *policy.DefaultLifecycle }

func (_ retainApplicationReplicas) Retention(ctx context.Context, _ policy.RetentionInput) (policy.RetentionDecision, error) {
	if err := ctx.Err(); err != nil {
		return policy.RetentionDecision{}, err
	}
	return policy.RetentionDecision{Reason: "application_archive"}, nil
}

func ExampleDefaultLifecycle() {
	defaults, _ := policy.NewDefaultLifecycle(policy.DefaultLifecycleOptions{})
	var selected policy.LifecyclePolicy = retainApplicationReplicas{defaults}
	decision, _ := selected.Retention(context.Background(), policy.RetentionInput{})
	fmt.Println(decision.Collect, decision.Reason)
	// Output: false application_archive
}
