//go:build !functional

package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/lifecycle/lifecycletest"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestDefaultHooksReadiness(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	for _, all := range []bool{false, true} {
		for _, fresh := range []bool{false, true} {
			for _, missing := range []bool{false, true} {
				for _, late := range []bool{false, true} {
					for _, admitted := range []bool{false, true} {
						t.Run(fmt.Sprintf("all=%v/fresh=%v/missing=%v/late=%v/admitted=%v", all, fresh, missing, late, admitted), func(t *testing.T) {
							t.Parallel()
							opts := lifecycle.DefaultOptions{}
							if all {
								opts.Startup, opts.FoundersTimeout = lifecycle.AllFounders, time.Minute
							}
							p, err := lifecycle.NewDefaultHooks(opts)
							if err != nil {
								t.Fatal(err)
							}
							in := lifecycle.ReadinessInput{Now: now, StartedAt: now.Add(-time.Second), FirstBootstrap: fresh, Admitted: admitted, Recovered: admitted, SessionLive: admitted}
							if late {
								in.StartedAt = now.Add(-time.Minute)
							}
							if missing {
								in.MissingFounders = []topology.Identity{{NodeName: "late-founder", MemberID: 2}}
							}
							want := lifecycle.Ready
							if !admitted || all && fresh && missing {
								want = lifecycle.Waiting
							}
							if all && fresh && late && want != lifecycle.Ready {
								want = lifecycle.Failed
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
	factory := func() lifecycle.Hooks {
		p, err := lifecycle.NewDefaultHooks(lifecycle.DefaultOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	lifecycletest.CheckHooks(t, factory, lifecycletest.HooksCases{
		Retention: []lifecycletest.RetentionCase{
			{Name: "exact deadline", Input: lifecycle.RetentionInput{Now: now, Retired: old}, Want: lifecycle.RetentionDecision{Collect: true, NotBefore: now}},
			{Name: "before deadline", Input: lifecycle.RetentionInput{Now: now.Add(-time.Nanosecond), Retired: old}, Want: lifecycle.RetentionDecision{NotBefore: now}},
			{Name: "missing retirement time", Input: lifecycle.RetentionInput{Now: now}, ErrorCode: fault.Invalid},
		},
		Removal: []lifecycletest.RemovalCase{
			{Name: "exact fence", Input: lifecycle.RemovalInput{Identity: id, Removal: removed, Fenced: true}, Want: lifecycle.RemovalDecision{Action: lifecycle.StopNode, Reason: removed.Reason}},
			{Name: "not fenced", Input: lifecycle.RemovalInput{Identity: id, Removal: removed}, ErrorCode: fault.IdentityMismatch},
			{Name: "different member", Input: lifecycle.RemovalInput{Identity: topology.Identity{NodeName: "n", ClusterID: "c", InstanceID: "i", MemberID: 8}, Removal: removed, Fenced: true}, ErrorCode: fault.IdentityMismatch},
		},
	})
	forever, err := lifecycle.NewDefaultHooks(lifecycle.DefaultOptions{RetainForever: true, Removal: lifecycle.RemovalKeepDiagnostics})
	if err != nil {
		t.Fatal(err)
	}
	decision, err := forever.Retention(context.Background(), lifecycle.RetentionInput{Now: now.Add(100 * 365 * 24 * time.Hour), Retired: old})
	if err != nil || decision.Collect || decision.Reason != "retain_forever" {
		t.Fatalf("forever: %+v/%v", decision, err)
	}
	removal, err := forever.Removal(context.Background(), lifecycle.RemovalInput{Identity: id, Removal: removed, Fenced: true})
	if err != nil || removal.Action != lifecycle.KeepDiagnostics {
		t.Fatalf("removal: %+v/%v", removal, err)
	}
}

func TestHooksInvalidOptionsAndCancellation(t *testing.T) {
	t.Parallel()
	for _, opts := range []lifecycle.DefaultOptions{
		{Startup: "unknown"}, {Removal: "unknown"}, {FoundersTimeout: -1}, {Retention: -1}, {RetainForever: true, Retention: time.Second}, {FoundersTimeout: time.Second},
	} {
		if _, err := lifecycle.NewDefaultHooks(opts); !errors.Is(err, fault.ErrInvalid) {
			t.Fatalf("options %+v: %v", opts, err)
		}
	}
	p, _ := lifecycle.NewDefaultHooks(lifecycle.DefaultOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, a := p.Readiness(ctx, lifecycle.ReadinessInput{})
	_, b := p.Retention(ctx, lifecycle.RetentionInput{})
	_, c := p.Removal(ctx, lifecycle.RemovalInput{})
	for _, err := range []error{a, b, c} {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation: %v", err)
		}
	}
}

type retainApplicationReplicas struct{ *lifecycle.DefaultHooks }

func (_ retainApplicationReplicas) Retention(ctx context.Context, _ lifecycle.RetentionInput) (lifecycle.RetentionDecision, error) {
	if err := ctx.Err(); err != nil {
		return lifecycle.RetentionDecision{}, err
	}
	return lifecycle.RetentionDecision{Reason: "application_archive"}, nil
}

func ExampleDefaultHooks() {
	defaults, _ := lifecycle.NewDefaultHooks(lifecycle.DefaultOptions{})
	var selected lifecycle.Hooks = retainApplicationReplicas{defaults}
	decision, _ := selected.Retention(context.Background(), lifecycle.RetentionInput{})
	fmt.Println(decision.Collect, decision.Reason)
	// Output: false application_archive
}
