//go:build functional

package observe

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMetricsReadOutcomesRequireActualDispatch(t *testing.T) {
	t.Parallel()
	pool, err := NewReadPool(2)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Stop(context.Background())
	cases := []struct {
		name     string
		dispatch bool
		err      error
		want     string
	}{
		{"connection failure", true, status.Error(codes.Unavailable, "refused"), "unreachable"},
		{"own deadline", true, context.DeadlineExceeded, "unreachable"},
		{"address resolution deadline", false, context.DeadlineExceeded, "unknown"},
		{"bad identity", true, fault.ErrIdentityMismatch, "unknown"},
		{"bad credential", true, status.Error(codes.PermissionDenied, "rejected"), "unknown"},
		{"unsupported adapter", true, status.Error(codes.Unimplemented, "old peer"), "unknown"},
		{"canceled owner", true, context.Canceled, "unknown"},
	}
	reads := make([]ReplicaRead, len(cases))
	for i, tc := range cases {
		reads[i] = ReplicaRead{Identity: metrics.ReplicaIdentity{ClusterID: "c", GroupID: "work", NodeName: tc.name, ReplicaID: uint64(i + 1), InstanceID: "process"}, Read: func(_ context.Context, a *ReadAttempt) (Probe, error) {
			if tc.dispatch {
				a.Dispatch()
			}
			return Probe{}, tc.err
		}}
	}
	probes, _ := CollectReads(t.Context(), pool, reads, ReadOptions{Parallel: 2, MaxAge: time.Second})
	for i, probe := range probes {
		if probe.Reachability != cases[i].want || probe.Dispatched != cases[i].dispatch {
			t.Fatalf("%s: %+v", cases[i].name, probe)
		}
	}
}
func TestMetricsPartialRoundKeepsUnknownUndispatchedAndOwnsLateRead(t *testing.T) {
	t.Parallel()
	pool, err := NewReadPool(1)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	defer func() {
		close(release)
		if err := pool.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	reads := []ReplicaRead{
		{Identity: metrics.ReplicaIdentity{ReplicaID: 1}, Read: func(_ context.Context, a *ReadAttempt) (Probe, error) { a.Dispatch(); <-release; return Probe{}, nil }},
		{Identity: metrics.ReplicaIdentity{ReplicaID: 2}, Read: func(context.Context, *ReadAttempt) (Probe, error) {
			t.Error("undispatched source was invoked")
			return Probe{}, nil
		}},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	probes, next := CollectReads(ctx, pool, reads, ReadOptions{Parallel: 1, MaxAge: time.Second})
	if probes[0].Reachability != "unreachable" || !probes[0].Dispatched || probes[1].Reachability != "unknown" || probes[1].Dispatched || next != 1 {
		t.Fatalf("partial round %v cursor %d", probes, next)
	}
	stop, end := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer end()
	if err := pool.Stop(stop); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatalf("late callback lost ownership: %v", err)
	}
}
