//go:build functional

package scheduling

import (
	"context"
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/policy"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type schedulerFunc func(context.Context, policy.ScheduleInput) (policy.ScheduleDecision, error)

func (f schedulerFunc) Schedule(ctx context.Context, in policy.ScheduleInput) (policy.ScheduleDecision, error) {
	return f(ctx, in)
}

func TestCustomSchedulerInputIsOwnedAndForeignActionRefused(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	r := newTestDriver(t, h, Settings{})
	r.policy = schedulerFunc(func(ctx context.Context, in policy.ScheduleInput) (policy.ScheduleDecision, error) {
		in.Active.ClusterID = "foreign"
		in.Active.Nodes[0].Name = "changed"
		in.Status["shard0"].ConfState.Voters[0] = 99
		return policy.ScheduleDecision{Action: &policy.ScheduleAction{Kind: policy.RestoreMissingReplica, Identity: storage.Identity{ClusterID: "foreign", GroupID: "shard0", NodeName: "n3", ReplicaID: 3}}}, nil
	})
	tick(t, r)
	assertStatus(t, r, topology.SchedulingError, ReconcileReasonPolicyRejected)
	if h.commits != 0 || h.machine.Active().Nodes[0].Name != "n1" || h.status["shard0"].ConfState.Voters[0] != 1 {
		t.Fatal("callback mutated host state or approved foreign identity")
	}
}

func TestCustomSchedulerDeadlineDoesNotApproveLateResult(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	r := newTestDriver(t, h, Settings{})
	r.callbackTimeout = time.Millisecond
	r.policy = schedulerFunc(func(ctx context.Context, in policy.ScheduleInput) (policy.ScheduleDecision, error) {
		<-ctx.Done()
		return policy.ScheduleDecision{Action: &policy.ScheduleAction{Kind: policy.RestoreMissingReplica, Identity: storage.Identity{ClusterID: in.Active.ClusterID, GroupID: "shard0", NodeName: "n3", ReplicaID: 3}}}, nil
	})
	tick(t, r)
	assertStatus(t, r, topology.SchedulingError, ReconcileReasonPolicyRejected)
	if h.commits != 0 {
		t.Fatal("timed-out callback result was approved")
	}
}

func TestCustomSchedulerCannotOverrideLocalHold(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	r := newTestDriver(t, h, Settings{})
	id := storage.Identity{ClusterID: "repair-test", GroupID: "shard0", NodeName: "n3", ReplicaID: 3}
	r.held = func() []storage.Identity { return []storage.Identity{id} }
	r.policy = &stubPolicy{decision: policy.ScheduleDecision{Action: &policy.ScheduleAction{Kind: policy.RestoreMissingReplica, Identity: id}}}
	tick(t, r)
	assertStatus(t, r, topology.SchedulingError, ReconcileReasonPolicyRejected)
	if h.commits != 0 {
		t.Fatal("local stop hold was replaced")
	}
}

func TestCustomSchedulerCannotReplaceReplicaRecoveredDuringCallback(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	r := newTestDriver(t, h, Settings{})
	r.policy = schedulerFunc(func(_ context.Context, in policy.ScheduleInput) (policy.ScheduleDecision, error) {
		// A fresh observation appears while caller code is running. Keep the
		// previously collected map intact, as the real service returns copies.
		h.status = maps.Clone(h.status)
		observed := h.status["shard0"]
		observed.ConfState = raftpb.ConfState{Voters: []uint64{1, 2, 3}}
		h.status["shard0"] = observed
		return policy.ScheduleDecision{Action: &policy.ScheduleAction{Kind: policy.RestoreMissingReplica, Identity: storage.Identity{ClusterID: in.Active.ClusterID, GroupID: "shard0", NodeName: "n3", ReplicaID: 3}}}, nil
	})
	tick(t, r)
	if h.commits != 0 || h.machine.Latest().Version != 1 {
		t.Fatalf("stale missing-replica observation approved a replacement: commits=%d latest=%d", h.commits, h.machine.Latest().Version)
	}
	assertStatus(t, r, topology.SchedulingError, ReconcileReasonPolicyRejected)
}

func TestCustomSchedulerRechecksApprovalBoundaryAfterCallback(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*testHost)
		reason string
	}{
		{"new staged intent", func(h *testHost) { h.stage(topology.OriginOperator, 1) }, ReconcileReasonCASConflict},
		{"unknown health", func(h *testHost) { h.health = nil }, ReconcileReasonPolicyRejected},
		{"lost quorum", func(h *testHost) {
			h.health = map[string]topology.Health{"n1": topology.HealthDead, "n2": topology.HealthDead, "n3": topology.HealthAlive}
		}, ReconcileReasonPolicyRejected},
		{"leadership transferred", func(h *testHost) { h.leader = false }, ReconcileReasonPolicyRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHost(t, 1, 2)
			r := newTestDriver(t, h, Settings{})
			r.policy = schedulerFunc(func(_ context.Context, in policy.ScheduleInput) (policy.ScheduleDecision, error) {
				tc.change(h)
				return policy.ScheduleDecision{Action: &policy.ScheduleAction{Kind: policy.RestoreMissingReplica, Identity: storage.Identity{ClusterID: in.Active.ClusterID, GroupID: "shard0", NodeName: "n3", ReplicaID: 3}}}, nil
			})
			tick(t, r)
			assertStatus(t, r, topology.SchedulingError, tc.reason)
			if h.commits != 0 || h.machine.Target().Version != 1 || h.machine.Active().Version != 1 {
				t.Fatal("callback result crossed a changed approval boundary")
			}
		})
	}
}

func TestCustomSchedulerStallRemainsVisibleAndOwnedUntilReturn(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	r := newTestDriver(t, h, Settings{Interval: 10 * time.Millisecond, TickTimeout: 5 * time.Millisecond})
	r.callbackTimeout = time.Millisecond
	stalled, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var once sync.Once
	r.policy = schedulerFunc(func(ctx context.Context, _ policy.ScheduleInput) (policy.ScheduleDecision, error) {
		calls.Add(1)
		<-ctx.Done()
		close(stalled)
		<-release
		return policy.ScheduleDecision{}, nil
	})
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stalled:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not enter callback")
	}
	assertStatus(t, r, topology.SchedulingError, "policy_stalled")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := r.Stop(ctx); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatalf("Stop abandoned callback: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("scheduler overlapped retries of a stalled callback")
	}
	once.Do(func() { close(release) })
	joined, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	if err := r.Stop(joined); err != nil {
		t.Fatal(err)
	}
	if h.commits != 0 {
		t.Fatal("stalled callback approved a decision")
	}
}

func TestDisabledSchedulerStillWakesApprovedOperator(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	h.operatorOverride = &topology.Operator{Status: topology.OperatorStatus{ID: "approved", Version: 1, State: topology.OperatorRunning}}
	r := newTestDriver(t, h, Settings{Disabled: true})
	r.policy = schedulerFunc(func(context.Context, policy.ScheduleInput) (policy.ScheduleDecision, error) {
		t.Error("disabled scheduler called")
		return policy.ScheduleDecision{}, nil
	})
	tick(t, r)
	assertStatus(t, r, topology.SchedulingDisabled, ReconcileReasonConfigDisabled)
	if h.wakes != 1 || h.commits != 0 {
		t.Fatalf("wakes=%d commits=%d", h.wakes, h.commits)
	}
}
