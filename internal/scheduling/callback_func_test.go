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
	schedulingapi "github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type schedulerFunc func(context.Context, schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error)

func (f schedulerFunc) Schedule(ctx context.Context, in schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
	return f(ctx, in)
}

func TestCustomSchedulerInputIsOwnedAndForeignActionRefused(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	r := newTestScheduler(t, h, Settings{})
	r.policy = schedulerFunc(func(_ context.Context, in schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
		in.Target.ClusterID = "foreign"
		in.Target.Nodes[0].Name = "changed"
		in.Status["shard0"].ConfState.Voters[0] = 99
		return schedulingapi.ScheduleDecision{Action: &schedulingapi.ScheduleAction{Kind: schedulingapi.RestoreMissingReplica, Identity: storage.Identity{ClusterID: "foreign", GroupID: "shard0", NodeName: "n3", ReplicaID: 3}}}, nil
	})
	tick(t, r)
	assertStatus(t, r, topology.SchedulingWaiting, ReconcileReasonPolicyRejected)
	if h.commits != 0 || h.machine.Target().Nodes[0].Name != "n1" || h.status["shard0"].ConfState.Voters[0] != 1 {
		t.Fatal("callback mutated host state or approved foreign identity")
	}
}

func TestCustomSchedulerDeadlineDoesNotApproveLateResult(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	r := newTestScheduler(t, h, Settings{})
	r.callbackTimeout = time.Millisecond
	r.policy = schedulerFunc(func(ctx context.Context, in schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
		<-ctx.Done()
		return schedulingapi.ScheduleDecision{Action: &schedulingapi.ScheduleAction{Kind: schedulingapi.RestoreMissingReplica, Identity: storage.Identity{ClusterID: in.Target.ClusterID, GroupID: "shard0", NodeName: "n3", ReplicaID: 3}}}, nil
	})
	tick(t, r)
	assertStatus(t, r, topology.SchedulingWaiting, ReconcileReasonPolicyRejected)
	if h.commits != 0 {
		t.Fatal("timed-out callback result was approved")
	}
}

func TestCustomSchedulerCannotOverrideLocalHold(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	r := newTestScheduler(t, h, Settings{})
	id := storage.Identity{ClusterID: "repair-test", GroupID: "shard0", NodeName: "n3", ReplicaID: 3}
	r.held = func() []storage.Identity { return []storage.Identity{id} }
	r.policy = &stubPolicy{decision: schedulingapi.ScheduleDecision{Action: &schedulingapi.ScheduleAction{Kind: schedulingapi.RestoreMissingReplica, Identity: id}}}
	tick(t, r)
	assertStatus(t, r, topology.SchedulingWaiting, ReconcileReasonPolicyRejected)
	if h.commits != 0 {
		t.Fatal("local stop hold was replaced")
	}
}

func TestCustomSchedulerCannotReplaceReplicaRecoveredDuringCallback(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	r := newTestScheduler(t, h, Settings{})
	r.policy = schedulerFunc(func(_ context.Context, in schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
		// A fresh observation appears while caller code is running. Keep the
		// previously collected map intact, as the real service returns copies.
		h.status = maps.Clone(h.status)
		observed := h.status["shard0"]
		observed.ConfState = raftpb.ConfState{Voters: []uint64{1, 2, 3}}
		h.status["shard0"] = observed
		return schedulingapi.ScheduleDecision{Action: &schedulingapi.ScheduleAction{Kind: schedulingapi.RestoreMissingReplica, Identity: storage.Identity{ClusterID: in.Target.ClusterID, GroupID: "shard0", NodeName: "n3", ReplicaID: 3}}}, nil
	})
	tick(t, r)
	if h.commits != 0 || h.machine.Latest().Version != 1 {
		t.Fatalf("stale missing-replica observation approved a replacement: commits=%d latest=%d", h.commits, h.machine.Latest().Version)
	}
	assertStatus(t, r, topology.SchedulingWaiting, ReconcileReasonPolicyRejected)
}

func TestCustomSchedulerRechecksApprovalBoundaryAfterCallback(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*testHost)
		reason string
	}{
		{"new staged intent", func(h *testHost) { h.stage(topology.OriginOperator, 1) }, "staged_conflict"},
		{"unknown health", func(h *testHost) { h.health = nil }, ReconcileReasonPolicyRejected},
		{"lost quorum", func(h *testHost) {
			h.health = map[string]topology.Health{"n1": topology.HealthDead, "n2": topology.HealthDead, "n3": topology.HealthAlive}
		}, ReconcileReasonPolicyRejected},
		{"leadership transferred", func(h *testHost) { h.leader = false }, ReconcileReasonNotLeader},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHost(t, 1, 2)
			r := newTestScheduler(t, h, Settings{})
			r.policy = schedulerFunc(func(_ context.Context, in schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
				tc.change(h)
				return schedulingapi.ScheduleDecision{Action: &schedulingapi.ScheduleAction{Kind: schedulingapi.RestoreMissingReplica, Identity: storage.Identity{ClusterID: in.Target.ClusterID, GroupID: "shard0", NodeName: "n3", ReplicaID: 3}}}, nil
			})
			tick(t, r)
			assertStatus(t, r, topology.SchedulingWaiting, tc.reason)
			if h.commits != 0 || h.machine.Target().Version != 1 || h.machine.Target().Version != 1 {
				t.Fatal("callback result crossed a changed approval boundary")
			}
		})
	}
}

func TestCustomSchedulerStallRemainsVisibleAndOwnedUntilReturn(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	r := newTestScheduler(t, h, Settings{Interval: 10 * time.Millisecond, TickTimeout: 5 * time.Millisecond})
	r.callbackTimeout = time.Millisecond
	stalled, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var once sync.Once
	r.policy = schedulerFunc(func(ctx context.Context, _ schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
		calls.Add(1)
		<-ctx.Done()
		close(stalled)
		<-release
		return schedulingapi.ScheduleDecision{}, nil
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
	r := newTestScheduler(t, h, Settings{Disabled: true})
	r.policy = schedulerFunc(func(context.Context, schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
		t.Error("disabled scheduler called")
		return schedulingapi.ScheduleDecision{}, nil
	})
	tick(t, r)
	assertStatus(t, r, topology.SchedulingDisabled, ReconcileReasonConfigDisabled)
	if h.wakes != 1 || h.commits != 0 {
		t.Fatalf("wakes=%d commits=%d", h.wakes, h.commits)
	}
}

func TestCustomPolicyCannotSeizeOwnedGroupOrClearItsCapturedOwner(t *testing.T) {
	t.Parallel()
	for _, kind := range []schedulingapi.ScheduleActionKind{schedulingapi.RestoreMissingReplica, schedulingapi.ReconfigureVoters} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			h := newScopedHost(t)
			r := scopedDriver(t, h, nil)
			row := h.read.Input.Groups["shard0"]
			row.OperationID = "exec-v2-a1"
			h.read.Input.Groups["shard0"] = row
			r.policy = schedulerFunc(func(_ context.Context, input schedulingapi.ScheduleInput) (schedulingapi.ScheduleDecision, error) {
				changed := input.Groups["shard0"]
				changed.OperationID = ""
				input.Groups["shard0"] = changed
				action := &schedulingapi.ScheduleAction{Kind: kind}
				if kind == schedulingapi.RestoreMissingReplica {
					action.Identity = storage.Identity{ClusterID: input.Target.ClusterID, GroupID: "shard0", NodeName: "n3", ReplicaID: 3}
				} else {
					action.VoterChange = &topology.VoterChange{GroupID: "shard0", Promote: &topology.ReplicaKey{NodeName: "n3", ReplicaID: 3}}
				}
				return schedulingapi.ScheduleDecision{Action: action}, nil
			})
			tick(t, r)
			assertStatus(t, r, topology.SchedulingWaiting, "group_busy")
			if len(h.attempts) != 0 || h.read.Input.Groups["shard0"].OperationID != row.OperationID {
				t.Fatal("custom policy seized an owned scope")
			}
		})
	}
}
