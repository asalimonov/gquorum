package scheduling

import (
	"context"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/policy"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
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
	assertStatus(t, r, topology.ReconciliationError, ReconcileReasonPolicyRejected)
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
	assertStatus(t, r, topology.ReconciliationError, ReconcileReasonPolicyRejected)
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
	assertStatus(t, r, topology.ReconciliationError, ReconcileReasonPolicyRejected)
	if h.commits != 0 {
		t.Fatal("local stop hold was replaced")
	}
}

func TestDisabledSchedulerStillWakesApprovedOperator(t *testing.T) {
	t.Parallel()
	h := newTestHost(t, 1, 2)
	h.executionOverride = &topology.ExecutionRecord{Status: topology.ExecutionStatus{ID: "approved", Version: 1, State: topology.ExecutionRunning}}
	r := newTestDriver(t, h, Settings{Disabled: true})
	r.policy = schedulerFunc(func(context.Context, policy.ScheduleInput) (policy.ScheduleDecision, error) {
		t.Error("disabled scheduler called")
		return policy.ScheduleDecision{}, nil
	})
	tick(t, r)
	assertStatus(t, r, topology.ReconciliationDisabled, ReconcileReasonConfigDisabled)
	if h.wakes != 1 || h.commits != 0 {
		t.Fatalf("wakes=%d commits=%d", h.wakes, h.commits)
	}
}
