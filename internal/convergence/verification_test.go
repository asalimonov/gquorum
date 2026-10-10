package convergence

import (
	"context"
	"github.com/asalimonov/gquorum/membership"
	"testing"
	"time"
)

func TestConvergenceAcknowledgedMembershipRequiresObservedExactResult(t *testing.T) {
	active, target, replica := growDocuments()
	step := Step{Seq: 1, Kind: StepAddLearner, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	f := newConvergenceFixture(active, target, []Step{step, {Seq: 2, Kind: StepWaitCaughtUp, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID}}, 0, ExecutionPending)
	original := statusFor("shard0", []uint64{1}, nil, 1)
	f.groups.statuses["shard0"] = original
	f.groups.after = func(AuthorizedStep) { f.groups.setStatus("shard0", original) }
	f.engine.Tick(context.Background())
	ex, _ := f.coord.CurrentExecution(context.Background())
	if ex.CurrentStep != 1 || ex.State.Terminal() {
		t.Fatal("advanced on unverified acknowledgement", ex)
	}
	f.groups.after = nil
	f.engine.Tick(context.Background())
	ex, _ = f.coord.CurrentExecution(context.Background())
	if ex.CurrentStep != 2 || ex.State != ExecutionRunning {
		t.Fatal("verified retry did not advance", ex)
	}
}
func TestExecutionTransitionDoesNotRegressWallClock(t *testing.T) {
	before := time.Unix(20, 0)
	now := before.Add(-time.Second)
	if !executionTime(now, before).Equal(before) {
		t.Fatal("transition regressed time")
	}
}

func TestPermanentPromotionRefusalDoesNotWaitForever(t *testing.T) {
	state, _, retry := classifyStepError(Step{Kind: StepPromoteVoter}, &membership.SafetyRefusal{Reason: "replica is not a current learner; add it as a learner first"})
	if retry || state != ExecutionFailed {
		t.Fatal("permanent promotion refusal treated as waiting")
	}
	state, _, retry = classifyStepError(Step{Kind: StepRemoveReplica}, &membership.SafetyRefusal{Reason: "post-change live voter quorum would be unavailable"})
	if !retry || state != ExecutionWaitingReplica {
		t.Fatal("transient liveness refusal treated as terminal")
	}
}
