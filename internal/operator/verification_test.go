package operator

import (
	"context"
	"github.com/asalimonov/gquorum/membership"
	"reflect"
	"testing"
	"time"
)

func TestControllerAcknowledgedMembershipRequiresObservedExactResult(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	active, target, replica := growDocuments()
	step := Step{Seq: 1, Kind: StepAddLearner, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	f := newControllerFixture(active, target, []Step{step, {Seq: 2, Kind: StepWaitCaughtUp, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID}}, 0, OperatorPending)
	original := statusFor("shard0", []uint64{1}, nil, 1)
	f.groups.statuses["shard0"] = original
	f.groups.after = func(AuthorizedStep) { f.groups.setStatus("shard0", original) }
	f.engine.Tick(context.Background())
	ex, _ := f.coord.CurrentOperator(context.Background())
	if ex.CurrentStep != 1 || ex.State.Terminal() {
		t.Fatal("advanced on unverified acknowledgement", ex)
	}
	f.groups.after = nil
	f.engine.Tick(context.Background())
	ex, _ = f.coord.CurrentOperator(context.Background())
	if ex.CurrentStep != 2 || ex.State != OperatorRunning {
		t.Fatal("verified retry did not advance", ex)
	}
}
func TestOperatorTransitionDoesNotRegressWallClock(t *testing.T) {
	t.Parallel()
	before := time.Unix(20, 0)
	now := before.Add(-time.Second)
	if !operatorTime(now, before).Equal(before) {
		t.Fatal("transition regressed time")
	}
}

func TestPermanentPromotionRefusalDoesNotWaitForever(t *testing.T) {
	t.Parallel()
	state, _, retry := classifyStepError(Step{Kind: StepPromoteVoter}, &membership.SafetyRefusal{Reason: "replica is not a current learner; add it as a learner first"})
	if retry || state != OperatorFailed {
		t.Fatal("permanent promotion refusal treated as waiting")
	}
	state, _, retry = classifyStepError(Step{Kind: StepRemoveReplica}, &membership.SafetyRefusal{Reason: "post-change live voter quorum would be unavailable"})
	if !retry || state != OperatorWaitingReplica {
		t.Fatal("transient liveness refusal treated as terminal")
	}
}

func TestControllerFreshGroupSeedsVotersBeforeIntentionalLearner(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	active := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"old"})
	target := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"old", "new"})
	target.Version = 2
	target.Group("new").Replicas[1].Role = ReplicaLearner
	steps := []Step{{Seq: 1, Kind: StepCreateFoundingGroup, GroupID: "new"}, {Seq: 2, Kind: StepPrepareReplica, GroupID: "new", NodeName: "n2", ReplicaID: 2}, {Seq: 3, Kind: StepAddLearner, GroupID: "new", NodeName: "n2", ReplicaID: 2}}
	f := newControllerFixture(active, target, steps, 0, OperatorPending)
	f.engine.Tick(context.Background())
	ex, _ := f.coord.CurrentOperator(context.Background())
	if ex.CurrentStep != 2 || ex.State != OperatorRunning {
		t.Fatal("founding did not accept exact voter seed", ex)
	}
	f.engine.Tick(context.Background())
	f.engine.Tick(context.Background())
	ex, _ = f.coord.CurrentOperator(context.Background())
	if ex.State != OperatorSucceeded {
		t.Fatal("intentional learner did not finish", ex)
	}
	if !reflect.DeepEqual(f.groups.calls, []StepKind{StepCreateFoundingGroup, StepPrepareReplica, StepAddLearner}) {
		t.Fatal("learner bypassed prepare/add scope", f.groups.calls)
	}
}
