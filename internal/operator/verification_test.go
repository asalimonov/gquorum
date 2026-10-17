//go:build !functional

package operator

import (
	"testing"
	"time"

	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/topology"
)

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
	classification := classifyStepError(Step{Kind: StepPromoteVoter}, &membership.SafetyRefusal{Reason: "replica is not a current learner; add it as a learner first"})
	if classification.retry || classification.state != topology.OperationFailed {
		t.Fatal("permanent promotion refusal treated as waiting")
	}
	classification = classifyStepError(Step{Kind: StepRemoveReplica}, &membership.SafetyRefusal{Reason: "post-change live voter quorum would be unavailable"})
	if !classification.retry || classification.state != topology.OperationWaitingForReplica {
		t.Fatal("transient liveness refusal treated as terminal")
	}
}
