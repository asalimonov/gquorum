//go:build !functional

package topology

import "testing"

func TestAdmissionSequenceMatchesDesiredRoleAndExactIdentity(t *testing.T) {
	t.Parallel()
	var steps []Step
	for i, kind := range []StepKind{StepPrepareReplica, StepAddLearner, StepWaitCaughtUp, StepPromoteVoter} {
		steps = append(steps, Step{Seq: i + 1, Kind: kind, GroupID: MetaGroupID, NodeName: "n4", ReplicaID: 4})
	}
	if !ValidAdmissionSequence(steps, "n4", 4, ReplicaVoter) || !ValidAdmissionSequence(steps[:2], "n4", 4, ReplicaLearner) {
		t.Fatal("valid sequence rejected")
	}
	if ValidAdmissionSequence(steps[:2], "n4", 4, ReplicaVoter) || ValidAdmissionSequence(steps, "n4", 4, ReplicaLearner) {
		t.Fatal("wrong role sequence accepted")
	}
	if ValidAdmissionSequence(steps[:1], "n4", 4, ReplicaLearner) || ValidAdmissionSequence(steps[:2], "n4", 5, ReplicaLearner) || ValidAdmissionSequence(steps, "successor", 4, ReplicaVoter) {
		t.Fatal("incomplete or foreign sequence accepted")
	}
	steps[1].GroupID = "load"
	if ValidAdmissionSequence(steps, "n4", 4, ReplicaVoter) {
		t.Fatal("load step authorized meta admission")
	}
}
