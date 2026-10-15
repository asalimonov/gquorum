//go:build !functional

package topology

import (
	"slices"
	"testing"
)

func TestBuildPlanBindsCanonicalVoterProof(t *testing.T) {
	t.Parallel()
	active := planDoc(1, 5, []string{"n1", "n2", "n3"}, shardOf("work", rep("n1", 1), rep("n2", 2), learner("n3", 3)))
	desired := active.Clone()
	desired.Version++
	desired.Group("work").Replicas[1].Role = ReplicaLearner
	desired.Group("work").Replicas[2].Role = ReplicaVoter
	statuses := statusForDocument(active)
	work := statuses["work"]
	work.ConfState.Voters = []uint64{2, 1}
	work.HealthKnown = true
	work.LiveVoters = map[uint64]bool{1: true, 2: true}
	statuses["work"] = work
	plan, err := BuildPlan(testPlanInput(active, statuses), desired, plannerRules())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.VoterBases) != 1 || plan.VoterBases[0].GroupID != "work" || !slices.Equal(plan.VoterBases[0].Voters, []uint64{1, 2}) {
		t.Fatalf("noncanonical voter base: %+v", plan.VoterBases)
	}
	if !slices.Equal(work.ConfState.Voters, []uint64{2, 1}) {
		t.Fatal("planner mutated caller observation")
	}
	set := []uint64{1, 2}
	for _, step := range plan.Steps {
		if !isMembershipStep(step.Kind) {
			continue
		}
		if step.ExpectedVotersDigest != VoterSetDigest(set) {
			t.Fatalf("wrong predecessor at step %d", step.Seq)
		}
		switch step.Kind {
		case StepPromoteVoter:
			set = append(set, step.ReplicaID)
		case StepDemoteVoter, StepRemoveReplica:
			set = slices.DeleteFunc(set, func(id uint64) bool { return id == step.ReplicaID })
		}
		if step.ResultVotersDigest != VoterSetDigest(set) {
			t.Fatalf("wrong result at step %d", step.Seq)
		}
	}
}

func TestBuildPlanBindsEmptyFoundingBase(t *testing.T) {
	t.Parallel()
	active := planDoc(1, 3, []string{"n1"})
	desired := active.Clone()
	desired.Version++
	desired.Groups = append(desired.Groups, shardOf("new", rep("n1", 1)))
	statuses := statusForDocument(active)
	statuses["new"] = GroupStatus{GroupID: "new"}
	plan, err := BuildPlan(testPlanInput(active, statuses), desired, plannerRules())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.VoterBases) != 1 || len(plan.VoterBases[0].Voters) != 0 || plan.Steps[0].ExpectedVotersDigest != VoterSetDigest(nil) || plan.Steps[0].ResultVotersDigest != VoterSetDigest([]uint64{1}) {
		t.Fatalf("founding proof is incomplete: %+v", plan)
	}
}

func TestBuildPlanRejectsUnknownObservedIdentity(t *testing.T) {
	t.Parallel()
	active := planDoc(1, 3, []string{"n1"}, shardOf("work", rep("n1", 1)))
	desired := active.Clone()
	desired.Version++
	statuses := statusForDocument(active)
	work := statuses["work"]
	work.ConfState.Learners = []uint64{999}
	statuses["work"] = work
	if plan, err := BuildPlan(testPlanInput(active, statuses), desired, plannerRules()); err == nil {
		t.Fatalf("accepted unknown native identity: %+v", plan)
	}
}
