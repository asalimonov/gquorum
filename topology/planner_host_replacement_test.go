//go:build !functional

package topology

import (
	"errors"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestSameHostReplacementJoinsPredecessorBeforePreparation(t *testing.T) {
	t.Parallel()
	for _, count := range []int{1, 2, 3} {
		base := planDoc(1, 3, []string{"n1", "n2", "n3"},
			shardOf("work", rep("n1", 1), rep("n2", 2), rep("n3", 3)))
		desired := base.Clone()
		desired.Version++
		for i := range count {
			desired.Group("work").Replicas[i].ReplicaID = uint64(4 + i)
		}
		desired.Group("work").NextReplicaID = uint64(4 + count)
		observed := statusForDocument(base)
		status := observed["work"]
		status.HealthKnown, status.LiveVoters = true, map[uint64]bool{1: true, 2: true, 3: true}
		observed["work"] = status
		input := testPlanInput(base, observed)
		plan, err := BuildPlan(input, desired, plannerRules())
		if err != nil {
			t.Fatal(err)
		}
		stopped := make(map[string]int)
		for _, step := range plan.Steps {
			if step.GroupID != "work" {
				continue
			}
			if step.Kind == StepStopReplica {
				stopped[step.NodeName] = step.Seq
			}
			if step.Kind == StepPrepareReplica && stopped[step.NodeName] == 0 {
				t.Fatalf("%d replacements prepare onto an owned host: %+v", count, plan.Steps)
			}
		}
		block := plan.Blocks[slices.IndexFunc(plan.Blocks, func(b PlanBlock) bool { return b.GroupID == "work" })]
		for _, seq := range stopped {
			if !slices.Contains(block.Steps, seq) {
				t.Fatal("host ownership join detached from replacement", plan)
			}
		}
		if count == 1 {
			status.LiveVoters[3] = false
			observed["work"] = status
			if _, err := BuildPlan(testPlanInput(base, observed), desired, plannerRules()); !errors.Is(err, fault.ErrUnsafeMembership) {
				t.Fatal("same-host replacement lost a surviving quorum without refusal", err)
			}
		}
	}
}
