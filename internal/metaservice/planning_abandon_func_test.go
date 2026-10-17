//go:build functional

package metaservice

import (
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

func TestApproveAbandonsUnissuedLoadIntent(t *testing.T) {
	t.Parallel()
	for _, founding := range []bool{true, false} {
		name := "unprepared replacement"
		if founding {
			name = "unfounded group"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newMutationFixture(t)
			active := fixtureConvergedDocument(t, f.g.machine)
			desired := active.Clone()
			groupID := "work"
			if founding {
				group := *desired.Group("work")
				group.ID = "extra"
				desired.Groups = append(desired.Groups, group)
				groupID = group.ID
			} else {
				group := desired.Group(groupID)
				group.Replicas[2].ReplicaID, group.NextReplicaID = 4, 5
			}
			f.commit(t, desired, false)
			desired = f.g.machine.Latest()
			approval := f.fixtureApproval(t, desired)
			plan := approval.Plan
			at := time.Now().UTC()
			op := f.command(t, approval)
			first := plan.Steps[0]
			f.command(t, metaengine.TransitionOperationProposal{Transition: topology.OperationTransition{
				OperationID: op.OperationID, Version: desired.Version, Attempt: 1, StepSeq: first.Seq,
				GroupID: first.GroupID, State: topology.OperationFailed,
				Error: "failed before any grant", At: at.Add(time.Second)}, ProposalID: newProposalID()})
			if founding {
				desired.Groups = slices.DeleteFunc(desired.Groups, func(g topology.Group) bool { return g.ID == groupID })
			} else {
				desired.Group(groupID).Replicas = desired.Group(groupID).Replicas[:2]
			}
			f.commit(t, desired, false)
			desired = f.g.machine.Latest()
			approval = f.fixtureApproval(t, desired)
			approval.At = at.Add(2 * time.Second)
			plan = approval.Plan
			f.command(t, approval)
			// An approved plan alone cannot create host work. Exact established
			// replica 3 still needs retirement in the replacement case.
			for _, step := range plan.Steps {
				if step.GroupID == groupID && (founding || step.ReplicaID == 4) {
					t.Fatalf("unissued intent produced host work: %+v", plan)
				}
			}
			if !founding && (len(plan.Steps) != 3 || plan.Steps[0].Kind != topology.StepRemoveReplica || plan.Steps[0].ReplicaID != 3 || !slices.Equal(plan.Blocks[0].Steps, []int{1}) || !slices.Equal(plan.Blocks[0].Cleanup, []int{2, 3})) {
				t.Fatalf("lost established replica retirement: %+v", plan)
			}
		})
	}
}
