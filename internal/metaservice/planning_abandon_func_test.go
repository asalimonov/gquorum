//go:build functional

package metaservice

import (
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
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
			active := f.g.machine.Active()
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
			plan := abandonedIntentPlan(t, f, active)
			at := time.Now().UTC()
			op := f.command(t, metaengine.BeginOperatorProposal{Version: desired.Version, Plan: plan, At: at})
			first := plan.Steps[0]
			f.command(t, metaengine.TransitionOperatorProposal{Transition: topology.OperatorTransition{
				OperatorID: op.OperatorID, Version: desired.Version, Attempt: 1, StepSeq: first.Seq,
				GroupID: first.GroupID, NodeName: first.NodeName, State: topology.OperatorFailed,
				Error: "failed before any grant", At: at.Add(time.Second)}})
			if founding {
				desired.Groups = slices.DeleteFunc(desired.Groups, func(g topology.Group) bool { return g.ID == groupID })
			} else {
				desired.Group(groupID).Replicas = desired.Group(groupID).Replicas[:2]
			}
			f.commit(t, desired, false)
			desired = f.g.machine.Latest()
			plan = abandonedIntentPlan(t, f, active)
			f.command(t, metaengine.BeginOperatorProposal{Version: desired.Version, Plan: plan, At: at.Add(2 * time.Second)})
			// An approved plan alone cannot create host work. Exact established
			// replica 3 still needs retirement in the replacement case.
			for _, step := range plan.Steps {
				if step.GroupID == groupID && (founding || step.ReplicaID == 4) {
					t.Fatalf("unissued intent produced host work: %+v", plan)
				}
			}
			if !founding && len(plan.Steps) != 3 {
				t.Fatalf("lost established replica retirement: %+v", plan)
			}
		})
	}
}

func abandonedIntentPlan(t *testing.T, f *mutationFixture, active topology.Document) topology.Plan {
	t.Helper()
	desired := f.g.machine.Latest()
	base, err := f.g.machine.DeletionBase(t.Context(), desired)
	if err != nil {
		t.Fatal(err)
	}
	observed := make(map[string]topology.GroupStatus)
	for _, doc := range []topology.Document{base, desired} {
		for _, group := range doc.Groups {
			observed[group.ID] = topology.GroupStatus{GroupID: group.ID}
		}
	}
	for _, group := range active.Groups {
		observed[group.ID] = topology.GroupStatus{GroupID: group.ID, Found: true, LeaderID: 1,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}, HealthKnown: true,
			LiveVoters: map[uint64]bool{1: true, 2: true, 3: true}}
	}
	plan, err := topology.BuildPlan(testPlanningInput(t, f.g.machine, desired, observed), desired, f.g.rules)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
