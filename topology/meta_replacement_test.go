//go:build !functional

package topology

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestMetaAdmissionPrecedesDecommissionAtCapacity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		nodes    int
		old, new ReplicaRole
	}{
		{"three voter replacement", 3, ReplicaVoter, ReplicaVoter},
		{"three learner replacement", 3, ReplicaLearner, ReplicaLearner},
		{"four learner host replacement", 4, ReplicaLearner, ReplicaLearner},
		{"four voters shrink with learner arrival", 4, ReplicaVoter, ReplicaLearner},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var names []string
			for i := 1; i <= tc.nodes; i++ {
				names = append(names, fmt.Sprintf("n%d", i))
			}
			departing, arrival := names[tc.nodes-1], fmt.Sprintf("n%d", tc.nodes+1)
			base := planDoc(1, 3, names, shardOf("work", rep("n1", 1), rep("n2", 2), rep(departing, 3)))
			base.MetaGroup().Replicas[tc.nodes-1].Role = tc.old
			desired := base.Clone()
			desired.Version++
			desired.Node(departing).State = NodeExcluded
			desired.Nodes = append(desired.Nodes, Node{Name: arrival, Type: "worker", State: NodeActive})
			desired.MetaGroup().Replicas[tc.nodes-1] = Replica{NodeName: arrival, ReplicaID: uint64(tc.nodes + 1), Role: tc.new}
			desired.MetaGroup().NextReplicaID++
			desired.Group("work").Replicas[2] = rep(arrival, 4)
			desired.Group("work").NextReplicaID = 5
			observed := statusForDocument(base)
			for id, status := range observed {
				status.HealthKnown, status.LiveVoters = true, make(map[uint64]bool)
				for _, voter := range status.ConfState.Voters {
					status.LiveVoters[voter] = true
				}
				observed[id] = status
			}
			input := testPlanInput(base, observed)
			rules := Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: tc.nodes, MaxReplicas: 5}.WithDefaults()}
			flat, err := BuildPlan(input, desired, rules)
			if err != nil || len(flat.Blocks) != 0 {
				t.Fatal("live planner rejected replacement or emitted partitions", flat, err)
			}
			plan, err := scopedPlan(input, desired, PlanOptions{Rules: rules})
			if err != nil || !slices.Equal(flat.Steps, plan.Steps) {
				t.Fatal("scoped barriers made replacement cyclic", plan, err)
			}
			prepare, remove := 0, 0
			for _, step := range plan.Steps {
				if step.GroupID != MetaGroupID {
					continue
				}
				if step.Kind == StepPrepareReplica && step.NodeName == arrival {
					prepare = step.Seq
				}
				if step.Kind == StepRemoveReplica && step.NodeName == departing {
					remove = step.Seq
				}
			}
			if prepare == 0 || remove <= prepare || len(plan.Dependencies) != 2 {
				t.Fatal("admission did not precede evacuation-gated removal", plan)
			}
		})
	}
}

func TestLivePlanPreviewAllowsAlreadyConvergedSources(t *testing.T) {
	t.Parallel()
	base := planDoc(1, 3, []string{"n1", "n2", "n3"}, shardOf("work", rep("n1", 1)))
	input := testPlanInput(base, statusForDocument(base))
	plan, err := BuildPlan(input, base, plannerRules())
	if err != nil || len(plan.Steps) != 0 {
		t.Fatal("converged preview failed", plan, err)
	}
	desired := base.Clone()
	desired.Version++
	desired.Group("work").Replicas = append(desired.Group("work").Replicas, learner("n2", 2))
	desired.Group("work").NextReplicaID = 3
	input.Approved = desired.Clone()
	input.Groups[MetaGroupID] = GroupProgress{GroupID: MetaGroupID,
		ConvergedVersion: desired.Version, Converged: desired.MetaGroup()}
	plan, err = BuildPlan(input, desired, plannerRules())
	if err != nil || len(plan.Steps) != 2 || plan.Steps[0].GroupID != "work" {
		t.Fatal("partially converged preview failed", plan, err)
	}
}

func TestMetaReplacementRefusesUnknownCapacityAndKeepsReadmissionOrder(t *testing.T) {
	t.Parallel()
	base := planDoc(1, 3, []string{"n1", "n2", "n3"})
	desired := base.Clone()
	desired.Version++
	desired.MetaGroup().Replicas[2].ReplicaID = 4
	desired.MetaGroup().NextReplicaID = 5
	observed := statusForDocument(base)
	status := observed[MetaGroupID]
	status.HealthKnown, status.LiveVoters = true, map[uint64]bool{1: true, 2: true, 3: true}
	observed[MetaGroupID] = status
	input := testPlanInput(base, observed)
	rules := Rules{SchemaVersion: 1, Limits: Limits{}.WithDefaults()}
	plan, err := scopedPlan(input, desired, PlanOptions{Rules: rules})
	if err != nil || plan.Steps[0].Kind != StepRemoveReplica || plan.Steps[0].ReplicaID != 3 ||
		len(plan.Dependencies) != 0 {
		t.Fatal("same-node readmission acquired a decommission order or barrier", plan, err)
	}
	flat, err := BuildPlan(input, desired, rules)
	if err != nil || !slices.Equal(flat.Steps, plan.Steps) {
		t.Fatal("live and scoped readmission order differs", flat, err)
	}
	native := input.Native[MetaGroupID]
	native.ConfState.Learners = []uint64{99}
	input.Native[MetaGroupID] = native
	if _, err := BuildPlan(input, desired, rules); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal("unknown native learner supplied a capacity seat", err)
	}
}
