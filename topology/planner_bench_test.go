//go:build !functional

package topology

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func plannerMatrix(size int) (Document, Document, map[string]GroupStatus, Rules) {
	names := make([]string, size)
	for i := range names {
		names[i] = fmt.Sprintf("node%d", i+1)
	}
	var groups []Group
	for i := range 15 {
		groups = append(groups, shardOf(fmt.Sprintf("work-%02d", i), rep("node1", 1), rep("node2", 2), rep("node3", 3)))
	}
	active := planDoc(2, 3, names, groups...)
	active.CreatedAt = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	active.Placement.AllowedNodeTypes = []NodeType{"worker"}
	for i := 3; i < size; i++ {
		active.Nodes[i].Type = "observer"
	}
	desired := active.Clone()
	desired.Version++
	desired.ParentVersion = active.Version
	observed := statusForDocument(active)
	for _, declared := range desired.LoadGroups() {
		group := desired.Group(declared.ID)
		group.Replicas[2].ReplicaID = 4
		group.NextReplicaID = 5
		status := observed[group.ID]
		status.LeaderID = 3 // The departing voter must transfer before removal.
		status.HealthKnown = true
		status.LiveVoters = map[uint64]bool{1: true, 2: true, 3: true}
		observed[group.ID] = status
	}
	return active, desired, observed, Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: size, MaxReplicas: 5}}
}

func TestPlannerMatrixDeterministicSafeAndWithinDeadline(t *testing.T) {
	t.Parallel()
	for _, size := range []int{3, 5, 7} {
		t.Run(fmt.Sprintf("%d-nodes", size), func(t *testing.T) {
			active, desired, observed, rules := plannerMatrix(size)
			if err := active.Validate(rules); err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			plan, err := BuildPlan(active, desired, observed, rules)
			if err != nil {
				t.Fatal(err)
			}
			if elapsed := time.Since(start); elapsed >= 200*time.Millisecond {
				t.Fatal("planner exceeded the matrix's operation deadline", elapsed)
			}
			if plan.Version != desired.Version || len(plan.Steps) != 15*9 {
				t.Fatalf("planner skipped part of the fifteen-group replacement: %d steps", len(plan.Steps))
			}
			kinds := []StepKind{StepPrepareReplica, StepAddLearner, StepWaitCaughtUp, StepTransferLeader, StepRemoveReplica, StepStopReplica, StepCleanupReplicaState, StepWaitCaughtUp, StepPromoteVoter}
			for i := range 15 {
				for offset, kind := range kinds {
					seq := i*len(kinds) + offset
					step := plan.Steps[seq]
					node, id := "node3", uint64(4)
					if kind == StepTransferLeader {
						node, id = "node1", 1
					} else if offset >= 4 && offset <= 6 {
						id = 3
					}
					if step.Seq != seq+1 || step.GroupID != fmt.Sprintf("work-%02d", i) || step.Kind != kind || step.NodeName != node || step.ReplicaID != id {
						t.Fatal("replacement lacks its exact safe transfer/cleanup/learner ordering", step)
					}
				}
			}
			for repeat := range 32 {
				next, err := BuildPlan(active, desired, observed, rules)
				if err != nil || !reflect.DeepEqual(plan, next) {
					t.Fatal("planner changed its exact ordered result", repeat, err)
				}
			}
		})
	}
}

func BenchmarkPlannerFifteenGroupReplacement(b *testing.B) {
	for _, size := range []int{3, 5, 7} {
		b.Run(fmt.Sprintf("%d-nodes", size), func(b *testing.B) {
			active, desired, observed, rules := plannerMatrix(size)
			b.ReportAllocs()
			for b.Loop() {
				plan, err := BuildPlan(active, desired, observed, rules)
				if err != nil || len(plan.Steps) != 135 {
					b.Fatal("invalid benchmark workload", err)
				}
			}
		})
	}
}
