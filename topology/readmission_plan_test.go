//go:build !functional

package topology

import (
	"slices"
	"testing"
)

func TestReadmissionPlanUsesPreparationAsHostRetirementBoundary(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"old member", "already learner", "already voter", "partial deletion", "complete deletion", "numeric group"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			group := "work"
			if mode == "numeric group" {
				group = "001"
			}
			active := planDoc(1, 3, []string{"a", "b", "c"}, shardOf(group, rep("a", 1), rep("b", 2), rep("c", 3)))
			desired := active.Clone()
			desired.Version++
			desired.MetaGroup().Replicas[0].ReplicaID = 4
			desired.MetaGroup().NextReplicaID = 5
			desired.Group(group).Replicas[0].ReplicaID = 4
			desired.Group(group).NextReplicaID = 5
			if mode == "complete deletion" {
				active.Group(group).Replicas = active.Group(group).Replicas[:1]
			}
			if mode == "partial deletion" || mode == "complete deletion" {
				desired.Groups = desired.Groups[:1]
			}
			statuses := statusForDocument(active)
			meta := statuses[MetaGroupID]
			meta.LeaderID = 2
			if mode == "already learner" || mode == "already voter" {
				meta.ConfState.Voters = []uint64{2, 3}
				if mode == "already learner" {
					meta.ConfState.Learners = []uint64{4}
				} else {
					meta.ConfState.Voters = append(meta.ConfState.Voters, 4)
				}
			}
			statuses[MetaGroupID] = meta
			plan, err := BuildPlan(active, desired, statuses, Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 3, MaxReplicas: 5}})
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Steps) == 0 || plan.Steps[0].GroupID != MetaGroupID {
				t.Fatal("metadata admission must precede load work", plan)
			}
			if !ReadmissionCovers(active, desired, plan, "a") {
				t.Fatal("missing retirement boundary", plan)
			}
			for _, s := range plan.Steps {
				if s.NodeName == "a" && s.ReplicaID == 1 && (s.Kind == StepStopReplica || s.Kind == StepCleanupReplicaState) {
					t.Fatal("plan dispatches to retired host", s)
				}
			}
			hasDelete := slices.ContainsFunc(plan.Steps, func(s Step) bool { return s.GroupID == group && s.Kind == StepDeleteGroupState })
			if hasDelete != (mode == "partial deletion") {
				t.Fatal("wrong residual deletion", plan)
			}
			if mode == "old member" && !slices.ContainsFunc(plan.Steps, func(s Step) bool { return s.GroupID == MetaGroupID && s.Kind == StepRemoveReplica && s.ReplicaID == 1 }) {
				t.Fatal("lost native removal", plan)
			}
			incomplete := Plan{Version: plan.Version, Steps: slices.Clone(plan.Steps)}
			incomplete.Steps = slices.DeleteFunc(incomplete.Steps, func(s Step) bool { return s.GroupID == MetaGroupID && s.Kind == StepPrepareReplica })
			if ReadmissionCovers(active, desired, incomplete, "a") {
				t.Fatal("incomplete admission satisfied retirement")
			}
		})
	}
}
