//go:build !functional

package scheduling

import (
	"testing"
	"time"

	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

func scopedVoterInput() ScheduleInput {
	in := voterInput()
	template := in.Target.Clone().Groups[0]
	in.Target.Groups[0].VoterConfig = nil
	original := in.Metrics.Groups[0]
	in.Metrics.Groups = nil
	for _, id := range []string{"a", "b"} {
		group := topology.Document{Groups: []topology.Group{template}}.Clone().Groups[0]
		group.ID = id
		in.Target.Groups = append(in.Target.Groups, group)
		observed := metrics.ClusterSnapshot{Groups: []metrics.GroupObservation{original}}.Clone().Groups[0]
		observed.GroupID = id
		for i := range observed.Replicas {
			observed.Replicas[i].Sample.Identity.GroupID = id
		}
		in.Metrics.Groups = append(in.Metrics.Groups, observed)
		status := in.Status[topology.MetaGroupID].Clone()
		status.GroupID = id
		in.Status[id] = status
	}
	in.Groups = provenPolicyGroups(in.Target)
	return in
}

func TestScopedRepairRotatesSelectedGroupsAndSkipsBusyScopes(t *testing.T) {
	t.Parallel()
	in := scopedVoterInput()
	policy, _ := NewDefaultRepairPolicy(DefaultRepairOptions{})
	for _, want := range []string{"a", "b", "a"} {
		decision, err := policy.Schedule(t.Context(), in)
		if err != nil || decision.Action == nil || decision.Action.VoterChange.GroupID != want {
			t.Fatal(decision, err, want)
		}
	}
	// Selection rotates even when the driver refused the prior candidate. An
	// actual busy block is skipped without suppressing the independent repair.
	row := in.Groups["a"]
	row.OperationID = "exec-v2-a1"
	row.Blocked = &topology.BlockedReason{Code: topology.BlockedCatchUp}
	in.Groups["a"] = row
	for range 2 {
		decision, err := policy.Schedule(t.Context(), in)
		if err != nil || decision.Action == nil || decision.Action.VoterChange.GroupID != "b" {
			t.Fatal(decision, err)
		}
	}
}

func TestScopedVoterEvidenceSurvivesUnrelatedDocumentAndGroupChanges(t *testing.T) {
	t.Parallel()
	in := scopedVoterInput()
	row := in.Groups["a"]
	row.OperationID = "exec-v8-a1"
	in.Groups["a"] = row
	in.Target.Version = 9
	in.Metrics.TargetVersion = 9
	in.Metrics.Groups[0].SourceVersion = 8
	policy, _ := NewDefaultRepairPolicy(DefaultRepairOptions{})
	decision, err := policy.Schedule(t.Context(), in)
	if err != nil || decision.Action == nil || decision.Action.VoterChange.GroupID != "b" {
		t.Fatal(decision, err)
	}
	// Neither version churn nor another group's repair erases B's cooldown.
	in.Metrics.Groups[1].CooldownRemaining = time.Minute
	decision, err = policy.Schedule(t.Context(), in)
	if err != nil || decision.Action != nil {
		t.Fatal(decision, err)
	}
	in.Metrics.Groups[1].CooldownRemaining = 0
	in.Metrics.Groups[1].SourceVersion++
	decision, err = policy.Schedule(t.Context(), in)
	if err != nil || decision.Action != nil {
		t.Fatal("stale B evidence accepted", decision, err)
	}
}
