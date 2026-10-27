//go:build !functional

package metaengine

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedApprovalRetirementDoesNotReissueConsumedWork(t *testing.T) {
	t.Parallel()
	base := baseDoc()
	base.Version = 1
	desired := base.Clone()
	desired.Version = 2
	desired.Group("shard0").Replicas[0].ReplicaID = 2
	desired.Group("shard0").NextReplicaID = 3
	input := topology.PlanInput{Approved: base,
		Groups: map[string]topology.GroupProgress{"shard0": {ConvergedVersion: 1, Converged: base.Group("shard0")}}}
	cases := []struct {
		name     string
		kind     topology.StepKind
		progress topology.RetainedProgress
		removed  uint64
		refuse   bool
	}{
		{"consumed removal", topology.StepRemoveReplica, topology.RetainedCleanup, 10, true},
		{"cleanup owned", topology.StepCleanupReplicaState, topology.RetainedCleanup, 10, true},
		{"cleanup stopped", topology.StepCleanupReplicaState, topology.RetainedStopped, 10, true},
		{"stop consumed", topology.StepStopReplica, topology.RetainedStopped, 10, true},
		{"required stop", topology.StepStopReplica, topology.RetainedCleanup, 10, false},
		{"delete cleanup only", topology.StepDeleteGroupState, topology.RetainedCleanup, 10, true},
		{"delete hosted", topology.StepDeleteGroupState, topology.RetainedHosted, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			collector := planningCollector{owners: map[planningReplicaKey]*planningReplica{{"shard0", 1}: {
				target:   topology.AuthorityTarget{NodeName: "n1", MemberID: 1, ReplicaID: 1},
				progress: tc.progress, removed: tc.removed}}}
			plan := topology.Plan{Version: 2, Steps: []topology.Step{{Seq: 1, GroupID: "shard0",
				NodeName: "n1", ReplicaID: 1, Kind: tc.kind}}}
			err := collector.validateNewRetirement(input, desired, plan, DeletionSemanticsLegacy)
			if tc.refuse != errors.Is(err, fault.ErrOperationAuthorization) {
				t.Fatal("incorrect new ownership decision", err)
			}
		})
	}
	owner := planningReplica{progress: topology.RetainedCleanup}
	stop := topology.Step{GroupID: "shard0", NodeName: "n1", ReplicaID: 1, Kind: topology.StepStopReplica}
	if !duplicateRetirement(owner, base.Group("shard0"), stop, DeletionSemanticsLegacy) || duplicateRetirement(owner, nil, stop, DeletionSemanticsLegacy) {
		t.Fatal("cleanup stop must be required by replacement or deletion")
	}
}

func TestScopedImmutableRetirementShapeSurvivesConsumption(t *testing.T) {
	t.Parallel()
	base := baseDoc()
	base.Version = 1
	desired := base.Clone()
	desired.Version = 2
	desired.Group("shard0").Replicas[0].ReplicaID = 2
	desired.Group("shard0").NextReplicaID = 3
	input := topology.PlanInput{Approved: desired,
		Groups: map[string]topology.GroupProgress{"shard0": {ConvergedVersion: 1, Converged: base.Group("shard0")}},
		Retained: map[string]topology.RetainedGroup{"shard0": {Replicas: []topology.RetainedReplica{{
			ReplicaKey: topology.ReplicaKey{NodeName: "n1", ReplicaID: 1}, Progress: topology.RetainedStopped}}}}}
	plan := topology.Plan{Version: 2, Steps: []topology.Step{
		{Seq: 1, GroupID: "shard0", NodeName: "n1", ReplicaID: 1, Kind: topology.StepStopReplica},
	}, Blocks: []topology.PlanBlock{{GroupID: "shard0", SourceVersion: 1, Steps: []int{1}}}}
	if err := topology.ValidatePlanShape(input, desired, plan); err != nil {
		t.Fatal("immutable completed actions became invalid", err)
	}
	collector := planningCollector{owners: map[planningReplicaKey]*planningReplica{{"shard0", 1}: {
		target: topology.AuthorityTarget{NodeName: "n1", MemberID: 1, ReplicaID: 1}, progress: topology.RetainedStopped}}}
	if err := collector.validateNewRetirement(input, desired, plan, DeletionSemanticsLegacy); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("new approval reclaimed completed work", err)
	}
}
