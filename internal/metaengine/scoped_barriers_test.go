//go:build !functional

package metaengine

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedLoadBarrierSurvivesMetadataSupersession(t *testing.T) {
	t.Parallel()
	for _, consumed := range []bool{false, true} {
		t.Run(map[bool]string{false: "ready only", true: "consumed"}[consumed], func(t *testing.T) {
			t.Parallel()
			s := loadAdmissionFixture(t)
			s.Operations[0].Plan.Dependencies = []topology.StepDependency{{StepSeq: 3, Requires: []int{1}}}
			s.Admissions[0].State = AdmissionReady
			if consumed {
				admissionPrefixReceipt(&s, 0, 1)
			}
			prepareScopedFixture(t, &s)
			original := s.Operations[0].Clone()
			action := scopedPlanAction(&original, original.Plan.Steps[2])
			s, p := admissionApprovalFixture(t, s, 0)
			p.Plan.Steps, p.Plan.Blocks = p.Plan.Steps[:2], p.Plan.Blocks[:1]
			p.Plan.Blocks[0].Steps, p.Plan.Dependencies = []int{1, 2}, nil
			applyScopedApprovalFixture(t, &s, p)
			if s.indexes.groups["shard0"].Owner != original.Status.ID ||
				!reflect.DeepEqual(s.Operations[0].Status.Blocks[1], original.Status.Blocks[1]) ||
				s.Admissions[0].OperationID != "exec-v3-a1" {
				t.Fatal("metadata supersession took over the unchanged load block")
			}
			// Dispatch/execution revalidation and grant issuance share the same
			// predicate. The dependency's original metadata step is superseded.
			err := s.authorizeLocked(context.Background(), action)
			issued, issueErr := s.applyBeginEffect(s.AppliedIndex+1, BeginEffectProposal{Action: action})
			if consumed {
				if err != nil || issueErr != nil || !issued.changed {
					t.Fatal("consumed original-owner preparation was lost", err, issueErr)
				}
			} else if !errors.Is(err, fault.ErrUnavailable) || !errors.Is(issueErr, fault.ErrUnavailable) || issued.changed {
				t.Fatal("Ready substituted for preparation consumption", err, issueErr)
			}
		})
	}
}

func TestScopedMetadataRemovalKeepsOwnerAcrossLoadSupersession(t *testing.T) {
	t.Parallel()
	s, deletion := readmissionDeletionFixture(t)
	original := s.Operations[0].Clone()
	action := scopedPlanAction(&original, original.Plan.Steps[5])
	applyScopedApprovalFixture(t, &s, deletion)
	if s.indexes.groups[topology.MetaGroupID].Owner != original.Status.ID {
		t.Fatal("load deletion superseded metadata admission/removal")
	}
	if err := s.validateActionBarriers(&original, action); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("same-node replacement skipped old-owner retirement", err)
	}
	grants := append([]scopedAuthorityRecord(nil), s.AuthorityRecords...)
	for _, grant := range grants {
		recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: fixtureEffectReceipts(grant.Grant)[0],
			At: original.UpdatedAt})
	}
	if len(s.Cleanup) == 0 || s.indexes.groups["shard0"].Declaration != nil {
		t.Fatal("fixture lacks detached cleanup after completed deletion")
	}
	if err := s.validateActionBarriers(&original, action); err != nil {
		t.Fatal("detached cleanup or load supersession stranded metadata removal", err)
	}
}

func evacuationFixture(t *testing.T) scopedState {
	t.Helper()
	s := scopedAdmissionFixture()
	base := s.Target.Clone()
	base.Version, base.ParentVersion = 1, 0
	base.Group("shard0").Replicas = append(base.Group("shard0").Replicas,
		topology.Replica{NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaLearner})
	base.Group("shard0").NextReplicaID = 3
	desired := base.Clone()
	desired.Version, desired.ParentVersion = 2, 1
	desired.Node("n2").State = topology.NodeExcluded
	for i := range desired.Groups {
		desired.Groups[i].Replicas = desired.Groups[i].Replicas[:1]
	}
	s.Target, s.Latest, s.History, s.Admissions = desired, desired.Clone(), []topology.Document{base}, nil
	op := &s.Operations[0]
	voters := topology.VoterSetDigest([]uint64{1})
	op.Plan = topology.Plan{Version: 2, Steps: []topology.Step{
		{Seq: 1, GroupID: topology.MetaGroupID, Kind: topology.StepRemoveReplica, NodeName: "n2", ReplicaID: 2,
			ExpectedVotersDigest: voters, ResultVotersDigest: voters},
		{Seq: 2, GroupID: "shard0", Kind: topology.StepRemoveReplica, NodeName: "n2", ReplicaID: 2,
			ExpectedVotersDigest: voters, ResultVotersDigest: voters},
		{Seq: 3, GroupID: "shard0", Kind: topology.StepStopReplica, NodeName: "n2", ReplicaID: 2},
		{Seq: 4, GroupID: "shard0", Kind: topology.StepCleanupReplicaState, NodeName: "n2", ReplicaID: 2},
	}, Blocks: []topology.PlanBlock{{GroupID: topology.MetaGroupID, SourceVersion: 1, Steps: []int{1}},
		{GroupID: "shard0", SourceVersion: 1, Steps: []int{2}, Cleanup: []int{3, 4}}},
		Dependencies: []topology.StepDependency{{StepSeq: 1, Requires: []int{2}}},
		VoterBases: []topology.PlanVoterBase{{GroupID: topology.MetaGroupID, Voters: []uint64{1}},
			{GroupID: "shard0", Voters: []uint64{1}}}}
	op.Status.State, op.Status.TotalBlocks = topology.OperationRunning, 2
	op.Status.Blocks = []topology.BlockStatus{
		{GroupID: topology.MetaGroupID, State: topology.OperationRunning, CurrentStep: 1, TotalSteps: 1},
		{GroupID: "shard0", State: topology.OperationRunning, CurrentStep: 2, TotalSteps: 1}}
	for i := range s.Groups {
		s.Groups[i].SourceVersion, s.Groups[i].Declaration, s.Groups[i].Owner = 1, base.Group(s.Groups[i].GroupID), op.Status.ID
		s.Allocations[i].NextReplicaID = 3
	}
	s.Nodes[1].SourceVersion, s.Nodes[1].Declaration, s.Nodes[1].MemberID, s.Nodes[1].CompletedAt = 1, base.Node("n2"), 2, op.UpdatedAt
	prepareScopedFixture(t, &s)
	return s
}

func TestScopedEvacuationRechecksSupersededLoadAndIgnoresDetachedDebt(t *testing.T) {
	t.Parallel()
	s := evacuationFixture(t)
	metadata := s.Operations[0].Clone()
	removal := scopedPlanAction(&metadata, metadata.Plan.Steps[0])
	if err := s.authorizeLocked(context.Background(), removal); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("metadata removal skipped assigned load replica", err)
	}
	// New explicit load intent supersedes the originally named evacuation step.
	// The unchanged metadata block must follow current state, not that step ID.
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = 3, 2
	s.Latest.Group("shard0").Labels = map[string]string{"new": "owner"}
	plan := metadata.Plan.Clone()
	plan.Version, plan.Dependencies, plan.Steps = 3, nil, plan.Steps[1:]
	for i := range plan.Steps {
		plan.Steps[i].Seq = i + 1
	}
	plan.Blocks = []topology.PlanBlock{{GroupID: "shard0", SourceVersion: 1, Steps: []int{1}, Cleanup: []int{2, 3}}}
	plan.VoterBases = plan.VoterBases[1:]
	applyScopedApprovalFixture(t, &s, BeginOperationProposal{Version: 3, Plan: plan, At: metadata.UpdatedAt.Add(time.Second),
		Scopes: []ScopeExpectation{{GroupID: "shard0", SourceVersion: 1, Owner: metadata.Status.ID}}})
	if s.indexes.groups[topology.MetaGroupID].Owner != metadata.Status.ID {
		t.Fatal("load supersession released metadata owner")
	}
	if _, err := s.applyBeginEffect(s.AppliedIndex+1, BeginEffectProposal{Action: removal}); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("new load owner hid unfinished evacuation", err)
	}
	load := s.Operations[1].Clone()
	applyScopedTransitionFixture(t, &s, scopedTransitionFor(load, "shard0", 1))
	issued, err := s.applyBeginEffect(s.AppliedIndex+1, BeginEffectProposal{Action: scopedPlanAction(&load, load.Plan.Steps[0])})
	if err != nil {
		t.Fatal(err)
	}
	recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: fixtureEffectReceipts(*issued.result.Grant)[0], At: load.UpdatedAt})
	complete := scopedTransitionFor(load, "shard0", 1)
	complete.State = topology.OperationSucceeded
	applyScopedTransitionFixture(t, &s, complete)
	if len(s.Cleanup) != 1 || s.Cleanup[0].Stopped != nil || s.indexes.groups["shard0"].SourceVersion != 3 {
		t.Fatal("fixture lacks unstopped detached debt", s.Cleanup)
	}
	if err := s.authorizeLocked(context.Background(), removal); err != nil {
		t.Fatal("superseded dependency or detached cleanup blocked proven evacuation", err)
	}
}

func TestScopedEvacuationTreatsUnconvergedApprovedAssignmentAsUnknown(t *testing.T) {
	t.Parallel()
	s := evacuationFixture(t)
	s.Groups[1].SourceVersion, s.Groups[1].Declaration, s.Groups[1].CompletedAt = 0, nil, time.Time{}
	s.Operations[0].Plan.Blocks[1].SourceVersion = 0
	prepareScopedFixture(t, &s)
	action := scopedPlanAction(&s.Operations[0], s.Operations[0].Plan.Steps[0])
	if err := s.authorizeLocked(context.Background(), action); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("nil initial convergence was treated as proven evacuation", err)
	}
}

func TestScopedEvacuationFindsNewlyRetainedPreparation(t *testing.T) {
	t.Parallel()
	s := loadAdmissionFixture(t)
	s.Admissions[0].State = AdmissionReady
	admissionPrefixReceipt(&s, 0, 1)
	prepareScopedFixture(t, &s)
	load := scopedPlanAction(&s.Operations[0], s.Operations[0].Plan.Steps[2])
	if _, err := s.applyBeginEffect(s.AppliedIndex+1, BeginEffectProposal{Action: load}); err != nil {
		t.Fatal(err)
	}
	// Exercise the removal predicate with the later desired declaration. The
	// old issued preparation can still have reached its host; it has no native
	// removal or required Stop receipt, and never reached convergence.
	s.Target = s.Target.Clone()
	s.Target.MetaGroup().Replicas = s.Target.MetaGroup().Replicas[:1]
	s.Target.Node("n2").State = topology.NodeExcluded
	s.Target.Group("shard0").Replicas = s.Target.Group("shard0").Replicas[:1]
	removal := topology.AuthorizedStep{GroupID: topology.MetaGroupID, Kind: topology.StepRemoveReplica,
		NodeName: "n2", ReplicaID: 2}
	if err := s.validateLoadEvacuation(removal); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("an intermediate host outside the removal plan was forgotten", err)
	}
}
