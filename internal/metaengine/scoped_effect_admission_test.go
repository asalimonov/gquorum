//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func loadAdmissionFixture(t *testing.T) scopedState {
	t.Helper()
	s := scopedAdmissionFixture()
	s.Target.Group("shard0").Replicas = append(s.Target.Group("shard0").Replicas,
		topology.Replica{NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaLearner})
	s.Target.Group("shard0").NextReplicaID, s.Allocations[1].NextReplicaID = 3, 3
	s.Latest = s.Target.Clone()
	op := &s.Operations[0]
	op.Plan.Steps = append(op.Plan.Steps, topology.Step{Seq: 3, Kind: topology.StepPrepareReplica,
		GroupID: "shard0", NodeName: "n2", ReplicaID: 2})
	op.Plan.Blocks = append(op.Plan.Blocks, topology.PlanBlock{GroupID: "shard0", SourceVersion: 1, Steps: []int{3}})
	op.Status.State, op.Status.TotalBlocks = topology.OperationRunning, 2
	op.Status.Blocks = append(op.Status.Blocks, topology.BlockStatus{GroupID: "shard0",
		State: topology.OperationRunning, CurrentStep: 3, TotalSteps: 1})
	s.Groups[1].Owner = op.Status.ID
	prepareScopedFixture(t, &s)
	return s
}

func TestScopedLoadIssuanceRequiresConsumedMetadataPreparationWithoutEdge(t *testing.T) {
	t.Parallel()
	for _, proof := range []string{"missing", "ready", "issued", "consumed"} {
		t.Run(proof, func(t *testing.T) {
			t.Parallel()
			s := loadAdmissionFixture(t)
			if proof != "missing" {
				s.Admissions[0].State = AdmissionReady
			}
			if proof == "issued" || proof == "consumed" {
				admissionPrefixReceipt(&s, 0, 1)
				if proof == "issued" {
					s.AuthorityRecords[0].Effects = nil
				}
			}
			prepareScopedFixture(t, &s)
			action := scopedPlanAction(&s.Operations[0], s.Operations[0].Plan.Steps[2])
			before, _ := json.Marshal(s)
			result, err := s.applyBeginEffect(11, BeginEffectProposal{Action: action})
			if proof == "consumed" {
				if err != nil || !result.changed || result.result.Grant.Targets[0].MemberID != 2 {
					t.Fatal("exact prepared member did not authorize load preparation", result, err)
				}
			} else {
				if !errors.Is(err, fault.ErrUnavailable) {
					t.Fatal("unproven preparation was not retryable", result, err)
				}
				after, _ := json.Marshal(s)
				if !bytes.Equal(before, after) {
					t.Fatal("waiting for admission mutated authority")
				}
			}
		})
	}
}

func TestScopedLoadAdmissionUsesCurrentBindingAndEveryFoundingVoter(t *testing.T) {
	t.Parallel()
	s := loadAdmissionFixture(t)
	action := scopedPlanAction(&s.Operations[0], s.Operations[0].Plan.Steps[2])
	admissionPrefixReceipt(&s, 0, 1)
	s.Admissions[0].State = AdmissionReady
	prepareScopedFixture(t, &s)
	if err := s.validateActionBarriers(&s.Operations[0], action); err != nil {
		t.Fatal(err)
	}
	// The immutable approval continues to name member 2 while a later desired
	// declaration names member 3; no old receipt grants the new binding.
	s.Target = s.Target.Clone()
	s.Target.MetaGroup().Replicas[1].ReplicaID = 3
	if err := s.validateActionBarriers(&s.Operations[0], action); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("old preparation authorized reseated host", err)
	}
	s = loadAdmissionFixture(t)
	s.Target.Group("shard0").Replicas[1].Role = topology.ReplicaVoter
	s.Latest = s.Target.Clone()
	s.indexes.documents[2] = s.Target.Clone()
	action.Kind, action.NodeName, action.ReplicaID = topology.StepCreateFoundingGroup, "", 0
	if err := s.validateActionBarriers(&s.Operations[0], action); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("one proven founding voter hid an unprepared voter", err)
	}
	admissionPrefixReceipt(&s, 0, 1)
	s.Admissions[0].State = AdmissionReady
	index, err := s.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	s.indexes = index
	if err := s.validateActionBarriers(&s.Operations[0], action); err != nil {
		t.Fatal("all exact founding voters prepared", err)
	}
}

func TestScopedReadmissionIssuanceSurvivesUnrelatedTargetAdvance(t *testing.T) {
	t.Parallel()
	s, _ := readmissionDeletionFixture(t)
	s.AuthorityRecords, s.AuthorityFloors = nil, nil
	s.History = append(s.History, s.Target.Clone())
	s.Target.Version, s.Target.ParentVersion = 3, 2
	s.Latest = s.Target.Clone()
	prepareScopedFixture(t, &s)
	p := BeginReadmissionAuthority{NodeName: "n1", InstanceID: "process", MemberID: 2,
		PreviousMemberID: 1, OperationID: "exec-v2-a1", ProposalID: "readmit"}
	before, _ := json.Marshal(s)
	if _, err := s.applyBeginReadmissionAuthority(s.AppliedIndex, p); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("stale application index issued a batch", err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("failed batch preparation installed partial grants")
	}
	result, err := s.applyBeginReadmissionAuthority(11, p)
	if err != nil || !result.changed || len(result.result.Grants) != 2 {
		t.Fatal("older exact admission could not issue its batch", result, err)
	}
	for _, grant := range result.result.Grants {
		if grant.Key.Revision != 11 || grant.AdmissionID != ReadmissionID(s.Admissions[0]) ||
			!reflect.DeepEqual(grant.Targets, []topology.AuthorityTarget{{NodeName: "n1", MemberID: 1, ReplicaID: 1}}) {
			t.Fatal("batch mixed index or former identities", grant)
		}
	}
	retry, err := s.applyBeginReadmissionAuthority(12, p)
	if err != nil || retry.changed || !reflect.DeepEqual(retry.result.Grants, result.result.Grants) {
		t.Fatal("readmission retry changed the issued batch", retry, err)
	}
	before, _ = json.Marshal(s)
	p.PreviousMemberID = 0
	if _, err := s.applyBeginReadmissionAuthority(12, p); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("forged previous member accepted", err)
	}
	after, _ = json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("refused readmission changed authority")
	}
}

func TestScopedMetadataPrepareWaitsForJoiningProcess(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	admission := s.Admissions[0]
	s.Admissions = nil
	s.Operations[0].Status.State = topology.OperationRunning
	s.Operations[0].Status.Blocks[0].State = topology.OperationRunning
	prepareScopedFixture(t, &s)
	action := scopedPlanAction(&s.Operations[0], s.Operations[0].Plan.Steps[0])
	before, _ := json.Marshal(s)
	if _, err := s.applyBeginEffect(11, BeginEffectProposal{Action: action}); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("approved joining member permanently failed before binding", err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("waiting for the joining process changed authority")
	}
	s.Admissions = []AdmissionRecord{admission}
	prepareScopedFixture(t, &s)
	if result, err := s.applyBeginEffect(11, BeginEffectProposal{Action: action}); err != nil || result.result.Grant == nil {
		t.Fatal("same operation could not resume after exact binding", result, err)
	}
}
