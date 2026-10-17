//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func admissionSuccessorFixture(s scopedState, steps []topology.Step) scopedState {
	previous := &s.Operations[len(s.Operations)-1]
	next := previous.Clone()
	previous.Status.State, previous.Status.Blocks[0].State = topology.OperationSuperseded, topology.OperationSuperseded
	next.Status.Version++
	next.Status.ID = topology.MakeOperationID(next.Status.Version, 1)
	next.Plan.Version, next.Plan.Steps = next.Status.Version, steps
	if len(steps) == 0 {
		next.Plan.VoterBases = nil
	}
	next.Plan.Blocks[0].Steps = nil
	for i := range next.Plan.Steps {
		next.Plan.Steps[i].Seq = i + 1
		next.Plan.Blocks[0].Steps = append(next.Plan.Blocks[0].Steps, i+1)
	}
	next.Status.State, next.Status.Blocks[0].State = topology.OperationPending, topology.OperationPending
	next.Status.Blocks[0].CurrentStep, next.Status.Blocks[0].TotalSteps = 0, len(steps)
	if len(steps) != 0 {
		next.Status.Blocks[0].CurrentStep = 1
	}
	s.Operations = append(s.Operations, next)
	s.History = append(s.History, s.Target.Clone())
	s.Target.ParentVersion, s.Target.Version = s.Target.Version, next.Status.Version
	s.Latest = s.Target.Clone()
	s.Groups[0].Owner, s.Nodes[1].Owner = next.Status.ID, next.Status.ID
	if s.Admissions[0].ReadmissionOperatorID == "" {
		s.Admissions[0].ReadmissionOperatorID = topology.OperationID(s.Admissions[0].OperationID)
	}
	s.Admissions[0].OperationID = topology.OperationID(next.Status.ID)
	return s
}

func admissionPrefixReceipt(s *scopedState, operation int, seq int) {
	op := s.Operations[operation]
	step := op.Plan.Steps[seq-1]
	action := topology.AuthorizedStep{OperationID: topology.OperationID(op.Status.ID), Version: op.Status.Version,
		Attempt: op.Status.Attempt, StepSeq: seq, GroupID: step.GroupID, NodeName: step.NodeName,
		ReplicaID: step.ReplicaID, Kind: step.Kind, ExpectedVotersDigest: step.ExpectedVotersDigest,
		TransferNodeName: step.TransferNodeName, TransferReplicaID: step.TransferReplicaID,
		ResultVotersDigest: step.ResultVotersDigest}
	resource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: topology.MetaGroupID}
	if step.Kind == topology.StepPrepareReplica {
		resource.Kind, resource.NodeName = topology.AuthorityHost, step.NodeName
	}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: s.Target.ClusterID,
		GroupID: step.GroupID, Revision: uint64(len(s.AuthorityRecords) + 3)}, Purpose: topology.AuthorityEffect,
		OperationID: action.OperationID, Action: &action, Resources: []topology.AuthorityResource{resource},
		Targets: []topology.AuthorityTarget{{NodeName: step.NodeName, MemberID: step.ReplicaID, ReplicaID: step.ReplicaID}}}
	if step.TransferReplicaID != 0 {
		grant.Targets = append(grant.Targets, topology.AuthorityTarget{NodeName: step.TransferNodeName,
			MemberID: s.Target.MemberID(step.TransferNodeName), ReplicaID: step.TransferReplicaID})
	}
	slices.SortFunc(grant.Targets, topology.AuthorityTarget.Compare)
	receipts := fixtureEffectReceipts(grant)
	if resource.Kind == topology.AuthorityGroup {
		receipts[0].AppliedIndex = 5
	}
	s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "execution",
		authorityRecord: authorityRecord{Grant: grant, Effects: receipts}})
	for i, floor := range s.AuthorityFloors {
		if floor.Resource == resource {
			s.AuthorityFloors[i].Key = grant.Key
			return
		}
	}
	s.AuthorityFloors = append(s.AuthorityFloors, resourceFloor{Resource: resource, Key: grant.Key})
}

func TestScopedAdmissionResumesOnlyConsumedPrefix(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		receipts int
		omit     int
		state    AdmissionState
		valid    bool
	}{
		{"pending full", 0, 0, AdmissionPending, true},
		{"ready is not preparation", 0, 1, AdmissionReady, false},
		{"prepared add suffix", 1, 1, AdmissionReady, true},
		{"prepared is not native", 1, 2, AdmissionReady, false},
		{"prepared is not learner milestone", 1, 1, AdmissionLearner, false},
		{"native zero suffix", 2, 2, AdmissionLearner, true},
		{"native is not promoted", 2, 2, AdmissionPromoted, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			s := scopedAdmissionFixture()
			for i := 1; i <= test.receipts; i++ {
				admissionPrefixReceipt(&s, 0, i)
			}
			s = admissionSuccessorFixture(s, s.Operations[0].Plan.Clone().Steps[test.omit:])
			s.Admissions[0].State = test.state
			before, _ := json.Marshal(s)
			_, err := prepareScopedGraph(&s, s.Rules)
			if (err == nil) != test.valid || err != nil && !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("unexpected prefix result", err)
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("proof validation modified source state")
			}
		})
	}
}

func TestScopedAdmissionPromotionOnlySuccessorAndMultipleHandovers(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	s.Target.MetaGroup().Replicas[1].Role = topology.ReplicaVoter
	s.Latest = s.Target.Clone()
	op := &s.Operations[0]
	op.Plan.Steps = append(op.Plan.Steps,
		topology.Step{Seq: 3, Kind: topology.StepWaitCaughtUp, GroupID: topology.MetaGroupID, NodeName: "n2", ReplicaID: 2},
		topology.Step{Seq: 4, Kind: topology.StepPromoteVoter, GroupID: topology.MetaGroupID, NodeName: "n2", ReplicaID: 2,
			ExpectedVotersDigest: topology.VoterSetDigest([]uint64{1}), ResultVotersDigest: topology.VoterSetDigest([]uint64{1, 2})})
	op.Plan.Blocks[0].Steps, op.Status.Blocks[0].TotalSteps = []int{1, 2, 3, 4}, 4
	admissionPrefixReceipt(&s, 0, 1)
	s = admissionSuccessorFixture(s, s.Operations[0].Plan.Clone().Steps[1:])
	admissionPrefixReceipt(&s, 1, 1)
	s = admissionSuccessorFixture(s, s.Operations[1].Plan.Clone().Steps[1:])
	s.Admissions[0].State = AdmissionLearner
	data, _ := json.Marshal(s)
	var restored scopedState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal("ordered original preparation and successor learner prefix refused", err)
	}
	if restored.Operations[2].Previous != "" || restored.AuthorityRecords[0].Grant.Action.OperationID != "exec-v2-a1" ||
		restored.AuthorityRecords[1].Grant.Action.OperationID != "exec-v3-a1" {
		t.Fatal("handover relabeled original facts or invented retry lineage")
	}
	s.AuthorityRecords[0].Effects = nil
	bad, _ := json.Marshal(s)
	index := restored.indexes
	if err := json.Unmarshal(bad, &restored); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("missing prefix accepted", err)
	}
	after, _ := json.Marshal(restored)
	if !bytes.Equal(data, after) || restored.indexes != index {
		t.Fatal("invalid prefix replaced retained state")
	}
}

func TestScopedAdmissionOriginCannotFollowIssuedPrefix(t *testing.T) {
	t.Parallel()
	for _, issued := range []bool{false, true} {
		s := scopedAdmissionFixture()
		if issued {
			admissionPrefixReceipt(&s, 0, 1)
			s.AuthorityRecords[0].Effects = nil // Issuance already constrains provenance.
		}
		s = admissionSuccessorFixture(s, s.Operations[0].Plan.Clone().Steps)
		s = admissionSuccessorFixture(s, s.Operations[1].Plan.Clone().Steps)
		s.Admissions[0].ReadmissionOperatorID = "exec-v3-a1" // Eligible, but later than the original issued action.
		_, err := prepareScopedGraph(&s, s.Rules)
		if (err != nil) != issued {
			t.Fatal("issued-origin boundary violated", issued, err)
		}
	}
}

func TestScopedAdmissionEarlierConvergenceSuppliesOmittedPrefix(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	s = admissionSuccessorFixture(s, nil)
	s.Groups[0].SourceVersion, s.Groups[0].Declaration = 2, s.History[1].MetaGroup()
	s.Operations[1].Plan.Blocks[0].SourceVersion = 2
	s.Admissions[0].State = AdmissionLearner
	if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("live admission survived its metadata convergence", err)
	}
	// Once durably converged, the checkpoint can replace the admission barrier
	// root; restore must not require already-compacted preparation receipts.
	s.Groups[0].SourceVersion, s.Groups[0].Declaration, s.Groups[0].Owner = 3, s.Target.MetaGroup(), ""
	s.Nodes[1].Owner = ""
	s.Admissions[0].State = AdmissionCompleted
	s.Operations[1].Status.State, s.Operations[1].Status.Blocks[0].State =
		topology.OperationSucceeded, topology.OperationSucceeded
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("durable convergence did not replace compacted admission proof", err)
	}
	s.Groups[0].Declaration = s.History[0].MetaGroup()
	if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("foreign checkpoint proved membership", err)
	}
}

func TestScopedAdmissionSuffixKeepsUnrelatedStepOrdering(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	step := s.Operations[0].Plan.Steps[1]
	suffix := topology.Plan{Steps: []topology.Step{{Kind: topology.StepWaitCaughtUp, GroupID: topology.MetaGroupID,
		NodeName: "n1", ReplicaID: 1}, step}}
	p := admissionProgress{prepared: true}
	if !p.validSuffix(s.Admissions[0], suffix, s.Target) {
		t.Fatal("unrelated earlier work interrupted the proved member prefix")
	}
	s.Target.MetaGroup().Replicas[1].Role = topology.ReplicaVoter
	p.learner, p.promoted, p.nativeRole = true, true, topology.ReplicaVoter
	if !p.validSuffix(s.Admissions[0], topology.Plan{}, s.Target) {
		t.Fatal("proven voter required a fabricated fresh promotion")
	}
}

func TestScopedAdmissionEligibleOriginMustMatchRetirementBatch(t *testing.T) {
	t.Parallel()
	s := admissionOriginFixture()
	previous := &s.Operations[len(s.Operations)-1]
	next := previous.Clone()
	previous.Status.State = topology.OperationFailed
	for i := range previous.Status.Blocks {
		previous.Status.Blocks[i].State = topology.OperationFailed
	}
	next.Previous, next.Status.ID, next.Status.Attempt = previous.Status.ID, "exec-v3-a3", 3
	s.Operations = append(s.Operations, next)
	for i := range s.Groups {
		if s.Groups[i].Owner == next.Previous {
			s.Groups[i].Owner = next.Status.ID
		}
	}
	admission := &s.Admissions[len(s.Admissions)-1]
	admission.OperationID = topology.OperationID(next.Status.ID)
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("retained original batch refused", err)
	}
	admission.ReadmissionOperatorID = next.Previous
	index, err := s.buildIndexes()
	if err != nil || !validAdmissionOrigin(*admission, index) {
		t.Fatal("test must supply a structurally eligible wrong origin", err)
	}
	if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("eligible origin contradicted original AdmissionID without refusal", err)
	}
}
