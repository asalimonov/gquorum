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

func failScopedMetadata(t *testing.T, s *scopedState) {
	t.Helper()
	prepareScopedFixture(t, s)
	op := s.indexes.operations[s.indexes.latest[s.Target.Version]]
	transition := scopedTransitionFor(*op, topology.MetaGroupID, op.Status.Blocks[0].CurrentStep)
	transition.State, transition.Error = topology.OperationFailed, "offline"
	if _, err := s.applyTransitionOperation(s.AppliedIndex+1, TransitionOperationProposal{Transition: transition}); err != nil {
		t.Fatal(err)
	}
}

func TestScopedRetryDerivesAdmissionProgressWithoutReplayingConsumedPrefix(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"pending", "prepared", "learner", "promoted", "unacknowledged promotion", "replacement process"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			s := scopedAdmissionFixture()
			want, cursor := AdmissionPending, 1
			if phase == "promoted" || phase == "unacknowledged promotion" {
				s = promotedAdmissionFixture()
				want, cursor = AdmissionPromoted, 5
				if phase == "unacknowledged promotion" {
					s.Operations[0].Status.Blocks[0].CurrentStep = 4
					s.Admissions[0].State = AdmissionLearner
				}
			} else if phase != "pending" {
				admissionPrefixReceipt(&s, 0, 1)
				s.Admissions[0].State, want, cursor = AdmissionReady, AdmissionReady, 2
				if phase == "learner" || phase == "replacement process" {
					admissionPrefixReceipt(&s, 0, 2)
					s.Operations[0].Status.Blocks[0].CurrentStep = 2
					s.Admissions[0].State, want = AdmissionReady, AdmissionLearner
				}
			}
			if phase == "replacement process" {
				old := s.Admissions[0]
				old.State = AdmissionSuperseded
				s.Admissions = append(s.Admissions, old)
				s.Admissions[0].InstanceID, s.Admissions[0].ReadmissionInstanceID = "new-process", old.InstanceID
				s.Admissions[0].State, want = AdmissionPending, AdmissionPending
			}
			failScopedMetadata(t, &s)
			original := s.Operations[0].Clone()
			before := s.Admissions[0]
			result := applyScopedRetryFixture(t, &s, scopedRetryProposal(&s, &original))
			retry := s.indexes.operations[topology.OperationID(result.result.OperatorID)]
			if retry.Status.Blocks[0].CurrentStep != cursor || s.Admissions[0].State != want ||
				s.Admissions[0].OperatorID != result.result.OperatorID ||
				s.Admissions[0].ReadmissionOperatorID != original.Status.ID ||
				ReadmissionID(s.Admissions[0]) != ReadmissionID(before) || !reflect.DeepEqual(retry.Plan, original.Plan) {
				t.Fatal("retry lost admission origin, immutable prefix or process readiness", s.Admissions, retry)
			}
			if phase == "unacknowledged promotion" && (len(s.VoterSwitches) != 1 ||
				s.VoterSwitches[0].Last.OperatorID != topology.OperatorID(original.Status.ID)) {
				t.Fatal("retry skipped original promotion cooldown")
			}
			for _, record := range s.AuthorityRecords {
				if record.Grant.OperatorID == result.result.OperatorID {
					t.Fatal("retry manufactured effect consumption under its new identity")
				}
			}
		})
	}
}

func TestScopedRetryRetainsUnobservedCatchupAndScopesRevocations(t *testing.T) {
	t.Parallel()
	s := promotedAdmissionFixture()
	s.Operations[0].Status.Blocks[0].CurrentStep = 3
	s.Admissions[0].State = AdmissionLearner
	// Promotion has not run yet. Its earlier fixture proof must not skip catchup.
	s.AuthorityRecords = s.AuthorityRecords[:2]
	for i := range s.AuthorityFloors {
		if s.AuthorityFloors[i].Resource.Kind == topology.AuthorityGroup {
			s.AuthorityFloors[i].Key = s.AuthorityRecords[1].Grant.Key
		}
	}
	failScopedMetadata(t, &s)
	old := s.Operations[0].Clone()
	result := applyScopedRetryFixture(t, &s, scopedRetryProposal(&s, &old))
	retry := s.indexes.operations[topology.OperationID(result.result.OperatorID)]
	if retry.Status.Blocks[0].CurrentStep != 3 {
		t.Fatal("retry fabricated a catchup observation")
	}
	resources := s.successorResources(retry)
	host := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: topology.MetaGroupID, NodeName: "n2"}
	if resources[host] || !resources[topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: topology.MetaGroupID}] {
		t.Fatal("retry resource selection included consumed preparation or omitted live membership")
	}
}

func TestScopedRetryPreservesWholeReadmissionBatchAndFences(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"pending", "partial", "ready", "fenced"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			s, _ := readmissionDeletionFixture(t)
			grants := append([]scopedAuthorityRecord(nil), s.AuthorityRecords...)
			for i, record := range grants {
				if phase == "ready" || phase == "partial" && i == 0 {
					recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: fixtureEffectReceipts(record.Grant)[0],
						At: s.Operations[0].UpdatedAt})
				}
			}
			if phase == "ready" {
				s.Admissions[0].State = AdmissionReady
			}
			if phase == "fenced" {
				for i := range s.AuthorityRecords {
					s.AuthorityRecords[i].Closed = true
				}
			}
			failScopedMetadata(t, &s)
			op := s.Operations[0]
			failure := scopedTransitionFor(op, "shard0", 7)
			failure.State, failure.Error = topology.OperationFailed, "unavailable"
			applyScopedTransitionFixture(t, &s, failure)
			original := s.Admissions[0]
			before, _ := json.Marshal(s)
			p := scopedRetryProposal(&s, &s.Operations[0])
			if phase == "fenced" {
				if _, err := s.applyRetryOperation(s.AppliedIndex+1, p); !errors.Is(err, fault.ErrAuthorityFenced) {
					t.Fatal("retry revived canceled retirement", err)
				}
				after, _ := json.Marshal(s)
				if !bytes.Equal(before, after) {
					t.Fatal("refused retirement partially rebound admission")
				}
				return
			}
			result := applyScopedRetryFixture(t, &s, p)
			admission := s.Admissions[0]
			if ReadmissionID(admission) != ReadmissionID(original) || admission.OperatorID != result.result.OperatorID ||
				(scopedAdmissionRetirement(admission, &s, s.indexes, s.authorityCatalog())) != (phase == "ready") {
				t.Fatal("retry lost original batch or trusted partial retirement", admission)
			}
			replay, err := s.applyBeginReadmissionAuthority(s.AppliedIndex+1, BeginReadmissionAuthority{
				NodeName: admission.NodeName, InstanceID: admission.InstanceID, MemberID: admission.MemberID,
				PreviousMemberID: 1, OperatorID: admission.OperatorID})
			if err != nil || replay.changed || len(replay.result.Grants) != 2 {
				t.Fatal("retry could not recover the exact original retirement batch", replay, err)
			}
		})
	}
}

func TestScopedRetryBeforeProcessBindsAdmission(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	s.Admissions = nil
	failScopedMetadata(t, &s)
	result := applyScopedRetryFixture(t, &s, scopedRetryProposal(&s, &s.Operations[0]))
	op := s.indexes.operations[topology.OperationID(result.result.OperatorID)]
	if len(s.Admissions) != 0 || op.Status.Blocks[0].CurrentStep != 1 {
		t.Fatal("retry fabricated an admission or skipped preparation")
	}
	transition := scopedTransitionFor(*op, topology.MetaGroupID, 1)
	applyScopedTransitionFixture(t, &s, transition)
	if _, err := s.applyBeginEffect(s.AppliedIndex+1, BeginEffectProposal{
		Action: scopedPlanAction(op, op.Plan.Steps[0])}); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("retry approval bypassed actual admission binding", err)
	}
	bind := joiningProposal(&s)
	if _, err := s.applyBindAdmission(s.AppliedIndex+1, bind); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("fresh process selected the failed attempt", err)
	}
	bind.Admission.OperatorID = result.result.OperatorID
	bound := applyAdmissionFixture(t, &s, KindBindAdmission, bind)
	applyAdmissionFixture(t, &s, KindReadyAdmission, readyProposal(bound.result.Admission))
	if _, err := s.applyBeginEffect(s.AppliedIndex+1, BeginEffectProposal{
		Action: scopedPlanAction(op, op.Plan.Steps[0])}); err != nil {
		t.Fatal("exact retry admission could not prepare", err)
	}
}
