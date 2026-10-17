//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func scopedAdmissionRetryFixture() scopedState {
	s := scopedReadmissionCleanupFixture()
	retry := s.Operations[0].Clone()
	retry.Previous, retry.Status.ID, retry.Status.Attempt = retry.Status.ID, "exec-v3-a2", 2
	retry.Status.State = topology.OperationPending
	for i := range retry.Status.Blocks {
		retry.Status.Blocks[i].State = topology.OperationPending
	}
	s.Operations = append(s.Operations, retry)
	for i := range s.Groups {
		if s.Groups[i].GroupID == "a" || s.Groups[i].GroupID == topology.MetaGroupID {
			s.Groups[i].Owner = retry.Status.ID
		}
	}
	s.Admissions = append(s.Admissions, AdmissionRecord{NodeName: "n", InstanceID: "retried",
		MemberID: 2, OperationID: topology.OperationID(retry.Status.ID), State: AdmissionReady})
	return s
}

func TestScopedAdmissionRetryKeepsConsumedMemberRetirement(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionRetryFixture()
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal("retry readiness lost the predecessor's consumed retirement", err)
	}
	last := len(s.Admissions) - 1
	if restored.Admissions[last] != s.Admissions[last] {
		t.Fatal("retry admission changed on restore")
	}
	for _, record := range restored.AuthorityRecords {
		if record.Grant.Purpose == topology.AuthorityReadmit &&
			record.Grant.AdmissionID == ReadmissionID(s.Admissions[last]) {
			t.Fatal("retirement proof was relabeled to the retry")
		}
	}
	// Retirement alone is not native learner/promotion proof for the new owner.
	s.Admissions[last].State = AdmissionLearner
	if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("retry manufactured learner progress from retirement", err)
	}
}

func TestScopedAdmissionRetryRefusesIncompleteBatchAtomically(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionRetryFixture()
	// Test readiness independently of cleanup's own full-batch requirement.
	beforeReadmission := scopedCodecFixture()
	s.Cleanup[0] = beforeReadmission.Cleanup[0]
	s.AuthorityRecords[0].Effects = beforeReadmission.AuthorityRecords[0].Effects
	good, _ := json.Marshal(s)
	var restored scopedState
	if err := json.Unmarshal(good, &restored); err != nil {
		t.Fatal(err)
	}
	index := restored.indexes
	s.AuthorityRecords[len(s.AuthorityRecords)-1].Effects = nil
	bad, _ := json.Marshal(s)
	if err := json.Unmarshal(bad, &restored); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("retry accepted partial predecessor retirement", err)
	}
	after, _ := json.Marshal(restored)
	if !bytes.Equal(good, after) || restored.indexes != index {
		t.Fatal("rejected retirement replaced existing state or indexes")
	}
}

func TestAdmissionRetirementMatchesExactGrantedMember(t *testing.T) {
	t.Parallel()
	s := scopedReadmissionCleanupFixture()
	index, err := s.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := restoreScopedAuthority(&s, index)
	if err != nil {
		t.Fatal(err)
	}
	admission := s.Admissions[len(s.Admissions)-1]
	if !catalog.admissionRetirementConsumed(admission, "n", 2) {
		t.Fatal("complete retirement refused")
	}
	if catalog.admissionRetirementConsumed(admission, "other", 2) ||
		catalog.admissionRetirementConsumed(admission, "n", 3) {
		t.Fatal("retirement crossed node/member identity")
	}
	admission.OperationID = "exec-v3-a2"
	if catalog.admissionRetirementConsumed(admission, "n", 2) {
		t.Fatal("unrecorded batch accepted")
	}
}
