//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedReseatReplaysExactIntentAcrossAtomicRebase(t *testing.T) {
	t.Parallel()
	s, p := repairCommitFixture(t)
	p.Doc.Origin, p.Doc.ChangedBy, p.Doc.Comment = topology.OriginOperator, "operator", "replace lost replica"
	intent, err := NewIntent("reseat_replica", map[string]any{"node": "n2", "replica": 2, "comment": p.Doc.Comment})
	if err != nil {
		t.Fatal(err)
	}
	intent.Version, intent.ExpectedVersion, intent.GroupID, intent.NodeName, intent.PreviousID, intent.SuccessorID = 3, 2, "b", "n2", 2, 4
	p.Intent = &intent
	encoded, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	result, err := applyScopedProposal(&s, 11, "commit_repair_operation", encoded)
	if err != nil || !result.changed || result.result.Doc.Version != 3 || result.result.OperatorID != "exec-v3-a1" || s.Latest.Version != 4 || len(s.Intents) != 2 {
		t.Fatal(result, err)
	}
	before, _ := json.Marshal(s)
	var restored scopedState
	if err := json.Unmarshal(before, &restored); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		replay, err := applyScopedProposal(&restored, 12, "commit_repair_operation", encoded)
		if err != nil || replay.changed || replay.result.Doc.Version != 3 || replay.result.OperatorID != result.result.OperatorID {
			t.Fatal("reseat retry lost approval or allocated twice", replay, err)
		}
	}
	after, _ := json.Marshal(restored)
	if !bytes.Equal(before, after) {
		t.Fatal("replay changed rebase, allocations or authority")
	}
	wrong := intent
	wrong.PreviousID = 1
	p.Intent = &wrong
	if _, err := restored.applyCommitRepairOperation(12, p); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal("arbitrary predecessor replayed", err)
	}
	wrong = intent
	wrong.SuccessorID = 5
	p.Intent = &wrong
	if _, err := restored.applyCommitRepairOperation(12, p); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal("arbitrary successor replayed", err)
	}
}

func TestScopedReseatRejectsForgedPredecessorBeforeInstall(t *testing.T) {
	t.Parallel()
	s, p := repairCommitFixture(t)
	p.Doc.Origin = topology.OriginOperator
	intent, err := NewIntent("reseat_replica", map[string]string{"node": "n2"})
	if err != nil {
		t.Fatal(err)
	}
	intent.Version, intent.ExpectedVersion, intent.GroupID, intent.NodeName, intent.PreviousID, intent.SuccessorID = 3, 2, "b", "n2", 1, 4
	p.Intent = &intent
	before, _ := json.Marshal(s)
	result, err := s.applyCommitRepairOperation(11, p)
	after, _ := json.Marshal(s)
	if err == nil || result.changed || !bytes.Equal(before, after) {
		t.Fatal("forged tuple created repair/rebase", result, err)
	}
}

func TestScopedReadmissionIssuanceAfterUnrelatedApprovalThenDeletion(t *testing.T) {
	t.Parallel()
	s, deletion := readmissionDeletionFixture(t)
	s.AuthorityRecords, s.AuthorityFloors = nil, nil
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = 3, 2
	s.Latest.Comment = "independent approval"
	prepareScopedFixture(t, &s)
	empty := BeginOperationProposal{Version: 3, Plan: topology.Plan{Version: 3}, At: deletion.At, Actor: "operator", Origin: topology.OriginOperator}
	result, err := s.applyBeginOperation(11, empty)
	if err != nil || !result.changed || s.Target.Version != 3 || s.indexes.groups[topology.MetaGroupID].Owner != "exec-v2-a1" {
		t.Fatal(result, err)
	}
	a := s.Admissions[0]
	issued, err := s.applyBeginReadmissionAuthority(12, BeginReadmissionAuthority{NodeName: a.NodeName, InstanceID: a.InstanceID, MemberID: a.MemberID, PreviousMemberID: 1, OperatorID: a.OperatorID})
	if err != nil || !issued.changed || len(issued.result.Grants) != 2 {
		t.Fatal("whole Target version blocked exact admission", issued, err)
	}
	s.History = append(slices.Clone(s.History), s.Target.Clone())
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = 4, 3
	s.Latest.Groups = slices.DeleteFunc(s.Latest.Groups, func(g topology.Group) bool { return g.ID == "shard0" })
	prepareScopedFixture(t, &s)
	deletion.Version, deletion.Plan.Version, deletion.At = 4, 4, deletion.At.Add(time.Second)
	result, err = s.applyBeginOperation(13, deletion)
	if err != nil || !result.changed || !s.indexes.groups["shard0"].Deleting {
		t.Fatal("issued exact batch did not authorize dependent deletion", result, err)
	}
	encoded, _ := json.Marshal(s)
	var restored scopedState
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
}
