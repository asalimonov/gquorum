//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func joiningProposal(s *scopedState) BindAdmissionProposal {
	return BindAdmissionProposal{ProposalID: "bind", At: s.Operations[0].UpdatedAt,
		Admission: AdmissionRecord{NodeName: "n2", MemberID: 2, InstanceID: "first", OperatorID: "exec-v2-a1"}}
}

func readyProposal(a AdmissionRecord) ReadyAdmissionProposal {
	return ReadyAdmissionProposal{ProposalID: "ready", NodeName: a.NodeName, InstanceID: a.InstanceID,
		MemberID: a.MemberID, OperatorID: a.OperatorID}
}

func applyAdmissionFixture(t *testing.T, s *scopedState, kind ProposalKind, p any) proposalApplication {
	t.Helper()
	body, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	result, err := applyScopedProposal(s, s.AppliedIndex+1, kind, body)
	if err != nil || !result.result.Accepted {
		t.Fatal("admission refused", result, err)
	}
	return result
}

func TestScopedJoinBindsPinnedOwnerAfterUnrelatedApproval(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	s.Admissions = nil
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion, s.Latest.Comment = 3, 2, "unrelated"
	prepareScopedFixture(t, &s)
	p := BeginOperationProposal{Version: 3, Plan: topology.Plan{Version: 3}, At: s.Operations[0].UpdatedAt,
		Actor: "operator", Origin: topology.OriginOperator}
	if _, err := s.applyBeginOperation(s.AppliedIndex+1, p); err != nil {
		t.Fatal(err)
	}
	bind := joiningProposal(&s)
	bind.Admission.State = AdmissionPromoted // The caller cannot supply progress.
	bound := applyAdmissionFixture(t, &s, KindBindAdmission, bind)
	if !bound.changed || bound.result.Admission.State != AdmissionPending ||
		bound.result.Admission.OperatorID != "exec-v2-a1" || s.Target.Version != 3 {
		t.Fatal("binding used whole Target or trusted caller progress", bound)
	}
	ready := readyProposal(bound.result.Admission)
	result := applyAdmissionFixture(t, &s, KindReadyAdmission, ready)
	if !result.changed || result.result.Admission.State != AdmissionReady {
		t.Fatal("unrelated approval stranded ready", result)
	}
	before, _ := json.Marshal(s)
	var restored scopedState
	if err := json.Unmarshal(before, &restored); err != nil {
		t.Fatal(err)
	}
	if applyAdmissionFixture(t, &restored, KindBindAdmission, bind).changed ||
		applyAdmissionFixture(t, &restored, KindReadyAdmission, ready).changed {
		t.Fatal("idempotent response rewrote admission")
	}
	after, _ := json.Marshal(restored)
	if !bytes.Equal(before, after) {
		t.Fatal("replay changed retained state")
	}
}

func TestScopedJoinProcessReplacementPreservesHandoverOrigin(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	s.Admissions = nil
	prepareScopedFixture(t, &s)
	bound := applyAdmissionFixture(t, &s, KindBindAdmission, joiningProposal(&s)).result.Admission
	s, p := admissionApprovalFixture(t, s, 0)
	applyScopedApprovalFixture(t, &s, p)
	bind := joiningProposal(&s)
	bind.Admission.InstanceID, bind.Admission.OperatorID, bind.ReplaceInstance = "second", "exec-v3-a1", true
	replacement := applyAdmissionFixture(t, &s, KindBindAdmission, bind).result.Admission
	if replacement.ReadmissionInstanceID != bound.InstanceID ||
		replacement.ReadmissionOperatorID != topology.OperationID(bound.OperatorID) ||
		ReadmissionID(replacement) != ReadmissionID(bound) || s.Admissions[0].State != AdmissionSuperseded {
		t.Fatal("process replacement lost the original batch identity", s.Admissions)
	}
	for _, late := range []AdmissionRecord{bound, s.Admissions[0]} {
		if _, err := s.applyReadyAdmission(s.AppliedIndex+1, readyProposal(late)); err == nil {
			t.Fatal("old process gained readiness", late)
		}
	}
	// A further owner handover keeps both origins, and rejects a late response
	// naming the prior operator even though the process itself is unchanged.
	s, p = admissionApprovalFixture(t, s, 0)
	s.Latest.Node("n2").Labels["owner"] = "third"
	prepareScopedFixture(t, &s)
	applyScopedApprovalFixture(t, &s, p)
	if _, err := s.applyReadyAdmission(s.AppliedIndex+1, readyProposal(replacement)); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("late prior-owner Ready rebound itself", err)
	}
	current := s.Admissions[1]
	if ReadmissionID(current) != ReadmissionID(bound) || current.OperatorID != "exec-v4-a1" {
		t.Fatal("second handover changed the origin", current)
	}
	applyAdmissionFixture(t, &s, KindReadyAdmission, readyProposal(current))
	encoded, _ := json.Marshal(s)
	var restored scopedState
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
}

func TestScopedJoinRefusesStaleAndPreparedProcessReplacement(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*scopedState, *BindAdmissionProposal){
		"member":                 func(_ *scopedState, p *BindAdmissionProposal) { p.Admission.MemberID = 3 },
		"owner":                  func(_ *scopedState, p *BindAdmissionProposal) { p.Admission.OperatorID = "exec-v2-a2" },
		"process":                func(_ *scopedState, p *BindAdmissionProposal) { p.Admission.InstanceID = "" },
		"process origin":         func(_ *scopedState, p *BindAdmissionProposal) { p.Admission.ReadmissionInstanceID = "forged" },
		"operator origin":        func(_ *scopedState, p *BindAdmissionProposal) { p.Admission.ReadmissionOperatorID = "exec-v1-a1" },
		"timestamp":              func(_ *scopedState, p *BindAdmissionProposal) { p.At = time.Time{} },
		"replace flag":           func(_ *scopedState, p *BindAdmissionProposal) { p.ReplaceInstance = false },
		"ready process":          func(s *scopedState, _ *BindAdmissionProposal) { s.Admissions[0].State = AdmissionReady },
		"consumed before cursor": func(s *scopedState, _ *BindAdmissionProposal) { admissionPrefixReceipt(s, 0, 1) },
		"failed metadata": func(s *scopedState, _ *BindAdmissionProposal) {
			s.Operations[0].Status.State, s.Operations[0].Status.Blocks[0].State = topology.OperationFailed, topology.OperationFailed
			s.Groups[0].Owner, s.Nodes[1].Owner = "", ""
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := scopedAdmissionFixture()
			p := joiningProposal(&s)
			p.ReplaceInstance = true
			mutate(&s, &p)
			prepareScopedFixture(t, &s)
			before, _ := json.Marshal(s)
			if result, err := s.applyBindAdmission(s.AppliedIndex+1, p); err == nil || result.changed {
				t.Fatal("invalid process acquired admission", result, err)
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("refusal retired the prior process or changed authority")
			}
		})
	}
}

func TestScopedReadyRequiresWholeReadmissionRetirement(t *testing.T) {
	t.Parallel()
	s, _ := readmissionDeletionFixture(t)
	p := readyProposal(s.Admissions[0])
	grants := append([]scopedAuthorityRecord(nil), s.AuthorityRecords...)
	for _, grant := range grants {
		before, _ := json.Marshal(s)
		if _, err := s.applyReadyAdmission(s.AppliedIndex+1, p); !errors.Is(err, fault.ErrAuthorityPending) {
			t.Fatal("partial old-owner retirement allowed readiness", err)
		}
		after, _ := json.Marshal(s)
		if !bytes.Equal(before, after) {
			t.Fatal("pending retirement changed admission")
		}
		recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: fixtureEffectReceipts(grant.Grant)[0],
			At: s.Operations[0].UpdatedAt})
	}
	result := applyAdmissionFixture(t, &s, KindReadyAdmission, p)
	if result.result.Admission.State != AdmissionReady || !result.changed {
		t.Fatal("whole retirement did not unblock the exact process", result)
	}
}
