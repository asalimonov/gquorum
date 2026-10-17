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

func TestReadmissionOriginDimensionsPreserveGrantIdentity(t *testing.T) {
	t.Parallel()
	original := AdmissionRecord{NodeName: "node/one", InstanceID: "first", MemberID: 7, OperationID: "exec-v2-a1"}
	want := "exec-v2-a1/7/8:node/one/first"
	for _, owner := range []topology.OperationID{"exec-v2-a2", "exec-v9-a1"} {
		a := original
		a.ReadmissionOperatorID, a.OperationID = topology.OperationID(a.OperationID), owner
		if ReadmissionID(a) != want {
			t.Fatal("owner handover changed retirement identity")
		}
		a.ReadmissionInstanceID, a.InstanceID = a.InstanceID, "second"
		encoded, err := json.Marshal(a)
		var restored AdmissionRecord
		if err != nil || json.Unmarshal(encoded, &restored) != nil || restored != a ||
			!bytes.Contains(encoded, []byte(`"execution_id"`)) || ReadmissionID(a) != want {
			t.Fatal("process change or disk projection lost origin", a)
		}
		old := a
		old.InstanceID, old.ReadmissionInstanceID, old.State = "first", "", AdmissionSuperseded
		old.OperationID, old.ReadmissionOperatorID = original.OperationID, ""
		if err := validateReadmissionOrigins([]AdmissionRecord{a}, map[string]AdmissionRecord{
			admissionKey(old.NodeName, old.InstanceID): old,
		}); err != nil {
			t.Fatal("different current operators invalidated the shared origin", err)
		}
	}
	if ReadmissionID(original) != want {
		t.Fatal("original encoding changed")
	}
}

func admissionOriginFixture() scopedState {
	s := scopedAdmissionRetryFixture()
	last := len(s.Admissions) - 1
	current, original := s.Admissions[last], s.Admissions[last-1]
	current.InstanceID = original.InstanceID
	current.ReadmissionOperatorID = topology.OperationID(original.OperationID)
	s.Admissions[last-1], s.Admissions = current, s.Admissions[:last]
	// Include debt whose only original authority is the readmission batch.
	debt := readmissionCreatedCleanup().Cleanup[2]
	s.Cleanup = append(s.Cleanup, debt)
	return s
}

func TestScopedAdmissionOriginKeepsExactConsumedRetirement(t *testing.T) {
	t.Parallel()
	s := admissionOriginFixture()
	good, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(good, &restored); err != nil {
		t.Fatal("same-process handover lost origin proof", err)
	}
	admission := restored.Admissions[len(restored.Admissions)-1]
	if admission.ReadmissionOperatorID != "exec-v3-a1" || admission.OperationID != "exec-v3-a2" {
		t.Fatal("restore lost independent ownership and origin", admission)
	}
	catalog, err := restoreScopedAuthority(&restored, restored.indexes)
	if err != nil || !catalog.admissionRetirementConsumed(admission, admission.NodeName, admission.MemberID) {
		t.Fatal("handover lost the complete original batch", err)
	}
	for name, corrupt := range map[string]func(*scopedState){
		"omitted": func(s *scopedState) { s.Admissions[len(s.Admissions)-1].ReadmissionOperatorID = "" },
		"self": func(s *scopedState) {
			a := &s.Admissions[len(s.Admissions)-1]
			a.ReadmissionOperatorID = topology.OperationID(a.OperationID)
		},
		"missing":        func(s *scopedState) { s.Admissions[len(s.Admissions)-1].ReadmissionOperatorID = "exec-v99-a1" },
		"foreign member": func(s *scopedState) { s.Admissions[len(s.Admissions)-1].ReadmissionOperatorID = "exec-v1-a1" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			bad := admissionOriginFixture()
			corrupt(&bad)
			data, _ := json.Marshal(bad)
			var retained scopedState
			if err := json.Unmarshal(good, &retained); err != nil {
				t.Fatal(err)
			}
			index := retained.indexes
			if err := json.Unmarshal(data, &retained); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("corrupt origin accepted", err)
			}
			after, _ := json.Marshal(retained)
			if !bytes.Equal(good, after) || retained.indexes != index {
				t.Fatal("invalid origin partially replaced retained state")
			}
		})
	}
}

func TestScopedAdmissionOriginCrossesVersionsWithoutRetryLink(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	origin := &s.Operations[0]
	next := origin.Clone()
	origin.Status.State, origin.Status.Blocks[0].State = topology.OperationSuperseded, topology.OperationSuperseded
	next.Status.ID, next.Status.Version, next.Plan.Version = "exec-v3-a1", 3, 3
	s.Operations = append(s.Operations, next)
	s.History = append(s.History, s.Target.Clone())
	s.Target.Version, s.Target.ParentVersion = 3, 2
	s.Latest = s.Target.Clone()
	s.Groups[0].Owner, s.Nodes[1].Owner = next.Status.ID, next.Status.ID
	s.Admissions[0].ReadmissionOperatorID = "exec-v2-a1"
	s.Admissions[0].OperationID = topology.OperationID(next.Status.ID)
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal("same-member version handover required a false immutable retry", err)
	}
	if restored.Operations[1].Previous != "" || restored.Admissions[0] != s.Admissions[0] {
		t.Fatal("restore fabricated retry ancestry or lost origin")
	}
}
