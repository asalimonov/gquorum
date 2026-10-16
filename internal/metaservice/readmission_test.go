//go:build !functional

package metaservice

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

func TestReadmissionResponseRequiresExactWholeGrantBatch(t *testing.T) {
	t.Parallel()
	id := topology.Identity{ClusterID: "cluster", NodeName: "one", InstanceID: "process", MemberID: 1}
	a := metaengine.AdmissionRecord{NodeName: id.NodeName, InstanceID: id.InstanceID, MemberID: 2, OperatorID: "exec-v2-a1"}
	grants := []topology.AuthorityGrant{}
	for _, group := range []string{topology.MetaGroupID, "load"} {
		grants = append(grants, topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: id.ClusterID, GroupID: group, Revision: 10}, Purpose: topology.AuthorityReadmit, AdmissionID: metaengine.ReadmissionID(a), Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: group, NodeName: id.NodeName}}, Targets: []topology.AuthorityTarget{{NodeName: id.NodeName, MemberID: 1, ReplicaID: 1}}})
	}
	valid := joinResponse{MemberID: 2, PreviousMemberID: 1, Admission: a, Readmitted: true, Grants: grants, PreviousGroups: []topology.Group{{ID: "load", Replicas: []topology.Replica{{NodeName: id.NodeName, ReplicaID: 1}}}}}
	if err := validateReadmissionResponse(id, valid); err != nil {
		t.Fatal(err)
	}
	valid.Admission.ReadmissionOperatorID = topology.OperationID(a.OperatorID)
	valid.Admission.OperatorID = "exec-v3-a1"
	if err := validateReadmissionResponse(id, valid); err != nil {
		t.Fatal("same-member handover invalidated the original grant batch", err)
	}
	for name, mutate := range map[string]func(*joinResponse){
		"lost_operator_origin":   func(r *joinResponse) { r.Admission.ReadmissionOperatorID = "" },
		"forged_operator_origin": func(r *joinResponse) { r.Admission.ReadmissionOperatorID = "exec-v1-a1" },
		"no_grants":              func(r *joinResponse) { r.Grants = nil },
		"wrong_admission":        func(r *joinResponse) { r.Grants[1].AdmissionID = "other" },
		"wrong_member":           func(r *joinResponse) { r.Grants[1].Targets[0].MemberID = 2 },
		"wrong_node":             func(r *joinResponse) { r.Grants[1].Targets[0].NodeName = "other" },
		"mixed_revision":         func(r *joinResponse) { r.Grants[1].Key.Revision++ },
		"missing_meta":           func(r *joinResponse) { r.Grants = r.Grants[1:] },
		"missing_load_scope":     func(r *joinResponse) { r.PreviousGroups = nil },
		"scope_without_grant":    func(r *joinResponse) { r.Grants = r.Grants[:1] },
		"wrong_meta_pin":         func(r *joinResponse) { r.Grants[0].Targets[0].ReplicaID = 3 },
		"replacement_in_scope": func(r *joinResponse) {
			r.Granted.Groups = []topology.Group{{ID: "load", Replicas: []topology.Replica{{NodeName: id.NodeName, ReplicaID: 1}}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := valid
			r.Grants = []topology.AuthorityGrant{grants[0].Clone(), grants[1].Clone()}
			mutate(&r)
			if err := validateReadmissionResponse(id, r); err == nil {
				t.Fatal("unsafe retirement scope accepted")
			}
		})
	}
	var receipts []topology.EffectReceipt
	for _, g := range grants {
		receipts = append(receipts, topology.EffectReceipt{Key: g.Key, Resource: g.Resources[0], Target: g.Targets[0], AdmissionID: g.AdmissionID, Outcome: topology.EffectApplied, LocalSequence: 1})
	}
	if err := validateReadmissionReceipts(grants, receipts); err != nil {
		t.Fatal(err)
	}
	if err := validateReadmissionReceipts(grants, receipts[:1]); !errors.Is(err, fault.ErrAuthorityPending) {
		t.Fatal("partial completion accepted", err)
	}
	if err := validateReadmissionReceipts(grants, []topology.EffectReceipt{receipts[0], receipts[0]}); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("duplicate completion accepted", err)
	}
}
