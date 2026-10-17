//go:build !functional

package load_test

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func retirementFixture() (storage.Identity, load.RetirementRequest) {
	id := storage.Identity{ClusterID: "c", GroupID: "work", NodeName: "n1", ReplicaID: 4}
	step := topology.AuthorizedStep{OperationID: topology.MakeOperationID(2, 1), Version: 2, Attempt: 1, StepSeq: 3, Kind: topology.StepCleanupReplicaState, GroupID: "work", NodeName: "n1", ReplicaID: 4}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "c", GroupID: "work", Revision: 9}, Purpose: topology.AuthorityEffect, OperationID: step.OperationID, Action: &step,
		Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: "work", NodeName: "n1"}},
		Targets:   []topology.AuthorityTarget{{NodeName: "n1", MemberID: 7, ReplicaID: 4}}}
	authorization := step
	return id, load.RetirementRequest{Grant: grant, Identity: id, Reason: load.RetirementTopology, Authorization: &authorization}
}

func TestRetirementVerifyAcceptsCompleteTopologyProof(t *testing.T) {
	t.Parallel()
	id, req := retirementFixture()
	if err := req.Verify(id); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*storage.Identity, *load.RetirementRequest){
		"other identity": func(id *storage.Identity, _ *load.RetirementRequest) { id.ReplicaID = 5 },
		"other cluster":  func(_ *storage.Identity, r *load.RetirementRequest) { r.Grant.Key.ClusterID = "x" },
		"missing host": func(_ *storage.Identity, r *load.RetirementRequest) {
			r.Grant.Resources = []topology.AuthorityResource{{Kind: topology.AuthorityGroup, GroupID: "work"}}
		},
		"authorization differs": func(_ *storage.Identity, r *load.RetirementRequest) {
			a := *r.Authorization
			a.StepSeq = 9
			r.Authorization = &a
		},
		"no authorization": func(_ *storage.Identity, r *load.RetirementRequest) { r.Authorization = nil },
		"admission fields": func(_ *storage.Identity, r *load.RetirementRequest) { r.AdmissionID = "a" },
		"unknown reason":   func(_ *storage.Identity, r *load.RetirementRequest) { r.Reason = "other" },
	}
	for name, edit := range cases {
		id, req := retirementFixture()
		edit(&id, &req)
		if err := req.Verify(id); !errors.Is(err, fault.ErrOperationAuthorization) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestRetirementVerifyReadmission(t *testing.T) {
	t.Parallel()
	id, _ := retirementFixture()
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "c", GroupID: "work", Revision: 9}, Purpose: topology.AuthorityReadmit, AdmissionID: "adm",
		Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: "work", NodeName: "n1"}},
		Targets:   []topology.AuthorityTarget{{NodeName: "n1", MemberID: 7, ReplicaID: 4}}}
	req := load.RetirementRequest{Grant: grant, Identity: id, Reason: load.RetirementReadmission, AdmissionID: "adm", PreviousMemberID: 7, GrantedMemberID: 8}
	if err := req.Verify(id); err != nil {
		t.Fatal(err)
	}
	req.GrantedMemberID = 7
	if err := req.Verify(id); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("non-increasing member accepted", err)
	}
}

func TestRetirementResultEchoesOwnedProof(t *testing.T) {
	t.Parallel()
	_, req := retirementFixture()
	result := req.Result()
	if !result.Verified || result.Identity != req.Identity || result.Reason != req.Reason || !result.Grant.Equal(req.Grant) || *result.Authorization != *req.Authorization {
		t.Fatalf("result does not echo the request: %+v", result)
	}
	result.Authorization.StepSeq = 99
	result.Grant.Targets[0].MemberID = 99
	if req.Authorization.StepSeq == 99 || req.Grant.Targets[0].MemberID == 99 {
		t.Fatal("result aliases the request")
	}
}
