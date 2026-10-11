//go:build !functional

package managed

import (
	"testing"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestRetirementProofRequiresTheCompleteAuthorizedTuple(t *testing.T) {
	t.Parallel()
	a := topology.AuthorizedStep{OperatorID: "exec-v2-a1", Version: 2, Attempt: 1, StepSeq: 3, Kind: topology.StepCleanupReplicaState, GroupID: "work", NodeName: "a", ReplicaID: 7}
	req := load.RetirementRequest{Identity: storage.Identity{NodeName: "a", ClusterID: "cluster", GroupID: "work", ReplicaID: 7}, Reason: load.RetirementTopology, Authorization: &a}
	for _, mutate := range []func(*load.RetirementResult){
		func(r *load.RetirementResult) { r.Verified = false },
		func(r *load.RetirementResult) { r.Identity.NodeName = "foreign" },
		func(r *load.RetirementResult) { r.Identity.ClusterID = "foreign" },
		func(r *load.RetirementResult) { r.Identity.GroupID = "foreign" },
		func(r *load.RetirementResult) { r.Identity.ReplicaID++ },
		func(r *load.RetirementResult) { r.Reason = load.RetirementReadmission },
		func(r *load.RetirementResult) { r.AdmissionID = "forged" },
		func(r *load.RetirementResult) { r.PreviousMemberID = 1 },
		func(r *load.RetirementResult) { r.GrantedMemberID = 2 },
		func(r *load.RetirementResult) { r.Authorization.OperatorID = "other" },
		func(r *load.RetirementResult) { r.Authorization.Version++ },
		func(r *load.RetirementResult) { r.Authorization.Attempt++ },
		func(r *load.RetirementResult) { r.Authorization.StepSeq++ },
		func(r *load.RetirementResult) { r.Authorization.Kind = topology.StepStopReplica },
		func(r *load.RetirementResult) { r.Authorization.GroupID = "other" },
		func(r *load.RetirementResult) { r.Authorization.NodeName = "other" },
		func(r *load.RetirementResult) { r.Authorization.ReplicaID++ },
		func(r *load.RetirementResult) { r.Authorization = nil },
	} {
		result := retirementProof(req)
		mutate(&result)
		if matchingRetirement(req, result) {
			t.Fatal("accepted incomplete identity", result)
		}
	}
	if !matchingRetirement(req, retirementProof(req)) {
		t.Fatal("rejected exact proof")
	}
}
