//go:build !functional

package metaengine

import (
	"testing"

	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func fencedRecoverySeedFixture(t *testing.T) (scopedState, PlanRead, topology.AuthorityGrant) {
	t.Helper()
	s, p := consumedRecoveryFixture(t)
	birth := s.indexes.grants[p.Evidence.GroupReceipt.Key].Grant.Clone()
	fence := birth.Clone()
	fence.Key.Revision++
	fence.Purpose = topology.AuthorityFence
	fence.Action = nil
	fence.OperationID = ""
	target := birth.Targets[0]
	proof := topology.FenceEvidence{Key: fence.Key, Resource: topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: "shard0"},
		Receiver:  topology.Identity{NodeName: target.NodeName, ClusterID: fence.Key.ClusterID, MemberID: target.MemberID, InstanceID: "run"},
		ReplicaID: target.ReplicaID, AppliedIndex: 3, AppliedTerm: 2}
	if err := proof.Validate(fence); err != nil {
		t.Fatal(err)
	}
	s.indexes.grants[fence.Key] = &scopedAuthorityRecord{authorityRecord: authorityRecord{Grant: fence, Fences: []topology.FenceEvidence{proof}}}
	op := s.indexes.operations[s.indexes.first[2]].Clone()
	old := op.Clone()
	op.Previous = old.Status.ID
	op.Status.Attempt = 2
	op.Status.ID = topology.MakeOperationID(2, 2)
	read := PlanRead{Recovery: s.recoveryPlacementState(), Operation: op, Previous: []topology.Operation{old}, Target: s.Target.Clone(),
		Grants: []topology.AuthorityGrant{birth}, Input: topology.PlanInput{Native: map[string]topology.NativeState{
			"shard0": {GroupID: "shard0", ReplicaID: target.ReplicaID, Applied: 3, Grant: &fence,
				ConfState: raftpb.ConfState{Voters: []uint64{target.ReplicaID}}}}}}
	return s, read, fence
}

func TestRecoveryFoundingBasisRequiresExactRetainedNativeFence(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"valid", "unknown fence", "wrong replica", "unconfirmed fence", "before fence", "no birth grant", "wrong target", "after import"} {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			s, read, fence := fencedRecoverySeedFixture(t)
			native := read.Input.Native["shard0"]
			switch shape {
			case "valid":
			case "unknown fence":
				delete(s.indexes.grants, fence.Key)
			case "wrong replica":
				native.ReplicaID++
			case "unconfirmed fence":
				s.indexes.grants[fence.Key].Fences = nil
			case "before fence":
				native.Applied = 2
			case "no birth grant":
				read.Grants = nil
			case "wrong target":
				read.Grants[0].Targets[0].MemberID++
			case "after import":
				s.Recovery.Phase = "complete"
			}
			read.Input.Native["shard0"] = native
			proof := s.recoveryFoundingSeeds(read)
			if (len(proof) == 1) != (shape == "valid") {
				t.Fatal("incorrect recovery founding basis", shape, proof)
			}
		})
	}
}
