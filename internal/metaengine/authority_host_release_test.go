//go:build !functional

package metaengine

import (
	"encoding/json"
	"testing"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestHostReleaseWaitsForExactRetirementAndAllHostAuthority(t *testing.T) {
	t.Parallel()
	m := mustMachine(t, testRules(), nil)
	s := m.scoped
	s.Target = baseDoc()
	s.indexes.grants = make(map[topology.AuthorityKey]*scopedAuthorityRecord)
	insert := func(grant topology.AuthorityGrant) {
		s.indexes.grants[grant.Key] = &scopedAuthorityRecord{authorityRecord: authorityRecord{Grant: grant}}
		s.AuthorityFloors = append(s.AuthorityFloors, resourceFloor{Resource: grant.Resources[0], Key: grant.Key})
	}
	target := topology.AuthorityTarget{NodeName: "n1", MemberID: 1, ReplicaID: 1}
	a := topology.AuthorizedStep{OperationID: topology.MakeOperationID(2, 1), Version: 2, Attempt: 1, StepSeq: 2, Kind: topology.StepStopReplica, GroupID: topology.MetaGroupID, NodeName: "n1", ReplicaID: 1}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: s.Target.ClusterID, GroupID: topology.MetaGroupID, Revision: 10}, Purpose: topology.AuthorityEffect, OperationID: a.OperationID, Action: &a,
		Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: topology.MetaGroupID, NodeName: "n1"}}, Targets: []topology.AuthorityTarget{target}}
	insert(grant)
	check := func(want bool) {
		t.Helper()
		data, _ := json.Marshal(QueryRequest{Kind: "authority_proof", AuthorityKey: &grant.Key})
		result, err := m.Query(t.Context(), data)
		if err != nil {
			t.Fatal(err)
		}
		var proof AuthorityProof
		if err = json.Unmarshal(result.Data, &proof); err != nil || (len(proof.ReleasedHosts) == 1) != want || want && proof.ReleasedHosts[0] != target {
			t.Fatal("incorrect host release", proof, err)
		}
	}
	check(false) // stop not recorded; approved topology still assigns member
	s.indexes.grants[grant.Key].Effects = fixtureEffectReceipts(grant)
	check(false)
	s.Target.Version++
	s.Target.Group(topology.MetaGroupID).Replicas = s.Target.Group(topology.MetaGroupID).Replicas[1:]
	s.Cleanup = []cleanupRecord{{Identity: storage.Identity{ClusterID: s.Target.ClusterID,
		GroupID: "shard0", NodeName: "n1", ReplicaID: 1}, MemberID: 1}}
	check(false) // The host is still needed even before a cleanup grant exists.
	stopped := topology.EffectReceipt{Target: target}
	s.Cleanup[0].Stopped = &stopped
	check(false) // Joined stop alone does not quarantine retained application data.
	s.Cleanup[0].Retired = &stopped
	check(true) // Collection can remain deferred after exact retirement.
	successorDebt := s.Cleanup[0]
	successorDebt.MemberID, successorDebt.Retired = 2, nil
	s.Cleanup = append(s.Cleanup, successorDebt)
	check(true) // A different member on the same node does not own this process.
	cleanup := grant.Clone()
	cleanup.Key.GroupID, cleanup.Key.Revision = "shard0", 11
	cleanup.Action.GroupID, cleanup.Action.Kind = "shard0", topology.StepCleanupReplicaState
	cleanup.Resources[0].GroupID = "shard0"
	insert(cleanup)
	check(false) // independently retained host work blocks removal
	s.indexes.grants[cleanup.Key].Closed = true
	check(false) // supersession alone does not fence the resource
	fence := cleanup.Clone()
	fence.Key.Revision++
	fence.Purpose, fence.Action = topology.AuthorityFence, nil
	insert(fence)
	check(false)
	s.indexes.grants[fence.Key].Fences = []topology.FenceEvidence{{Key: fence.Key, Resource: fence.Resources[0], LocalSequence: 9}}
	check(true)
	// A distinct successor member's work must not become this owner's obligation.
	newMember := cleanup.Clone()
	newMember.Key.Revision += 10
	newMember.Targets[0].MemberID++
	insert(newMember)
	check(true)
}
