//go:build !functional

package metaengine

import (
	"encoding/json"
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func TestHostReleaseWaitsForConvergenceAndAllHostAuthority(t *testing.T) {
	t.Parallel()
	m := NewMachine(testRules(), nil)
	m.latest, m.target, m.active = baseDoc(), baseDoc(), baseDoc()
	target := topology.AuthorityTarget{NodeName: "n1", MemberID: 1, ReplicaID: 1}
	a := topology.AuthorizedStep{OperatorID: topology.MakeOperatorID(2, 1), Version: 2, Attempt: 1, StepSeq: 2, Kind: topology.StepStopReplica, GroupID: topology.MetaGroupID, NodeName: "n1", ReplicaID: 1}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: m.target.ClusterID, GroupID: topology.MetaGroupID, Revision: 10}, Purpose: topology.AuthorityEffect, OperatorID: a.OperatorID, Action: &a,
		Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: topology.MetaGroupID, NodeName: "n1"}}, Targets: []topology.AuthorityTarget{target}}
	m.insertGrantLocked(grant)
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
	m.authority.records[grant.Key].Effects = fixtureEffectReceipts(grant)
	check(false)
	m.target.Version++
	m.target.Group(topology.MetaGroupID).Replicas = m.target.Group(topology.MetaGroupID).Replicas[1:]
	check(false) // later cleanup may not have a grant yet
	m.active = m.target.Clone()
	check(true)
	cleanup := grant.Clone()
	cleanup.Key.GroupID, cleanup.Key.Revision = "shard0", 11
	cleanup.Action.GroupID, cleanup.Action.Kind = "shard0", topology.StepCleanupReplicaState
	cleanup.Resources[0].GroupID = "shard0"
	m.insertGrantLocked(cleanup)
	check(false) // independently retained host work blocks removal
	m.authority.records[cleanup.Key].Closed = true
	check(false) // supersession alone does not fence the resource
	fence := cleanup.Clone()
	fence.Key.Revision++
	fence.Purpose, fence.Action = topology.AuthorityFence, nil
	m.insertGrantLocked(fence)
	check(false)
	m.authority.records[fence.Key].Fences = []topology.FenceEvidence{{Key: fence.Key, Resource: fence.Resources[0], LocalSequence: 9}}
	check(true)
	// A distinct successor member's work must not become this owner's obligation.
	newMember := cleanup.Clone()
	newMember.Key.Revision += 10
	newMember.Targets[0].MemberID++
	m.insertGrantLocked(newMember)
	check(true)
}
