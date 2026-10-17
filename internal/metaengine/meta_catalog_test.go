//go:build !functional

package metaengine

import (
	"maps"
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func TestScopedMetaOwnerCatalogUsesOnlyCurrentEvidenceAndConsumedRemovals(t *testing.T) {
	t.Parallel()
	remove := topology.AuthorizedStep{OperationID: "exec-v4-a1", Version: 4, Attempt: 1,
		StepSeq: 2, Kind: topology.StepRemoveReplica, GroupID: topology.MetaGroupID,
		NodeName: "removed-by-grant", ReplicaID: 6}
	groupResource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: topology.MetaGroupID}
	cleanupRemove := topology.AuthorizedStep{Kind: topology.StepRemoveReplica,
		GroupID: topology.MetaGroupID, NodeName: "removed-by-cleanup", ReplicaID: 7}
	grantAction := topology.AuthorizedStep{Kind: topology.StepAddLearner,
		GroupID: topology.MetaGroupID, NodeName: "grant-learner", ReplicaID: 5}
	appliedAction := topology.AuthorizedStep{Kind: topology.StepAddLearner,
		GroupID: topology.MetaGroupID, NodeName: "already-applied", ReplicaID: 8}
	state := &scopedState{
		Target: topology.Document{ClusterID: "cluster", Version: 4, Groups: []topology.Group{{
			ID: topology.MetaGroupID, Replicas: []topology.Replica{{NodeName: "approved", ReplicaID: 2}}}}},
		History: []topology.Document{{ClusterID: "cluster", Version: 3, Groups: []topology.Group{{
			ID: topology.MetaGroupID, Replicas: []topology.Replica{{NodeName: "stale-history", ReplicaID: 99}}}}}},
		Groups: []groupConvergence{{GroupID: topology.MetaGroupID, SourceVersion: 2,
			Declaration: &topology.Group{ID: topology.MetaGroupID,
				Replicas: []topology.Replica{{NodeName: "proven", ReplicaID: 1}}}, Owner: "exec-v4-a1"}},
		Operations: []topology.Operation{{Status: topology.OperationStatus{ID: "exec-v4-a1"},
			Plan: topology.Plan{Steps: []topology.Step{
				{Seq: 1, GroupID: topology.MetaGroupID, Kind: topology.StepPrepareReplica, NodeName: "orphan-learner", ReplicaID: 3},
				{Seq: 2, GroupID: topology.MetaGroupID, Kind: topology.StepRemoveReplica, NodeName: "removed-by-grant", ReplicaID: 6},
				{Seq: 3, GroupID: topology.MetaGroupID, Kind: topology.StepRemoveReplica, NodeName: "pending-remove", ReplicaID: 4},
			}}}},
		AuthorityRecords: []scopedAuthorityRecord{
			{authorityRecord: authorityRecord{Grant: topology.AuthorityGrant{Key: topology.AuthorityKey{
				ClusterID: "cluster", GroupID: topology.MetaGroupID, Revision: 1}, Purpose: topology.AuthorityEffect, Action: &grantAction,
				Targets: []topology.AuthorityTarget{{NodeName: "grant-learner", MemberID: 5, ReplicaID: 5}}}}},
			{authorityRecord: authorityRecord{Grant: topology.AuthorityGrant{Key: topology.AuthorityKey{
				ClusterID: "cluster", GroupID: topology.MetaGroupID, Revision: 2}, Purpose: topology.AuthorityEffect, Action: &remove,
				Targets: []topology.AuthorityTarget{{NodeName: "removed-by-grant", MemberID: 6, ReplicaID: 6}}},
				Effects: []topology.EffectReceipt{{Resource: groupResource}}}},
			{authorityRecord: authorityRecord{Grant: topology.AuthorityGrant{Key: topology.AuthorityKey{
				ClusterID: "cluster", GroupID: topology.MetaGroupID, Revision: 3}, Purpose: topology.AuthorityEffect, Action: &appliedAction,
				Targets: []topology.AuthorityTarget{{NodeName: "already-applied", MemberID: 8, ReplicaID: 8}}},
				Effects: []topology.EffectReceipt{{Resource: groupResource}}}},
		},
		Cleanup: []cleanupRecord{{Removed: &topology.EffectReceipt{Action: &cleanupRemove}}},
	}
	got, err := scopedMetaMemberOwners(state)
	if err != nil {
		t.Fatal(err)
	}
	want := map[uint64]string{1: "proven", 2: "approved", 3: "orphan-learner", 4: "pending-remove", 5: "grant-learner"}
	if !maps.Equal(got, want) {
		t.Fatal("catalog resurrected history or retained a consumed removal", got)
	}
	got[2] = "mutated"
	if again, err := scopedMetaMemberOwners(state); err != nil || !maps.Equal(again, want) {
		t.Fatal("catalog result aliases scoped state", again)
	}
	state.AuthorityRecords[0].Grant.Targets[0].MemberID = 1
	if _, err := scopedMetaMemberOwners(state); err == nil {
		t.Fatal("catalog accepted one member ID owned by two names")
	}
}
