//go:build !functional

package metaengine

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestScopedMetaOwnerCatalogUsesOnlyCurrentEvidenceAndConsumedRemovals(t *testing.T) {
	t.Parallel()
	remove := topology.AuthorizedStep{OperatorID: "exec-v4-a1", Version: 4, Attempt: 1,
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

func TestIntermediateMetaOwnerSurvivesSupersessionPruningAndRetiresExactly(t *testing.T) {
	t.Parallel()
	m := convergedAuthorityFixture(t)
	m.rules.Limits.MaxNodes, m.rules.Limits.MaxReplicas = 4, 4
	index := uint64(6)
	at := time.Unix(300, 0).UTC()
	command := func(p any) ApplyResult {
		t.Helper()
		r := apply(t, m.topologySM, index, p)
		index++
		if !r.Accepted {
			t.Fatalf("%T: %+v", p, r)
		}
		return r
	}
	original := m.Active()
	joining := original.Clone()
	joining.Nodes = append(joining.Nodes, topology.Node{Name: "n4", Type: "worker", State: topology.NodeActive, Labels: map[string]string{"route": "approved-four"}})
	joining.MetaGroup().Replicas = append(joining.MetaGroup().Replicas, topology.Replica{NodeName: "n4", ReplicaID: 4, Role: topology.ReplicaLearner})
	joining.MetaGroup().NextReplicaID = 5
	command(ProposeDocument{Doc: joining, ExpectedVersion: 1})
	joining = m.Latest()
	steps := []topology.Step{
		{Seq: 1, GroupID: topology.MetaGroupID, NodeName: "n4", ReplicaID: 4, Kind: topology.StepPrepareReplica},
		{Seq: 2, GroupID: topology.MetaGroupID, NodeName: "n4", ReplicaID: 4, Kind: topology.StepAddLearner},
	}
	plan := withTestVoterProof(original, joining, topology.Plan{Version: joining.Version, Steps: steps})
	op := command(BeginOperatorProposal{Version: joining.Version, Plan: plan, At: at}).OperatorID
	applyAdmissionTransition(t, m.topologySM, index, op, plan, 1, topology.OperatorRunning, at)
	index++
	index = consumeTestStep(t, m.topologySM, op, index)
	// Model a durably prepared host that may have entered native membership
	// before its AddLearner acknowledgement reached the metadata controller.
	desired := original.Clone()
	desired.MetaGroup().NextReplicaID = 5
	command(ProposeDocument{Doc: desired, ExpectedVersion: joining.Version})
	desired = m.Latest()
	for range historyRetention + 3 {
		command(ProposeDocument{Doc: desired, ExpectedVersion: m.Latest().Version})
		desired = m.Latest()
	}
	base, err := m.DeletionBase(t.Context(), desired)
	if err != nil || documentReplica(base, topology.MetaGroupID, 4) == nil || base.Node("n4") == nil || base.Node("n4").Labels["route"] != "approved-four" {
		t.Fatal("lost interrupted metadata owner or approved routing attributes", base, err)
	}
	base.Node("n4").Labels["route"] = "mutated"
	base, err = m.DeletionBase(t.Context(), desired)
	if err != nil || base.Node("n4").Labels["route"] != "approved-four" {
		t.Fatal("planning input aliases retained metadata", base, err)
	}
	statuses := map[string]topology.GroupStatus{}
	for _, group := range original.Groups {
		statuses[group.ID] = topology.GroupStatus{GroupID: group.ID, Found: true, LeaderID: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}, HealthKnown: true, LiveVoters: map[uint64]bool{1: true, 2: true, 3: true}}
	}
	meta := statuses[topology.MetaGroupID]
	meta.ConfState.Learners = []uint64{4}
	statuses[topology.MetaGroupID] = meta
	plan, err = topology.BuildPlan(testPlanningInput(t, m, desired, statuses), desired, m.rules)
	if err != nil || len(plan.Steps) != 2 || plan.Steps[0].Kind != topology.StepRemoveReplica || plan.Steps[1].Kind != topology.StepStopReplica || plan.Steps[0].NodeName != "n4" || plan.Steps[0].ReplicaID != 4 {
		t.Fatal("cannot reconcile historical native metadata learner", plan, err)
	}
	cleanup := command(BeginOperatorProposal{Version: desired.Version, Plan: plan, At: at}).OperatorID
	if owner, established := m.MemberOwner(4); owner != "n4" || !established {
		t.Fatal("supersession hid an approved native participant", owner, established)
	}
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restored := NewMachine(m.rules, nil)
	if err := restored.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	m = restored
	if owner, _ := m.MemberOwner(4); owner != "n4" {
		t.Fatal("restore lost intermediate inbound routing", owner)
	}
	owners, _ := m.MemberOwners()
	delete(owners, 4)
	if owner, _ := m.MemberOwner(4); owner != "n4" {
		t.Fatal("routing map aliases FSM state")
	}
	for _, step := range plan.Steps {
		applyAdmissionTransition(t, m.topologySM, index, cleanup, plan, step.Seq, topology.OperatorRunning, at)
		index++
		index = consumeTestStep(t, m.topologySM, cleanup, index)
		if owner, _ := m.MemberOwner(4); owner != "" {
			t.Fatal("consumed native removal still authorizes inbound traffic", owner)
		}
		base, err = m.DeletionBase(t.Context(), desired)
		if err != nil || (documentReplica(base, topology.MetaGroupID, 4) != nil) != (step.Kind == topology.StepRemoveReplica) {
			t.Fatal("native absence discarded unfinished host retirement, or completed stop resurrected it", base, err)
		}
	}
	applyAdmissionTransition(t, m.topologySM, index, cleanup, plan, len(plan.Steps), topology.OperatorSucceeded, at)
	index++
	command(ActivateTargetProposal{OperatorID: cleanup, Version: desired.Version, Attempt: 1})
	for range historyRetention + 3 {
		command(ProposeDocument{Doc: desired, ExpectedVersion: m.Latest().Version})
		desired = m.Latest()
	}
	base, err = m.DeletionBase(t.Context(), desired)
	if err != nil || len(base.MetaGroup().Replicas) != 3 || slices.ContainsFunc(base.Nodes, func(n topology.Node) bool { return n.Name == "n4" }) {
		t.Fatal("completed intermediate metadata retirement returned to planning", base, err)
	}
	data, err = m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	if owner, _ := m.MemberOwner(4); owner != "" {
		t.Fatal("pruning/restore resurrected retired inbound identity", owner)
	}
}
