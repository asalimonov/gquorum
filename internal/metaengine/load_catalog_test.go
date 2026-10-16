//go:build !functional

package metaengine

import (
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestDeletionCatalogIncludesApprovedPreparationAndExcludesStagedIntent(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	group := func(ids ...uint64) topology.Group {
		g := topology.Group{ID: "work"}
		for _, id := range ids {
			g.Replicas = append(g.Replicas, topology.Replica{NodeName: "n1", ReplicaID: id, Role: topology.ReplicaVoter})
		}
		return g
	}
	sm.active = topology.Document{ClusterID: "cluster", Version: 1, Groups: []topology.Group{group(1)}}
	sm.target = topology.Document{ClusterID: "cluster", Version: 4}
	sm.history = []topology.Document{sm.active.Clone(), {ClusterID: "cluster", Version: 2, Groups: []topology.Group{group(1, 2)}}, {ClusterID: "cluster", Version: 3, Groups: []topology.Group{group(99)}}, sm.target.Clone()}
	old := newOperatorRecord(2, 1, []topology.Step{{Seq: 1, GroupID: "work", Kind: topology.StepPrepareReplica, NodeName: "n1", ReplicaID: 2}}, "actor", topology.OriginOperator, time.Now())
	old.State = topology.OperatorSuperseded
	current := newOperatorRecord(4, 1, []topology.Step{{Seq: 1, GroupID: "work", Kind: topology.StepDeleteGroupState}}, "actor", topology.OriginOperator, time.Now())
	current.State, current.CurrentStep = topology.OperatorRunning, 1
	sm.operators[old.ID], sm.operators[current.ID] = &old, &current
	a := topology.AuthorizedStep{OperatorID: current.ID, Version: 4, Attempt: 1, StepSeq: 1, GroupID: "work", Kind: topology.StepDeleteGroupState}
	catalogIDs, err := sm.deletionCatalogLocked(a.GroupID)
	ids := orderedIdentities(catalogIDs)
	if err != nil {
		t.Fatal(err)
	}
	want := []storage.Identity{{ClusterID: "cluster", NodeName: "n1", GroupID: "work", ReplicaID: 1}, {ClusterID: "cluster", NodeName: "n1", GroupID: "work", ReplicaID: 2}}
	if !slices.Equal(ids, want) {
		t.Fatal(ids)
	}
	sm.latest = sm.history[2].Clone() // staged replica 99 was never approved
	sm.latest.Version = 5
	catalog, err := sm.LegacyRecoveryCatalog(t.Context())
	if err != nil || len(catalog) != 1 || len(catalog[0].Replicas) != 2 || catalog[0].Replicas[0].ReplicaID != 1 || catalog[0].Replicas[1].ReplicaID != 2 {
		t.Fatal("readmission included staged intent or lost approved preparation", catalog, err)
	}
	catalog[0].Replicas[0].ReplicaID = 88
	catalog, err = sm.LegacyRecoveryCatalog(t.Context())
	if err != nil || catalog[0].Replicas[0].ReplicaID != 1 {
		t.Fatal("recovery catalog aliases FSM state", catalog, err)
	}
	ids[0].ReplicaID = 99
	catalogIDs, err = sm.deletionCatalogLocked(a.GroupID)
	again := orderedIdentities(catalogIDs)
	if err != nil || !slices.Equal(again, want) {
		t.Fatal("catalog returned mutable FSM data", again, err)
	}
	a.Version++
	if result := apply(t, sm, 10, BeginEffectProposal{Action: a}); result.Accepted || result.Code != proposalAuthorization {
		t.Fatal("unknown future approval issued a grant", result)
	}
}

func TestDeletionCatalogUsesDurablyCompletedCleanup(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	r1 := topology.Replica{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}
	r2 := topology.Replica{NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaLearner}
	oldDoc := topology.Document{ClusterID: "cluster", Version: 1, Groups: []topology.Group{{ID: "work", Replicas: []topology.Replica{r1, r2}}}}
	sm.active = topology.Document{ClusterID: "cluster", Version: 3, Groups: []topology.Group{{ID: "work", Replicas: []topology.Replica{r1}}}}
	sm.target = topology.Document{ClusterID: "cluster", Version: 4}
	sm.history = []topology.Document{oldDoc, sm.active, sm.target}
	created := newOperatorRecord(1, 1, []topology.Step{{Seq: 1, Kind: topology.StepCreateFoundingGroup, GroupID: "work"}}, "actor", topology.OriginOperator, time.Now())
	created.State, created.CurrentStep = topology.OperatorSucceeded, 1
	cleanup := newOperatorRecord(3, 1, []topology.Step{
		{Seq: 1, Kind: topology.StepRemoveReplica, GroupID: "work", NodeName: "n2", ReplicaID: 2},
		{Seq: 2, Kind: topology.StepStopReplica, GroupID: "work", NodeName: "n2", ReplicaID: 2},
		{Seq: 3, Kind: topology.StepCleanupReplicaState, GroupID: "work", NodeName: "n2", ReplicaID: 2},
	}, "actor", topology.OriginOperator, time.Now())
	cleanup.State, cleanup.CurrentStep = topology.OperatorSucceeded, 3
	steps := []topology.Step{{Seq: 1, Kind: topology.StepStopReplica, GroupID: "work", NodeName: "n1", ReplicaID: 1}, {Seq: 2, Kind: topology.StepDeleteGroupState, GroupID: "work"}}
	current := newOperatorRecord(4, 1, steps, "actor", topology.OriginOperator, time.Now())
	current.State, current.CurrentStep = topology.OperatorRunning, 2
	sm.operators[created.ID], sm.operators[cleanup.ID], sm.operators[current.ID] = &created, &cleanup, &current
	a := topology.AuthorizedStep{OperatorID: current.ID, Version: 4, Attempt: 1, StepSeq: 2, GroupID: "work", Kind: topology.StepDeleteGroupState}
	catalogIDs, err := sm.deletionCatalogLocked(a.GroupID)
	ids := orderedIdentities(catalogIDs)
	if err != nil || len(ids) != 1 || ids[0].ReplicaID != 1 {
		t.Fatal("completed cleanup required contacting former replica again", ids, err)
	}
	meta := topology.Group{ID: topology.MetaGroupID, Replicas: []topology.Replica{r1, r2}}
	sm.active.Groups = append(sm.active.Groups, meta)
	oldAction := topology.AuthorizedStep{OperatorID: created.ID, Version: 1, Attempt: 1, StepSeq: 1, Kind: topology.StepCreateFoundingGroup, GroupID: "work"}
	oldGrant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "cluster", GroupID: "work", Revision: 10}, Purpose: topology.AuthorityEffect, OperatorID: created.ID, Action: &oldAction,
		Resources: []topology.AuthorityResource{{Kind: topology.AuthorityGroup, GroupID: "work"}, {Kind: topology.AuthorityHost, GroupID: "work", NodeName: "n1"}, {Kind: topology.AuthorityHost, GroupID: "work", NodeName: "n2"}},
		Targets:   []topology.AuthorityTarget{{NodeName: "n1", MemberID: 1, ReplicaID: 1}, {NodeName: "n2", MemberID: 2, ReplicaID: 2}}}
	sm.insertGrantLocked(oldGrant)
	resources, err := sm.grantResourcesLocked(a)
	if err != nil || len(resources.Resources) != 1 || len(resources.Targets) != 1 || resources.Targets[0].NodeName != "n1" {
		t.Fatal("historical grant resurrected retired host", resources.Resources, resources.Targets, err)
	}
	deletion := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "cluster", GroupID: "work", Revision: 20}, Purpose: topology.AuthorityEffect, OperatorID: current.ID, Action: &a, Resources: resources.Resources, Targets: resources.Targets}
	sm.insertGrantLocked(deletion)
	// The first attempt completed deletion but another group's step then failed.
	// Retrying the version may reuse that durable completion proof.
	current.State = topology.OperatorFailed
	current.CurrentStep = 3
	current.Steps = append(current.Steps, topology.Step{Seq: 3, Kind: topology.StepCreateFoundingGroup, GroupID: "zz-other"})
	retry := newOperatorRecord(4, 2, steps, "actor", topology.OriginOperator, time.Now())
	retry.State, retry.CurrentStep = topology.OperatorRunning, 2
	sm.operators[retry.ID] = &retry
	a.OperatorID, a.Attempt = retry.ID, 2
	catalogIDs, err = sm.deletionCatalogLocked(a.GroupID)
	ids = orderedIdentities(catalogIDs)
	if err != nil || len(ids) != 0 {
		t.Fatal("verified whole-group deletion was repeated", ids, err)
	}
	resources, err = sm.grantResourcesLocked(a)
	if err != nil || len(resources.Resources) != 1 || !slices.Equal(resources.Targets, deletion.Targets) {
		t.Fatal("retry did not preserve exact prior deletion targets", resources.Resources, resources.Targets, err)
	}
}
