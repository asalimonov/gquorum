//go:build !functional

package metaengine

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
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
	ids, err := sm.LoadActionTargets(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	want := []storage.Identity{{ClusterID: "cluster", NodeName: "n1", GroupID: "work", ReplicaID: 1}, {ClusterID: "cluster", NodeName: "n1", GroupID: "work", ReplicaID: 2}}
	if !slices.Equal(ids, want) {
		t.Fatal(ids)
	}
	sm.latest = sm.history[2].Clone() // staged replica 99 was never approved
	sm.latest.Version = 5
	catalog, err := sm.RecoveryCatalog(t.Context())
	if err != nil || len(catalog) != 1 || len(catalog[0].Replicas) != 2 || catalog[0].Replicas[0].ReplicaID != 1 || catalog[0].Replicas[1].ReplicaID != 2 {
		t.Fatal("readmission included staged intent or lost approved preparation", catalog, err)
	}
	catalog[0].Replicas[0].ReplicaID = 88
	catalog, err = sm.RecoveryCatalog(t.Context())
	if err != nil || catalog[0].Replicas[0].ReplicaID != 1 {
		t.Fatal("recovery catalog aliases FSM state", catalog, err)
	}
	ids[0].ReplicaID = 99
	again, err := sm.LoadActionTargets(context.Background(), a)
	if err != nil || !slices.Equal(again, want) {
		t.Fatal("catalog returned mutable FSM data", again, err)
	}
	a.Version++
	if _, err := sm.LoadActionTargets(context.Background(), a); !errors.Is(err, fault.ErrOperatorAuthorization) {
		t.Fatal("unknown future approval treated as authenticated lag", err)
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
	ids, err := sm.LoadActionTargets(context.Background(), a)
	if err != nil || len(ids) != 1 || ids[0].ReplicaID != 1 {
		t.Fatal("completed cleanup required contacting former replica again", ids, err)
	}
	// The first attempt completed deletion but another group's step then failed.
	// Retrying the version may reuse that durable completion proof.
	current.State = topology.OperatorFailed
	current.CurrentStep = 3
	current.Steps = append(current.Steps, topology.Step{Seq: 3, Kind: topology.StepCreateFoundingGroup, GroupID: "zz-other"})
	retry := newOperatorRecord(4, 2, steps, "actor", topology.OriginOperator, time.Now())
	retry.State, retry.CurrentStep = topology.OperatorRunning, 2
	sm.operators[retry.ID] = &retry
	a.OperatorID, a.Attempt = retry.ID, 2
	ids, err = sm.LoadActionTargets(context.Background(), a)
	if err != nil || len(ids) != 0 {
		t.Fatal("verified whole-group deletion was repeated", ids, err)
	}
}
