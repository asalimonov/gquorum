//go:build !functional

package metaengine

import (
	"testing"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestRecoveryInventoryUsesApprovedGroupsAndKeepsStagedAllocationFloors(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = 3, 2
	staged := *s.Latest.Group("shard0")
	staged.ID, staged.NextReplicaID = "staged-only", 12
	staged.Replicas = []topology.Replica{{NodeName: "n1", ReplicaID: 11, Role: topology.ReplicaVoter}}
	s.Latest.Groups = append(s.Latest.Groups, staged)
	s.Allocations = append(s.Allocations, allocationRecord{GroupID: staged.ID, NextReplicaID: 12}, allocationRecord{GroupID: "retired", NextReplicaID: 8})
	s.RetiredGroups = append(s.RetiredGroups, retiredGroup{ID: "retired", Version: 1})
	var err error
	s.indexes, err = prepareScopedGraph(&s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	m := &Machine{rules: s.Rules, scoped: &s}
	out, err := m.RecoveryInventory(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if out.Approved.Version != 2 || out.Approved.Group(staged.ID) != nil || out.Archived.LatestVersion != 3 || len(out.Archived.OperationsNotResumed) != 2 {
		t.Fatal(out)
	}
	found := false
	for _, floor := range out.Floors {
		if floor.GroupID == staged.ID && floor.NextID == 12 {
			found = true
		}
	}
	if !found {
		t.Fatal("staged allocation high water lost")
	}
	if len(out.Retired) != 1 || out.Retired[0] != "retired" || out.Archived.RetiredGroupIDs[0] != "retired" {
		t.Fatal("retired group reservation lost", out)
	}
	for _, candidate := range out.Replicas {
		if candidate.Identity.GroupID == staged.ID {
			t.Fatal("staged intent became a donor")
		}
	}
	if s.FullyConverged.Version != 1 {
		t.Fatal("fixture must retain older historical marker")
	}
	out.Approved.Groups[0].Replicas[0].ReplicaID = 100
	out.Floors[0].NextID = 100
	if s.Target.Groups[0].Replicas[0].ReplicaID == 100 || s.Allocations[0].NextReplicaID == 100 {
		t.Fatal("projection aliases source")
	}
}

func TestRecoveryInventoryClassifiesExactPendingAndRetiredCleanup(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	var err error
	s.indexes, err = prepareScopedGraph(&s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	id := storage.Identity{ClusterID: s.Target.ClusterID, NodeName: "n1", GroupID: "shard0", ReplicaID: 7}
	action := topology.AuthorizedStep{Version: 2, Kind: topology.StepStopReplica}
	s.Cleanup = []cleanupRecord{{Identity: id, MemberID: 1, StopAction: &action}}
	out := recoveryInventory(&s)
	found := false
	for _, row := range out.Replicas {
		if row.Identity == id && row.Classification == "pending_predecessor" && row.MemberID == 1 && row.SourceVersion == 2 {
			found = true
		}
	}
	if !found || len(out.Archived.UnresolvedCleanup) != 1 {
		t.Fatal(out)
	}
	s.Cleanup[0].Retired = &topology.EffectReceipt{}
	out = recoveryInventory(&s)
	found = false
	for _, row := range out.Replicas {
		if row.Identity == id && row.Classification == "retired" {
			found = true
		}
	}
	if !found {
		t.Fatal("proven retired source remained a donor")
	}
	s.Cleanup[0].Identity.GroupID = "deleted"
	out = recoveryInventory(&s)
	found = false
	for _, row := range out.Replicas {
		if row.Identity.GroupID == "deleted" && row.Classification == "approved_deleted" {
			found = true
		}
	}
	if !found {
		t.Fatal("approved deletion lost its classification")
	}
}
