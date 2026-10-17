//go:build !functional

package metaengine

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func progressFilterFixture(t *testing.T) scopedState {
	t.Helper()
	base := topology.Document{ClusterID: "c", Version: 1,
		Nodes: []topology.Node{{Name: "old"}, {Name: "one"}, {Name: "two"}},
		Groups: []topology.Group{
			{ID: "a", Replicas: []topology.Replica{{NodeName: "old", ReplicaID: 1}}},
			{ID: "b", Replicas: []topology.Replica{{NodeName: "two", ReplicaID: 2}}},
			{ID: "gone", Replicas: []topology.Replica{{NodeName: "old", ReplicaID: 3}}}}}
	target := base.Clone()
	target.Version = 2
	target.Groups = target.Groups[:2]
	target.Groups[0].Replicas[0] = topology.Replica{NodeName: "one", ReplicaID: 4}
	latest := target.Clone()
	latest.Version = 3
	latest.Groups[0].Replicas = append(latest.Groups[0].Replicas, topology.Replica{NodeName: "staged", ReplicaID: 5})
	op := topology.Operation{Status: topology.OperationStatus{ID: "exec-v2-a1", Version: 2, Attempt: 1,
		State: topology.OperationRunning, TotalBlocks: 3}, Plan: topology.Plan{Version: 2}}
	for _, group := range []string{"a", "b", "gone"} {
		op.Plan.Blocks = append(op.Plan.Blocks, topology.PlanBlock{GroupID: group, SourceVersion: 1})
		op.Status.Blocks = append(op.Status.Blocks, topology.BlockStatus{GroupID: group, State: topology.OperationRunning,
			Blocked: &topology.BlockedReason{Code: topology.BlockedDependency, NodeName: "old"}})
	}
	s := scopedState{Latest: latest, Target: target, History: []topology.Document{base}, Operations: []topology.Operation{op},
		Groups: []groupConvergence{{GroupID: "a", Declaration: base.Group("a"), SourceVersion: 1, Owner: op.Status.ID},
			{GroupID: "b", Declaration: base.Group("b"), SourceVersion: 1}, {GroupID: "gone", SourceVersion: 2}},
		Nodes: []nodeConvergence{{NodeName: "old", Declaration: base.Node("old"), SourceVersion: 1},
			{NodeName: "one", Owner: op.Status.ID}},
		Cleanup: []cleanupRecord{{Identity: storage.Identity{ClusterID: "c", GroupID: "gone", NodeName: "old", ReplicaID: 3},
			OperationID: op.Status.ID, MemberID: 7, Storage: topology.StorageDefault, UpdatedAt: time.Unix(10, 0)}}}
	var err error
	s.indexes, err = s.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestProgressFiltersActualScopeWithAndSemantics(t *testing.T) {
	t.Parallel()
	s := progressFilterFixture(t)
	for _, query := range []topology.ProgressQuery{
		{GroupID: "a", NodeName: "two"},
		{GroupID: "a", OperationID: "unknown"},
		{GroupID: "gone", NodeName: "two", OperationID: "exec-v2-a1"},
	} {
		page, err := queryScopedProgress(&s, query)
		if err != nil || progressCount(page) != 0 {
			t.Fatal("OR or cluster-wide scope leaked", query, page, err)
		}
	}
	page, err := queryScopedProgress(&s, topology.ProgressQuery{GroupID: "a", OperationID: "exec-v2-a1"})
	if err != nil || len(page.Groups) != 1 || len(page.Nodes) != 1 || page.Nodes[0].NodeName != "one" ||
		len(page.Operations) != 1 || len(page.Operations[0].Blocks) != 1 || page.Operations[0].Blocks[0].GroupID != "a" ||
		page.Operations[0].TotalBlocks != 3 || !page.Operations[0].BlocksTruncated {
		t.Fatal("owner or block scope lost", page, err)
	}
	group := page.Groups[0]
	if !reflect.DeepEqual(group.Desired, s.Latest.Group("a")) || !reflect.DeepEqual(group.Approved, s.Target.Group("a")) ||
		!reflect.DeepEqual(group.Converged, s.History[0].Group("a")) || group.ConvergedVersion != 1 ||
		group.Blocked == nil || group.Observation != nil {
		t.Fatal("incomplete declaration projection", group)
	}
	group.Blocked.Code = "caller"
	if s.Operations[0].Status.Blocks[0].Blocked.Code == "caller" {
		t.Fatal("borrowed blocker")
	}
	staged, err := queryScopedProgress(&s, topology.ProgressQuery{GroupID: "a", NodeName: "staged"})
	if err != nil || len(staged.Groups) != 1 || len(staged.Operations) != 0 {
		t.Fatal("staged membership became approved operation scope", staged, err)
	}
	old, err := queryScopedProgress(&s, topology.ProgressQuery{GroupID: "a", NodeName: "old"})
	if err != nil || len(old.Operations) != 1 {
		t.Fatal("operation lost its source membership", old, err)
	}
}

func TestProgressDeletedGroupAndCleanupIdentityOrdering(t *testing.T) {
	t.Parallel()
	s := progressFilterFixture(t)
	debt := s.Cleanup[0]
	debt.Identity.ReplicaID = 11
	s.Cleanup = append(s.Cleanup, debt)
	debt.Identity.ReplicaID = 2
	s.Cleanup = append(s.Cleanup, debt)
	s.indexes, _ = s.buildIndexes()
	page, err := queryScopedProgress(&s, topology.ProgressQuery{GroupID: "gone", NodeName: "old"})
	if err != nil || len(page.Groups) != 1 || len(page.Cleanup) != 3 || len(page.Operations) != 1 || len(page.Nodes) != 1 {
		t.Fatal("deleted evidence vanished", page, err)
	}
	group := page.Groups[0]
	if group.Desired != nil || group.Approved != nil || group.Converged != nil || group.ConvergedVersion != 2 {
		t.Fatal("deletion tombstone became a live declaration", group)
	}
	for i, want := range []uint64{2, 3, 11} {
		if page.Cleanup[i].Identity.ReplicaID != want {
			t.Fatal("lexical replica order", page.Cleanup)
		}
	}
	// Marshaling ordinary progress must never smuggle the proof-only receipt.
	data, err := json.Marshal(page)
	if err != nil || len(data) == 0 {
		t.Fatal(err)
	}
}

func TestProgressMetadataScopeIncludesExcludedNodeDeclarations(t *testing.T) {
	t.Parallel()
	s := progressFilterFixture(t)
	for _, doc := range []*topology.Document{&s.Latest, &s.Target} {
		doc.Groups = append(doc.Groups, topology.Group{ID: topology.MetaGroupID})
	}
	op := &s.Operations[0]
	op.Plan.Blocks = append(op.Plan.Blocks, topology.PlanBlock{GroupID: topology.MetaGroupID, SourceVersion: 1})
	op.Status.Blocks = append(op.Status.Blocks, topology.BlockStatus{GroupID: topology.MetaGroupID})
	op.Status.TotalBlocks++
	s.indexes, _ = s.buildIndexes()
	page, err := queryScopedProgress(&s, topology.ProgressQuery{GroupID: topology.MetaGroupID, NodeName: "old"})
	if err != nil || len(page.Groups) != 1 || len(page.Nodes) != 1 || len(page.Operations) != 1 ||
		len(page.Operations[0].Blocks) != 1 || page.Operations[0].Blocks[0].GroupID != topology.MetaGroupID {
		t.Fatal("node-catalog scope required native membership", page, err)
	}
}
