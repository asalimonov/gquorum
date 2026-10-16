//go:build !functional

package metaengine

import (
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func legacyProgressFixture() *Machine {
	active := topology.Document{ClusterID: "legacy", Version: 1,
		Nodes: []topology.Node{{Name: "n1"}, {Name: "n2"}},
		Groups: []topology.Group{{ID: "a", Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1}}},
			{ID: "b", Replicas: []topology.Replica{{NodeName: "n2", ReplicaID: 2}}}}}
	target, latest := active.Clone(), active.Clone()
	target.Version, latest.Version = 2, 3
	older := &operatorRecord{ID: "exec-v1-a1", Version: 1, Attempt: 1, State: topology.OperatorSucceeded,
		CurrentStep: 2, GroupID: "b", Steps: []topology.Step{{Seq: 1, GroupID: "a", NodeName: "n1"},
			{Seq: 2, GroupID: "b", NodeName: "n2"}}}
	newer := &operatorRecord{ID: "exec-v2-a1", Version: 2, Attempt: 1, State: topology.OperatorRunning,
		CurrentStep: 1, GroupID: "a", Steps: []topology.Step{{Seq: 1, GroupID: "a", NodeName: "n1"}}}
	return &Machine{topologySM: &topologySM{latest: latest, target: target, active: active,
		appliedIndex: 42, operators: map[topology.OperatorID]*operatorRecord{older.ID: older, newer.ID: newer}}}
}

func TestLegacyProgressFiltersEveryPlanStepAndSharesCursor(t *testing.T) {
	t.Parallel()
	m := legacyProgressFixture()
	query := topology.ProgressQuery{Limit: 1}
	var seen int
	for {
		page, err := m.Progress(query)
		if err != nil || page.AppliedIndex != 42 || page.LatestVersion != 3 || page.TargetVersion != 2 ||
			page.ReadAt.IsZero() || page.FullyConverged.Version != 1 {
			t.Fatal("invalid legacy page", page, err)
		}
		seen += progressCount(page)
		if page.Next == "" {
			break
		}
		query.After = page.Next
	}
	if seen != 6 {
		t.Fatal("shared cursor omitted or duplicated rows", seen)
	}

	filtered := topology.ProgressQuery{GroupID: "a", NodeName: "n1", OperationID: "exec-v1-a1", Limit: 1}
	first, err := m.Progress(filtered)
	if err != nil || first.Next == "" {
		t.Fatal("filtered page did not issue a cursor", first, err)
	}
	filtered.After = first.Next
	groups, nodes, operations := len(first.Groups), len(first.Nodes), 0
	for _, op := range first.Operations {
		operations += len(op.Blocks)
	}
	var scopedGroup string
	for {
		page, err := m.Progress(filtered)
		if err != nil {
			t.Fatal("filtered page", page, err)
		}
		groups += len(page.Groups)
		nodes += len(page.Nodes)
		for _, op := range page.Operations {
			operations += len(op.Blocks)
			scopedGroup = op.Blocks[0].GroupID
		}
		if page.Next == "" {
			break
		}
		filtered.After = page.Next
	}
	if groups != 1 || nodes != 1 || operations != 1 || scopedGroup != "b" {
		t.Fatal("filter did not include operation by its earlier plan step", groups, nodes, operations, scopedGroup)
	}
	if _, err := m.Progress(topology.ProgressQuery{GroupID: "b", After: first.Next}); err == nil {
		t.Fatal("cursor was accepted after changing filters")
	}
}
