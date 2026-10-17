//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedStatusSharesCompleteProgressRowsAndBoundsHeaders(t *testing.T) {
	t.Parallel()
	s := progressFilterFixture(t)
	for version := uint64(4); version <= 104; version++ {
		op := s.Operations[0].Clone()
		op.Status.ID, op.Status.Version = topology.MakeOperationID(version, 1), version
		s.Operations = append(s.Operations, op)
	}
	var err error
	s.indexes, err = s.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	status, err := queryScopedStatus(&s)
	if err != nil || len(status.Operations) != 100 || !status.OperationsTruncated ||
		status.Operations[0].Version != 104 || status.LatestVersion != s.Latest.Version ||
		status.TargetVersion != s.Target.Version {
		t.Fatal(status, err)
	}
	page, err := queryScopedProgress(&s, topology.ProgressQuery{Limit: 1000})
	if err != nil || !reflect.DeepEqual(status.Groups, page.Groups) {
		t.Fatal("status omitted declaration fields or diverged from progress rows", status.Groups, page.Groups, err)
	}
	for _, op := range status.Operations {
		if op.TotalBlocks != 3 || !op.BlocksTruncated || len(op.Blocks) != 0 || op.State != topology.OperationRunning {
			t.Fatal("header exceeded budget or recomputed aggregate", op)
		}
	}
	status.Groups[0].Converged.Replicas[0].NodeName = "caller"
	if s.Groups[0].Declaration.Replicas[0].NodeName == "caller" || len(s.Operations[0].Status.Blocks) != 3 {
		t.Fatal("bounded status changed full retained lookup data")
	}
	s.Operations = s.Operations[:1]
	s.Operations[0].Status.Blocks = nil
	s.indexes, _ = s.buildIndexes()
	status, err = queryScopedStatus(&s)
	if err != nil || status.OperationsTruncated || status.Operations[0].BlocksTruncated || status.Operations[0].TotalBlocks != 0 {
		t.Fatal("empty operation header falsely truncated", status, err)
	}
}

func TestScopedStatusCleanupCountsPhysicalUncertaintyWithoutChangingMilestones(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	prepareScopedFixture(t, &s)
	collected := s.Cleanup[0].clone()
	unconfirmed := collected.clone()
	unconfirmed.Collection, unconfirmed.CreatedAt = nil, time.Unix(100, 0)
	pending := unconfirmed.clone()
	pending.Retired, pending.CreatedAt = nil, time.Unix(50, 0)
	memory := unconfirmed.clone()
	memory.Storage = topology.StorageMemory
	s.Cleanup = []cleanupRecord{collected, unconfirmed, pending, memory}
	summary := s.cleanupSummary()
	if summary.Pending != 2 || summary.Unconfirmed != 1 || summary.OldestPendingAt == nil ||
		!summary.OldestPendingAt.Equal(pending.CreatedAt) {
		t.Fatal(summary)
	}
	*summary.OldestPendingAt = time.Time{}
	if s.Cleanup[2].CreatedAt.IsZero() {
		t.Fatal("summary aliased retained debt")
	}
	s.Cleanup = []cleanupRecord{collected, memory}
	if summary = s.cleanupSummary(); summary.Pending != 0 || summary.Unconfirmed != 0 || summary.OldestPendingAt != nil {
		t.Fatal("completed debt remained pending", summary)
	}
}

func TestScopedTopologyReadOwnsPairAndHistoricalMarker(t *testing.T) {
	t.Parallel()
	s := progressFilterFixture(t)
	s.AppliedIndex = 72
	s.FullyConverged = topology.FullyConverged{Version: 1, At: time.Unix(10, 0)}
	view, err := queryScopedTopology(&s)
	if err != nil || view.AppliedIndex != 72 || view.FullyConverged != s.FullyConverged ||
		!reflect.DeepEqual(view.Latest, s.Latest) || !reflect.DeepEqual(view.Target, s.Target) {
		t.Fatal("topology pair waited for convergence or included a converged document", view, err)
	}
	// Query serializes under the read lock, just as the current topology path.
	data, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	var owned topology.ReadView
	if err := json.Unmarshal(data, &owned); err != nil {
		t.Fatal(err)
	}
	owned.Latest.Groups[0].Replicas[0].NodeName = "caller"
	owned.Target.Nodes[0].Name = "caller"
	if s.Latest.Groups[0].Replicas[0].NodeName == "caller" || s.Target.Nodes[0].Name == "caller" {
		t.Fatal("borrowed pair")
	}
	if _, err := queryScopedTopology(nil); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := mustMachine(t, testRules(), nil).ProgressStatus(); err != nil {
		t.Fatal(err)
	}
}
func TestScopedStatusDrainUsesApprovedAndProvenAssignments(t *testing.T) {
	t.Parallel()
	group := topology.Group{ID: "work", Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1}}}
	for _, tc := range []struct {
		name                      string
		proven, approved, desired bool
		want                      topology.NodeState
	}{
		{"proven", true, false, false, topology.NodeDraining},
		{"approved", false, true, false, topology.NodeDraining},
		{"staged_only", false, false, true, topology.NodeDrained},
		{"removed", false, false, false, topology.NodeDrained},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := scopedState{Groups: []groupConvergence{{GroupID: group.ID}}}
			if tc.proven {
				s.Groups[0].Declaration = &group
			}
			if tc.approved {
				s.Target.Groups = []topology.Group{group}
			}
			if tc.desired {
				s.Latest.Groups = []topology.Group{group}
			}
			row := topology.NodeProgress{NodeName: "n1", Desired: &topology.Node{State: topology.NodeActive},
				Converged: &topology.Node{State: topology.NodeDraining}}
			if got := s.progressNodeState(row); got != tc.want {
				t.Fatal(got, tc.want)
			}
		})
	}
}

func TestScopedStatusKeepsEffectiveNodesSeparateFromStagedIntent(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	m := restoreMachineFixture(t, s)
	doc := m.Latest()
	doc.Nodes = append(doc.Nodes, topology.Node{Name: "n2", Type: "worker", State: topology.NodeActive})
	doc.MetaGroup().Replicas = append(doc.MetaGroup().Replicas, topology.Replica{NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaLearner})
	doc.MetaGroup().NextReplicaID = 3
	if r := applyMachine(t, m, ProposeDocument{Doc: doc, ExpectedVersion: doc.Version}); !r.Accepted {
		t.Fatal(r)
	}
	status, err := m.ProgressStatus()
	if err != nil || len(status.Nodes) != 1 || status.Nodes[0].Node.Name != "n1" {
		t.Fatal("staged node became effective", status.Nodes, err)
	}
	page, err := m.Progress(topology.ProgressQuery{NodeName: "n2"})
	if err != nil || len(page.Nodes) != 1 || page.Nodes[0].Desired == nil || page.Nodes[0].Approved != nil {
		t.Fatal("staged intent disappeared from progress", page, err)
	}
	// A declaration removed from Latest remains visible through its approved or
	// proven identity. This read must not dereference an absent Desired row.
	m.scoped.Latest.Nodes = m.scoped.Latest.Nodes[1:]
	status, err = m.ProgressStatus()
	if err != nil || len(status.Nodes) != 1 || status.Nodes[0].Node.Name != "n1" || status.Nodes[0].EffectiveState != topology.NodeActive {
		t.Fatal(status.Nodes, err)
	}
}
