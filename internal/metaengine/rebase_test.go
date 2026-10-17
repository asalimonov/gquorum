//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func rebaseFixture(t *testing.T) (scopedState, topology.Document) {
	t.Helper()
	base := independenceFixture()
	base.Version, base.CreatedAt = 1, time.Date(2026, 10, 16, 1, 0, 0, 0, time.UTC)
	staged := base.Clone()
	staged.Version, staged.ParentVersion, staged.CreatedAt = 2, 1, base.CreatedAt.Add(time.Second)
	staged.ChangedBy, staged.Origin, staged.Comment = "caller", topology.OriginOperator, "keep all edits"
	staged.Group("shard0").Labels = map[string]string{"owner": "app"}
	staged.Group("shard0").NextReplicaID = 8
	staged.Node("n3").Region = "west"
	staged.Audit = &topology.ChangeAudit{Operation: "edit", Reason: "operator purpose", StagedBy: "caller", StagedAt: staged.CreatedAt}
	s := scopedState{SchemaVersion: scopedStateSchemaVersion, Rules: testRules(), Target: base, Latest: staged,
		History: []topology.Document{base}, AppliedIndex: 10, AppliedTerm: 1,
		FullyConverged: topology.FullyConverged{Version: 1, At: base.CreatedAt}}
	for _, group := range base.Groups {
		s.Groups = append(s.Groups, groupConvergence{GroupID: group.ID, Declaration: base.Group(group.ID), SourceVersion: 1, CompletedAt: base.CreatedAt})
		s.Allocations = append(s.Allocations, allocationRecord{group.ID, staged.Group(group.ID).NextReplicaID})
	}
	for _, node := range base.Nodes {
		s.Nodes = append(s.Nodes, nodeConvergence{NodeName: node.Name, Declaration: base.Node(node.Name),
			MemberID: base.MemberID(node.Name), SourceVersion: 1, CompletedAt: base.CreatedAt})
	}
	prepareScopedFixture(t, &s)
	repair, err := ReplicaRepairDocument(base, staged, ReplicaRepairOptions{GroupID: "b", NodeName: "n2", ReplicaID: 1,
		At: staged.CreatedAt.Add(time.Second), Origin: topology.OriginReconciler, Actor: "repair"})
	if err != nil {
		t.Fatal(err)
	}
	return s, repair.Doc
}

func TestRebasePreservesCompleteStagedIntentAndIndependentRepair(t *testing.T) {
	t.Parallel()
	s, repair := rebaseFixture(t)
	before, _ := json.Marshal(s)
	at := s.Latest.CreatedAt.Add(time.Second)
	pair, err := s.prepareRepairDocuments(repair, at)
	if err != nil {
		t.Fatal(err)
	}
	if pair.Repair.Version != 3 || pair.Repair.ParentVersion != 1 || pair.Latest.Version != 4 ||
		pair.Latest.ParentVersion != 3 || pair.Latest.RebasedFrom != 2 ||
		!pair.Repair.CreatedAt.Equal(at) || !pair.Latest.CreatedAt.After(at) ||
		pair.Repair.Group("shard0").Labels != nil || pair.Latest.Group("shard0").NextReplicaID != 8 ||
		pair.Latest.Group("b").Replicas[0].ReplicaID != 2 || pair.Latest.Node("n3").Region != "west" ||
		pair.Latest.ChangedBy != s.Latest.ChangedBy || pair.Latest.Comment != s.Latest.Comment ||
		!reflect.DeepEqual(pair.Latest.Audit, s.Latest.Audit) {
		t.Fatal("rebase lost intent, origin, identity or deterministic version/time", pair)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("rebase preparation modified source state")
	}
	next := s
	next.installRepairDocuments(pair)
	prepareScopedFixture(t, &next)
	if next.Target.Version != 3 || next.Latest.Version != 4 || len(next.Operations) != 0 ||
		next.indexes.documents[2].Comment != s.Latest.Comment || next.indexes.rebases[2] != 4 {
		t.Fatal("candidate installer lost rebase lineage or approved staged work")
	}
	pair.Latest.Node("n3").Labels["changed"] = "owned"
	pair.Latest.Audit.Reason = "changed"
	if next.Latest.Audit.Reason != s.Latest.Audit.Reason || next.Latest.Node("n3").Labels["changed"] != "" {
		t.Fatal("candidate retains mutable document aliases")
	}
}

func TestRebaseHandlesAddDeleteAndMultipleStagedEdits(t *testing.T) {
	t.Parallel()
	s, repair := rebaseFixture(t)
	old := s.Latest.Clone()
	s.History = append(s.History, old)
	s.Latest.Version, s.Latest.ParentVersion = 3, 2
	s.Latest.Groups = slices.DeleteFunc(s.Latest.Groups, func(g topology.Group) bool { return g.ID == "shard0" })
	s.Latest.Groups = append(s.Latest.Groups, topology.Group{ID: "new", NextReplicaID: 2,
		Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}}})
	s.Allocations = append(s.Allocations, allocationRecord{"new", 2})
	prepareScopedFixture(t, &s)
	pair, err := s.prepareRepairDocuments(repair, s.Latest.CreatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if pair.Latest.Group("shard0") != nil || pair.Latest.Group("new") == nil || pair.Repair.Group("new") != nil ||
		pair.Repair.ParentVersion != 1 || pair.Latest.RebasedFrom != 3 {
		t.Fatal("multi-edit delta was narrowed to the latest parent")
	}
	next := s
	next.installRepairDocuments(pair)
	prepareScopedFixture(t, &next)
}

func TestRebaseRejectsOverlapIdentityConflictsAndOverflowAtomically(t *testing.T) {
	t.Parallel()
	for name, corrupt := range map[string]func(*scopedState, *topology.Document){
		"same group": func(s *scopedState, _ *topology.Document) {
			s.Latest.Group("b").Labels = map[string]string{"change": "intent"}
		},
		"eligibility": func(s *scopedState, _ *topology.Document) { s.Latest.Node("n2").State = topology.NodeDrained },
		"placement": func(s *scopedState, _ *topology.Document) {
			s.Latest.Placement.NodeSelector = map[string]string{"tier": "hot"}
		},
		"foreign cluster": func(_ *scopedState, r *topology.Document) { r.ClusterID = "foreign" },
		"historical ID": func(s *scopedState, r *topology.Document) {
			s.Latest.Group("b").NextReplicaID, s.Allocations[2].NextReplicaID = 9, 9
			r.Group("b").Replicas[0].ReplicaID, r.Group("b").NextReplicaID = 8, 9
		},
		"identity host": func(_ *scopedState, r *topology.Document) {
			r.Group("b").Replicas[0].ReplicaID = 1
			r.Group("b").Replicas[0].NodeName = "n1"
		},
		"one version left": func(s *scopedState, _ *topology.Document) { s.Latest.Version = math.MaxUint64 - 1 },
		"no versions left": func(s *scopedState, _ *topology.Document) { s.Latest.Version = math.MaxUint64 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s, repair := rebaseFixture(t)
			corrupt(&s, &repair)
			before, _ := json.Marshal(s)
			if _, err := s.prepareRepairDocuments(repair, s.Latest.CreatedAt.Add(time.Second)); err == nil {
				t.Fatal("unsafe rebase accepted")
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("failed rebase modified retained declarations")
			}
		})
	}
}

func TestRebaseWithoutStagedIntentUsesOnlyOneVersion(t *testing.T) {
	t.Parallel()
	s, repair := rebaseFixture(t)
	s.Latest = s.Target.Clone()
	for i := range s.Allocations {
		s.Allocations[i].NextReplicaID = s.Target.Group(s.Allocations[i].GroupID).NextReplicaID
	}
	s.Latest.Version, s.Target.Version = math.MaxUint64-1, math.MaxUint64-1
	pair, err := s.prepareRepairDocuments(repair, s.Latest.CreatedAt.Add(time.Second))
	if err != nil || pair.Repair.Version != math.MaxUint64 || pair.Latest.RebasedFrom != 0 ||
		!reflect.DeepEqual(pair.Repair, pair.Latest) {
		t.Fatal("no-staging repair allocated a second document", pair, err)
	}
	_, err = rebaseStaged(s.Target, s.Latest, topology.Document{ClusterID: "different"})
	if !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal("foreign identity was not a rebase conflict", err)
	}
}

func TestRebaseDeterministicOrderAndAuditOnlyIntent(t *testing.T) {
	t.Parallel()
	s, repair := rebaseFixture(t)
	at := s.Latest.CreatedAt.Add(time.Second)
	first, err := s.prepareRepairDocuments(repair, at)
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range []*topology.Document{&s.Target, &s.Latest, &repair} {
		slices.Reverse(doc.Nodes)
		slices.Reverse(doc.Groups)
		for i := range doc.Groups {
			slices.Reverse(doc.Groups[i].Replicas)
		}
	}
	second, err := s.prepareRepairDocuments(repair, at)
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if err != nil || !bytes.Equal(a, b) {
		t.Fatal("declaration order changed generated versions/content", err)
	}
	s, repair = rebaseFixture(t)
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion, s.Latest.Comment = 2, 1, "new approval purpose"
	for i := range s.Allocations {
		s.Allocations[i].NextReplicaID = s.Target.Group(s.Allocations[i].GroupID).NextReplicaID
	}
	pair, err := s.prepareRepairDocuments(repair, at)
	if err != nil || pair.Latest.RebasedFrom != 2 || pair.Latest.Comment != s.Latest.Comment {
		t.Fatal("repair discarded caller intent with unchanged topology", err)
	}
}

func TestOrdinaryEditsAfterPreparedRebaseClearOnlyNewProvenance(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"edit", "new node"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			s, repair := rebaseFixture(t)
			s.Rules.Limits.MaxNodes = 4
			pair, err := s.prepareRepairDocuments(repair, s.Latest.CreatedAt.Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			s.installRepairDocuments(pair)
			m := restoreMachineFixture(t, s)
			edit := m.scoped.Latest.Clone()
			edit.Comment, edit.ParentVersion = "next edit", m.scoped.Latest.Version
			if kind == "new node" {
				edit.Nodes = append(edit.Nodes, topology.Node{Name: "n4", Type: "worker", State: topology.NodeActive})
				if err := edit.SetMemberID("n4", 4, pair.Latest.CreatedAt); err != nil {
					t.Fatal(err)
				}
			}
			proposal := ProposeDocument{Doc: edit, ExpectedVersion: m.scoped.Latest.Version}
			result := applyMachine(t, m, proposal)
			if !result.Accepted || result.Doc.Version != 5 || result.Doc.RebasedFrom != 0 || m.scoped.Target.Version != 3 {
				t.Fatal("ordinary mutation lost/approved rebased intent or copied lineage", result)
			}
			found := false
			for _, doc := range m.scoped.History {
				if doc.Version == pair.Latest.Version {
					found = reflect.DeepEqual(doc, pair.Latest)
				}
			}
			if !found || !reflect.DeepEqual(s.Latest, pair.Latest) {
				t.Fatal("ordinary mutation rewrote original rebased document")
			}
		})
	}
}

func TestRepairMissingPlanCannotInstallPartialApproval(t *testing.T) {
	t.Parallel()
	s, repair := rebaseFixture(t)
	before, _ := json.Marshal(s)
	p := CommitRepairOperationProposal{Doc: repair, ExpectedVersion: s.Latest.Version,
		ExpectedTarget: s.Target.Version, At: s.Latest.CreatedAt.Add(time.Second)}
	if _, err := s.applyCommitRepairOperation(s.AppliedIndex+1, p); err == nil {
		t.Fatal("repair without an approved plan became effect authority", err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("refused approval installed only its documents")
	}
}
