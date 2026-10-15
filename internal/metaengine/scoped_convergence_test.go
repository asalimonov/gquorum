//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedConvergenceAndIdentityRefuseContradictions(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*scopedState){
		"group identity":             func(s *scopedState) { s.Groups[1].Declaration.ID = "foreign" },
		"future source":              func(s *scopedState) { s.Groups[1].SourceVersion = 3 },
		"unproven declaration":       func(s *scopedState) { s.Groups[1].SourceVersion = 0; s.Groups[1].CompletedAt = time.Time{} },
		"missing completion time":    func(s *scopedState) { s.Groups[1].CompletedAt = time.Time{} },
		"missing owner":              func(s *scopedState) { s.Groups[1].Owner = "" },
		"dangling owner":             func(s *scopedState) { s.Groups[1].Owner = "gone" },
		"failed owner":               func(s *scopedState) { s.Groups[1].Owner = s.Operations[0].Status.ID },
		"wrong owner source":         func(s *scopedState) { s.Groups[1].SourceVersion = 2 },
		"duplicate ownership":        func(s *scopedState) { s.Groups[0].Owner = s.Groups[1].Owner },
		"false deletion":             func(s *scopedState) { s.Groups[1].Deleting = true },
		"member mismatch":            func(s *scopedState) { s.Nodes[0].MemberID = 2 },
		"unproven member":            func(s *scopedState) { s.Nodes[0].SourceVersion = 0; s.Nodes[0].CompletedAt = time.Time{} },
		"foreign node":               func(s *scopedState) { s.Nodes[0].Declaration.Name = "foreign" },
		"dangling node owner":        func(s *scopedState) { s.Nodes[0].Owner = "gone" },
		"missing approved group":     func(s *scopedState) { s.Groups = s.Groups[1:] },
		"missing approved node":      func(s *scopedState) { s.Nodes = nil },
		"lost successful checkpoint": func(s *scopedState) { s.Groups[0].SourceVersion = 1 },
		"missing allocator":          func(s *scopedState) { s.Allocations = s.Allocations[:1] },
		"decreasing floor":           func(s *scopedState) { s.Allocations[1].NextReplicaID = 1 },
		"decreasing latest":          func(s *scopedState) { s.Allocations[1].NextReplicaID = 3 },
		"unapproved retirement":      func(s *scopedState) { s.Latest.Version = 3; s.RetiredGroups = []retiredGroup{{ID: "gone", Version: 3}} },
		"retired current group":      func(s *scopedState) { s.RetiredGroups = []retiredGroup{{ID: "shard0", Version: 2}} },
		"orphan lock":                func(s *scopedState) { s.Locks = []lockEntry{{Key: "k", SessionID: "missing"}} },
		"unknown intent": func(s *scopedState) {
			s.Intents = []intentRecord{{Version: 2, ExpectedVersion: 1, Kind: "unknown", Digest: strings.Repeat("0", 64)}}
		},
		"malformed digest": func(s *scopedState) {
			s.Intents = []intentRecord{{Version: 2, ExpectedVersion: 1, Kind: "add_node", Digest: strings.Repeat("A", 64)}}
		},
		"foreign successor": func(s *scopedState) {
			s.Intents = []intentRecord{{Version: 2, ExpectedVersion: 1, Kind: "readmit_node", Digest: strings.Repeat("0", 64), NodeName: "n1", PreviousID: 1, SuccessorID: 3}}
		},
		"replacement regression": func(s *scopedState) {
			s.Intents = []intentRecord{{Version: 2, ExpectedVersion: 1, Kind: "readmit_node", Digest: strings.Repeat("0", 64), NodeName: "n1", PreviousID: 2, SuccessorID: 1}}
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := scopedGraphFixture()
			corrupt(&s)
			if _, err := prepareScopedGraph(&s, testRules()); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("corrupt convergence/provenance accepted", err)
			}
		})
	}
}

func TestScopedConvergenceRetainsTombstonesAndCompactedCheckpoints(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	s.Operations = nil
	s.Groups[1].Owner = ""
	s.History, s.FullyConverged = nil, topology.FullyConverged{}
	// Source v1 is compacted; validated Target v2 has exactly these declarations.
	if _, err := prepareScopedGraph(&s, testRules()); err != nil {
		t.Fatal(err)
	}
	s.Groups = append(s.Groups, groupConvergence{GroupID: "gone", SourceVersion: 2, CompletedAt: s.Nodes[0].CompletedAt})
	s.Nodes = append(s.Nodes, nodeConvergence{NodeName: "gone", SourceVersion: 2, CompletedAt: s.Nodes[0].CompletedAt})
	s.RetiredGroups = []retiredGroup{{ID: "gone", Version: 2}}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.indexes.groups["gone"].Declaration != nil || restored.indexes.nodes["gone"].MemberID != 0 {
		t.Fatal("tombstone invented a live declaration/member")
	}
	s.RetiredGroups = nil
	if _, err := prepareScopedGraph(&s, testRules()); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("deleted group lost permanent retirement", err)
	}
}

func TestScopedConvergenceDecodeRefusalPreservesPriorEvidence(t *testing.T) {
	t.Parallel()
	good, err := json.Marshal(scopedGraphFixture())
	if err != nil {
		t.Fatal(err)
	}
	var prior scopedState
	if err := json.Unmarshal(good, &prior); err != nil {
		t.Fatal(err)
	}
	index := prior.indexes
	bad := scopedGraphFixture()
	bad.Groups[1].Owner = ""
	data, err := json.Marshal(bad)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &prior); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("accepted ownerless pending block", err)
	}
	after, err := json.Marshal(prior)
	if err != nil || !bytes.Equal(good, after) || index != prior.indexes {
		t.Fatal("refusal changed prior evidence or index ownership", err)
	}
}
