//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func scopedGraphFixture() scopedState {
	at := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	base := baseDoc()
	base.Version = 1
	desired := base.Clone()
	desired.Version, desired.ParentVersion = 2, 1
	plan := topology.Plan{Version: 2, Steps: []topology.Step{
		{Seq: 1, GroupID: topology.MetaGroupID, Kind: topology.StepTransferLeader, NodeName: "n1", ReplicaID: 1},
		{Seq: 2, GroupID: "shard0", Kind: topology.StepTransferLeader, NodeName: "n1", ReplicaID: 1},
	}, Blocks: []topology.PlanBlock{
		{GroupID: topology.MetaGroupID, SourceVersion: 1, Steps: []int{1}},
		{GroupID: "shard0", SourceVersion: 1, Steps: []int{2}},
	}}
	first := topology.Operation{Status: topology.OperationStatus{
		ID: topology.MakeOperationID(2, 1), Version: 2, Attempt: 1, State: topology.OperationFailed, TotalBlocks: 2,
		Blocks: []topology.BlockStatus{
			{GroupID: topology.MetaGroupID, State: topology.OperationSucceeded, CurrentStep: 1, TotalSteps: 1},
			{GroupID: "shard0", State: topology.OperationFailed, CurrentStep: 2, TotalSteps: 1, Error: "unreachable"},
		},
	}, Plan: plan, StartedAt: at, UpdatedAt: at}
	retry := first.Clone()
	retry.Previous = first.Status.ID
	retry.Status.ID, retry.Status.Attempt, retry.Status.State = topology.MakeOperationID(2, 2), 2, topology.OperationPending
	retry.Status.Blocks[1].State, retry.Status.Blocks[1].Error = topology.OperationPending, ""
	return scopedState{SchemaVersion: scopedStateSchemaVersion, Rules: testRules(), Latest: desired, Target: desired,
		History: []topology.Document{base}, Operations: []topology.Operation{first, retry},
		Groups: []groupConvergence{{GroupID: topology.MetaGroupID, Declaration: desired.MetaGroup(), SourceVersion: 2, CompletedAt: at},
			{GroupID: "shard0", Declaration: base.Group("shard0"), SourceVersion: 1, Owner: retry.Status.ID, CompletedAt: at}},
		Nodes:          []nodeConvergence{{NodeName: "n1", Declaration: base.Node("n1"), MemberID: 1, SourceVersion: 1, CompletedAt: at}},
		Allocations:    []allocationRecord{{GroupID: topology.MetaGroupID, NextReplicaID: 2}, {GroupID: "shard0", NextReplicaID: 2}},
		FullyConverged: topology.FullyConverged{Version: 1, At: at}, AppliedIndex: 10, AppliedTerm: 1}
}

func TestScopedGraphPreservesRetrySiblingAndHistoricalMarker(t *testing.T) {
	t.Parallel()
	state := scopedGraphFixture()
	state.indexes = &scopedIndexes{}
	before, _ := json.Marshal(state)
	index := state.indexes
	if _, err := prepareScopedGraph(&state, testRules()); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(state)
	if !bytes.Equal(before, after) || state.indexes != index {
		t.Fatal("validation mutated the candidate or published indexes")
	}
	// Required history may exceed the old rolling window. The marker stays v1.
	for version := uint64(2); version <= historyRetention+10; version++ {
		doc := state.Latest.Clone()
		doc.Version, doc.ParentVersion = version, version-1
		state.History = append(state.History, doc)
		state.Latest, state.Target = doc, doc
	}
	if _, err := prepareScopedGraph(&state, testRules()); err != nil {
		t.Fatal("history cap or current-Target equality rejected retained proof", err)
	}
}

func TestScopedGraphRejectsCorruptAttemptsWithoutMutation(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*scopedState){
		"missing approved document": func(s *scopedState) { s.Target.Version, s.Latest.Version = 3, 3 },
		"missing base":              func(s *scopedState) { s.History = nil; s.FullyConverged = topology.FullyConverged{} },
		"future operation":          func(s *scopedState) { s.Operations[1].Status.Version = 3 },
		"mismatched plan":           func(s *scopedState) { s.Operations[1].Plan.Version++ },
		"truncated":                 func(s *scopedState) { s.Operations[1].Status.BlocksTruncated = true },
		"missing block":             func(s *scopedState) { s.Operations[1].Status.Blocks = s.Operations[1].Status.Blocks[:1] },
		"wrong count":               func(s *scopedState) { s.Operations[1].Status.TotalBlocks++ },
		"no partition":              func(s *scopedState) { s.Operations[1].Plan.Blocks = nil },
		"duplicate partition":       func(s *scopedState) { s.Operations[1].Plan.Blocks[1].Steps = []int{1, 2} },
		"unknown action":            func(s *scopedState) { s.Operations[1].Plan.Steps[1].Kind = "other" },
		"wrong replica":             func(s *scopedState) { s.Operations[1].Plan.Steps[1].ReplicaID = 0 },
		"wrong sequence":            func(s *scopedState) { s.Operations[1].Plan.Steps[1].Seq = 3 },
		"first has previous":        func(s *scopedState) { s.Operations[0].Previous = s.Operations[1].Status.ID },
		"self cycle":                func(s *scopedState) { s.Operations[1].Previous = s.Operations[1].Status.ID },
		"missing previous":          func(s *scopedState) { s.Operations[1].Previous = "gone" },
		"skip attempt": func(s *scopedState) {
			s.Operations[1].Status.Attempt, s.Operations[1].Status.ID = 3, topology.MakeOperationID(2, 3)
		},
		"changed plan":            func(s *scopedState) { s.Operations[1].Plan.Steps[1].ReplicaID = 2 },
		"restarted sibling":       func(s *scopedState) { s.Operations[1].Status.Blocks[0].State = topology.OperationPending },
		"failed became success":   func(s *scopedState) { s.Operations[0].Status.State = topology.OperationSucceeded },
		"pending became terminal": func(s *scopedState) { s.Operations[1].Status.State = topology.OperationFailed },
		"cursor outside block":    func(s *scopedState) { s.Operations[1].Status.Blocks[1].CurrentStep = 1 },
		"incomplete success":      func(s *scopedState) { s.Operations[0].Status.Blocks[0].CurrentStep = 0 },
		"clock reversal":          func(s *scopedState) { s.Operations[1].UpdatedAt = time.Time{} },
		"unsupported barrier": func(s *scopedState) {
			s.Operations[1].Plan.Dependencies = []topology.StepDependency{{StepSeq: 2, Requires: []int{1}}}
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			state := scopedGraphFixture()
			corrupt(&state)
			state.indexes = &scopedIndexes{}
			index := state.indexes
			before, _ := json.Marshal(state)
			if _, err := prepareScopedGraph(&state, testRules()); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("corrupt graph accepted", err)
			}
			after, _ := json.Marshal(state)
			if !bytes.Equal(before, after) || index != state.indexes {
				t.Fatal("refusal mutated candidate evidence")
			}
			good, err := json.Marshal(scopedGraphFixture())
			if err != nil {
				t.Fatal(err)
			}
			var restored scopedState
			if err := json.Unmarshal(good, &restored); err != nil {
				t.Fatal(err)
			}
			retainedIndex := restored.indexes
			if err := json.Unmarshal(before, &restored); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("decoder installed invalid graph", err)
			}
			retained, err := json.Marshal(restored)
			if err != nil || !bytes.Equal(good, retained) || restored.indexes != retainedIndex {
				t.Fatal("failed graph decode replaced prior state", err)
			}
		})
	}
}

func TestScopedGraphDocumentBoundaries(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*scopedState){
		"cluster identity":      func(s *scopedState) { s.History[0].ClusterID = "foreign" },
		"parent cycle":          func(s *scopedState) { s.History[0].ParentVersion = 1 },
		"unversioned target":    func(s *scopedState) { s.Target.Version, s.Target.ParentVersion = 0, 0 },
		"unversioned history":   func(s *scopedState) { s.History[0].Version = 0 },
		"missing marker source": func(s *scopedState) { s.FullyConverged.Version = 3 },
		"missing marker time":   func(s *scopedState) { s.FullyConverged.At = time.Time{} },
		"invalid applied pair":  func(s *scopedState) { s.AppliedTerm = 0 },
		"target ahead":          func(s *scopedState) { s.Target.Version = 4 },
		"rebase before repair":  func(s *scopedState) { s.Latest.RebasedFrom, s.Target.RebasedFrom = 1, 1 },
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			state := scopedGraphFixture()
			corrupt(&state)
			if _, err := prepareScopedGraph(&state, testRules()); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("invalid document graph accepted", err)
			}
		})
	}
	state := scopedGraphFixture()
	state.Rules.Limits.MaxNodes++
	if _, err := prepareScopedGraph(&state, testRules()); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("different cluster rules accepted", err)
	}
	state = scopedGraphFixture()
	state.SchemaVersion = 3
	if _, err := prepareScopedGraph(&state, testRules()); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("legacy state accepted", err)
	}
}
