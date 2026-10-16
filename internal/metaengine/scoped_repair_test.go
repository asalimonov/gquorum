//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func repairCommitFixture(t *testing.T) (scopedState, CommitRepairOperationProposal) {
	t.Helper()
	s, _ := rebaseFixture(t)
	for _, doc := range []*topology.Document{&s.Target, &s.Latest} {
		g := doc.Group("b")
		g.Replicas = nil
		for i, node := range doc.Nodes {
			g.Replicas = append(g.Replicas, topology.Replica{NodeName: node.Name, ReplicaID: uint64(i + 1), Role: topology.ReplicaVoter})
		}
		g.NextReplicaID = 4
	}
	s.History = []topology.Document{s.Target.Clone()}
	for i := range s.Groups {
		s.Groups[i].Declaration = s.Target.Group(s.Groups[i].GroupID)
	}
	for i := range s.Allocations {
		s.Allocations[i].NextReplicaID = s.Latest.Group(s.Allocations[i].GroupID).NextReplicaID
	}
	s.Intents = []Intent{{Version: 2, ExpectedVersion: 1, Kind: "set_group_placement", Digest: strings.Repeat("a", 64), GroupID: "shard0"}}
	prepareScopedFixture(t, &s)
	at := s.Latest.CreatedAt.Add(time.Second)
	repaired, err := ReplicaRepairDocument(s.Target, s.Latest, ReplicaRepairOptions{GroupID: "b", NodeName: "n2", ReplicaID: 2,
		At: at, Origin: topology.OriginReconciler, Actor: "repair"})
	if err != nil {
		t.Fatal(err)
	}
	p := CommitRepairOperationProposal{Doc: repaired.Doc, ExpectedVersion: s.Latest.Version, ExpectedTarget: s.Target.Version,
		At: at, ProposalID: "repair"}
	p.Plan = repairFixturePlan(t, &s, p.Doc)
	for _, block := range p.Plan.Blocks {
		p.Scopes = append(p.Scopes, ScopeExpectation{GroupID: block.GroupID, SourceVersion: block.SourceVersion})
	}
	return s, p
}

// The legacy public constructor remains flat until78. These ordinary reseat/role
// fixtures partition that exact plan; no admission or deletion edges are involved.
func repairFixturePlan(t *testing.T, s *scopedState, desired topology.Document) topology.Plan {
	t.Helper()
	collector := s.planningCollector()
	input, err := collector.collect(s.planningDeclarations(), desired)
	if err != nil {
		t.Fatal(err)
	}
	input.Native = make(map[string]topology.NativeState)
	for id, status := range assumedReadmissionStatuses(s.Target, "") {
		status.HealthKnown = true
		status.LiveVoters = make(map[uint64]bool)
		for _, id := range status.ConfState.Voters {
			status.LiveVoters[id] = true
		}
		row := input.Groups[id]
		row.Observation = &status
		input.Groups[id] = row
		input.Native[id] = topology.NativeState{GroupID: id, ReplicaID: status.ConfState.Voters[0], Applied: 1, ConfState: status.ConfState}
	}
	plan, err := topology.BuildPlan(input, desired, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range topology.AffectedGroups(s.Target, desired) {
		block := topology.PlanBlock{GroupID: id, SourceVersion: input.Groups[id].ConvergedVersion}
		for _, step := range plan.Steps {
			if step.GroupID != id {
				continue
			}
			if step.Kind == topology.StepCleanupReplicaState {
				block.Cleanup = append(block.Cleanup, step.Seq)
			} else {
				block.Steps = append(block.Steps, step.Seq)
			}
		}
		plan.Blocks = append(plan.Blocks, block)
	}
	return plan
}

func TestScopedRepairAtomicallyInstallsOperationAndPreservedIntent(t *testing.T) {
	t.Parallel()
	s, p := repairCommitFixture(t)
	staged, intents := s.Latest.Clone(), slices.Clone(s.Intents)
	body, _ := json.Marshal(p)
	prior, _ := json.Marshal(s)
	result, err := applyScopedProposal(&s, 11, "commit_repair_operation", body)
	if err != nil || !result.changed || !result.result.Accepted {
		t.Fatal(result, err)
	}
	if s.AppliedIndex != 11 || s.Target.Version != 3 || s.Latest.Version != 4 || s.Latest.RebasedFrom != 2 ||
		result.result.Doc.Version != 3 || result.result.OperatorID != "exec-v3-a1" ||
		s.indexes.groups["b"].Owner != "exec-v3-a1" || s.indexes.groups["shard0"].Owner != "" ||
		s.Target.Group("shard0").Labels != nil || documentReplica(s.Latest, "b", 4).NodeName != "n2" ||
		!reflect.DeepEqual(s.Latest.Audit, staged.Audit) || !reflect.DeepEqual(s.Intents, intents) ||
		s.indexes.documents[s.Latest.RebasedFrom].Version != intents[0].Version {
		t.Fatal("transaction lost scope ownership, documents or original intent", s)
	}
	before, _ := json.Marshal(s)
	var replayed scopedState
	if err := json.Unmarshal(prior, &replayed); err != nil {
		t.Fatal(err)
	}
	if applied, err := applyScopedProposal(&replayed, 11, "commit_repair_operation", body); err != nil || !applied.changed {
		t.Fatal("retained log entry did not replay", applied, err)
	}
	encoded, _ := json.Marshal(replayed)
	if !bytes.Equal(before, encoded) {
		t.Fatal("replicas installed different documents/proof at the same apply index")
	}
	var restored scopedState
	if err := json.Unmarshal(before, &restored); err != nil {
		t.Fatal(err)
	}
	for _, state := range []*scopedState{&s, &restored} {
		replay, err := applyScopedProposal(state, 12, "commit_repair_operation", body)
		if err != nil || replay.changed || replay.result.Code != proposalVersionConflict || replay.result.CurrentVersion != 4 {
			t.Fatal("stale repair replay changed accepted transaction", replay, err)
		}
		after, _ := json.Marshal(state)
		if !bytes.Equal(before, after) {
			t.Fatal("snapshot/replay changed complete installed transaction")
		}
	}
}

func TestScopedRepairRejectsForgedCaptureAndEditsAtomically(t *testing.T) {
	t.Parallel()
	for name, corrupt := range map[string]func(*scopedState, *CommitRepairOperationProposal){
		"latest": func(_ *scopedState, p *CommitRepairOperationProposal) { p.ExpectedVersion++ },
		"target": func(_ *scopedState, p *CommitRepairOperationProposal) { p.ExpectedTarget++ },
		"source": func(_ *scopedState, p *CommitRepairOperationProposal) { p.Scopes[0].SourceVersion++ },
		"owner":  func(_ *scopedState, p *CommitRepairOperationProposal) { p.Scopes[0].Owner = "exec-v1-a1" },
		"omitted scope": func(_ *scopedState, p *CommitRepairOperationProposal) {
			p.Scopes = p.Scopes[1:]
			p.Plan.Blocks = p.Plan.Blocks[1:]
		},
		"native guard": func(_ *scopedState, p *CommitRepairOperationProposal) { p.Plan.VoterBases[0].Voters = []uint64{2} },
		"scope changed": func(s *scopedState, _ *CommitRepairOperationProposal) {
			s.Latest.Group("b").Labels = map[string]string{"user": "edit"}
			prepareScopedFixture(t, s)
		},
		"unrelated label": func(_ *scopedState, p *CommitRepairOperationProposal) {
			p.Doc.Group("shard0").Labels = map[string]string{"repair": "edit"}
		},
		"lifecycle":        func(_ *scopedState, p *CommitRepairOperationProposal) { p.Doc.Node("n2").State = topology.NodeDraining },
		"origin":           func(_ *scopedState, p *CommitRepairOperationProposal) { p.Doc.Origin = topology.OriginAPI },
		"old clock":        func(_ *scopedState, p *CommitRepairOperationProposal) { p.At = p.At.Add(-time.Hour) },
		"extra allocation": func(_ *scopedState, p *CommitRepairOperationProposal) { p.Doc.Group("b").NextReplicaID++ },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s, p := repairCommitFixture(t)
			corrupt(&s, &p)
			before, _ := json.Marshal(s)
			result, err := s.applyCommitRepairOperation(11, p)
			if err == nil && result.result.Accepted || result.changed {
				t.Fatal("forged repair published", result, err)
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("refusal published a partial transaction")
			}
		})
	}
}

func TestScopedRepairRefusesOwnedGroupForSchedulerAndConvenienceReseat(t *testing.T) {
	t.Parallel()
	for _, origin := range []topology.Origin{topology.OriginReconciler, topology.OriginOperator} {
		t.Run(string(origin), func(t *testing.T) {
			t.Parallel()
			s, p := repairCommitFixture(t)
			s.Latest = p.Doc.Clone()
			s.Latest.Version, s.Latest.ParentVersion = 2, 1
			s.Intents = nil
			for i := range s.Allocations {
				s.Allocations[i].NextReplicaID = s.Latest.Group(s.Allocations[i].GroupID).NextReplicaID
			}
			prepareScopedFixture(t, &s)
			plan := p.Plan.Clone()
			plan.Version = 2
			user := BeginOperationProposal{Version: 2, Plan: plan, Scopes: p.Scopes, Actor: "user", Origin: topology.OriginOperator, At: p.At}
			if _, err := s.applyBeginOperation(11, user); err != nil {
				t.Fatal(err)
			}
			repair, err := ReplicaRepairDocument(s.Target, s.Latest, ReplicaRepairOptions{GroupID: "b", NodeName: "n2", ReplicaID: 4,
				At: p.At.Add(time.Second), Origin: origin, Actor: "reseat"})
			if err != nil {
				t.Fatal(err)
			}
			p.Doc, p.ExpectedVersion, p.ExpectedTarget, p.At = repair.Doc, 2, 2, p.At.Add(time.Second)
			for i := range p.Scopes {
				row := s.indexes.groups[p.Scopes[i].GroupID]
				p.Scopes[i].Owner, p.Scopes[i].SourceVersion = row.Owner, row.SourceVersion
				p.Plan.Blocks[i].SourceVersion = row.SourceVersion
			}
			before, _ := json.Marshal(s)
			if result, err := s.applyCommitRepairOperation(12, p); err != nil || result.changed ||
				result.result.Code != proposalAuthorityPending || result.result.OperatorID != "exec-v2-a1" {
				t.Fatal("repair seized existing user scope", result, err)
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("failed convenience operation rewrote user authority")
			}
		})
	}
}

func TestScopedRepairWithoutStagingAllocatesOneDocument(t *testing.T) {
	t.Parallel()
	s, p := repairCommitFixture(t)
	s.Latest, s.Intents = s.Target.Clone(), nil
	for i := range s.Allocations {
		s.Allocations[i].NextReplicaID = s.Latest.Group(s.Allocations[i].GroupID).NextReplicaID
	}
	prepareScopedFixture(t, &s)
	p.ExpectedVersion, p.Doc.Version, p.Plan.Version = 1, 2, 2
	result, err := s.applyCommitRepairOperation(11, p)
	if err != nil || !result.changed || s.Latest.Version != 2 || !reflect.DeepEqual(s.Latest, s.Target) ||
		s.Latest.RebasedFrom != 0 || s.indexes.first[2] != "exec-v2-a1" {
		t.Fatal("unstaged repair did not commit one complete document", result, err)
	}
}

func TestScopedRepairLeavesIndependentUserOperationOwned(t *testing.T) {
	t.Parallel()
	s, _ := repairCommitFixture(t)
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = 2, 1
	s.Latest.Group("shard0").Replicas = append(s.Latest.Group("shard0").Replicas,
		topology.Replica{NodeName: "n2", ReplicaID: 8, Role: topology.ReplicaLearner})
	s.Latest.Group("shard0").NextReplicaID = 9
	for i := range s.Allocations {
		s.Allocations[i].NextReplicaID = s.Latest.Group(s.Allocations[i].GroupID).NextReplicaID
	}
	prepareScopedFixture(t, &s)
	at := s.Target.CreatedAt.Add(time.Second)
	user := BeginOperationProposal{Version: 2, Plan: repairFixturePlan(t, &s, s.Latest),
		Scopes: []ScopeExpectation{{GroupID: "shard0", SourceVersion: 1}}, Actor: "user", Origin: topology.OriginOperator, At: at}
	if _, err := s.applyBeginOperation(11, user); err != nil {
		t.Fatal(err)
	}
	original := s.Operations[0].Clone()
	repaired, err := ReplicaRepairDocument(s.Target, s.Latest, ReplicaRepairOptions{GroupID: "b", NodeName: "n2", ReplicaID: 2,
		At: at.Add(time.Second), Origin: topology.OriginReconciler, Actor: "repair"})
	if err != nil {
		t.Fatal(err)
	}
	// Use the accepted ordinary replacement grammar. The flat legacy planner
	// would also plan the unchanged user's pending learner; scoped work excludes it.
	_, template := repairCommitFixture(t)
	p := template
	p.Doc, p.ExpectedVersion, p.ExpectedTarget, p.At = repaired.Doc, 2, 2, at.Add(time.Second)
	result, err := s.applyCommitRepairOperation(12, p)
	if err != nil || !result.changed || s.indexes.groups["shard0"].Owner != "exec-v2-a1" ||
		!reflect.DeepEqual(s.Operations[0], original) || s.indexes.groups["b"].Owner != "exec-v3-a1" {
		t.Fatal("unrelated user operation blocked repair or lost ownership", result, err)
	}
}
