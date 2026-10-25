//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/recoverytest"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func recoveryFoundingFixture(t *testing.T) scopedState {
	t.Helper()
	s := scopedState{SchemaVersion: scopedStateSchemaVersion, Rules: testRules()}
	prepareScopedFixture(t, &s)
	doc := baseDoc()
	doc.Groups = doc.Groups[:1]
	doc.ClusterID, doc.Origin, doc.CreatedAt = "recovered", topology.OriginFounding, time.Unix(100, 0).UTC()
	doc.MetaGroup().Replicas[0].ReplicaID, doc.MetaGroup().NextReplicaID = 5, 6
	seed := raftpb.ConfState{Voters: []uint64{5}}
	if _, err := foundingCommand(t, &s, ProposeDocument{Doc: doc}, seed); err != nil {
		t.Fatal(err)
	}
	input := topology.PlanInput{Native: map[string]topology.NativeState{topology.MetaGroupID: {GroupID: topology.MetaGroupID, ReplicaID: 5, Applied: 1, ConfState: seed}}}
	plan, err := topology.BuildPlan(input, s.Latest, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	approval := BeginOperationProposal{Version: 1, Plan: plan, At: doc.CreatedAt, Actor: "n1", Origin: topology.OriginFounding, Scopes: []ScopeExpectation{{GroupID: topology.MetaGroupID}}, ProposalID: "found"}
	if _, err := foundingCommand(t, &s, approval, seed); err != nil {
		t.Fatal(err)
	}
	bind := BindAdmissionProposal{Founding: true, At: doc.CreatedAt, ProposalID: "bind", Admission: AdmissionRecord{NodeName: "n1", MemberID: 5, InstanceID: "B-process", OperationID: topology.MakeOperationID(1, 1)}}
	if _, err := foundingCommand(t, &s, bind, seed); err != nil {
		t.Fatal(err)
	}
	if _, err := foundingCommand(t, &s, GrantSession{SessionID: "B-session", NodeName: "n1", InstanceID: "B-process", TTLSeconds: 15, ProposalID: "session"}, seed); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRecoveryImportFailuresLeaveWholeCandidateUnchanged(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*scopedState, *model.ImportProposal){
		"wrong source lineage": func(_ *scopedState, p *model.ImportProposal) { p.Source.ClusterID = p.Survivor.ClusterID },
		"wrong destination":    func(_ *scopedState, p *model.ImportProposal) { p.Survivor.ReplicaID++ },
		"bad source coverage":  func(_ *scopedState, p *model.ImportProposal) { p.Sources = nil },
		"staged version":       func(s *scopedState, _ *model.ImportProposal) { s.Latest.Version = 2 },
		"unactivated founder":  func(s *scopedState, _ *model.ImportProposal) { s.Activated = nil },
		"unbound founder":      func(s *scopedState, _ *model.ImportProposal) { s.Admissions = nil },
		"allocation overflow":  func(_ *scopedState, p *model.ImportProposal) { p.Floors[1].NextID = math.MaxUint64 },
		"retired live group":   func(_ *scopedState, p *model.ImportProposal) { p.Retired = []string{"shard0"} },
		"native absent":        func(s *scopedState, _ *model.ImportProposal) { s.nativeConf = nil },
		"native extra voter":   func(s *scopedState, _ *model.ImportProposal) { s.nativeConf.Voters = append(s.nativeConf.Voters, 8) },
		"source next floor":    func(_ *scopedState, p *model.ImportProposal) { p.ApprovedSource.Group("shard0").NextReplicaID = 10 },
		"duplicate floor":      func(_ *scopedState, p *model.ImportProposal) { p.Floors = append(p.Floors, p.Floors[0]) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := recoveryFoundingFixture(t)
			p := recoveryImportFixture()
			p.Sources[0].Identity.ClusterID = p.Source.ClusterID
			s.nativeConf = &raftpb.ConfState{Voters: []uint64{5}}
			change(&s, &p)
			before, _ := json.Marshal(s)
			if _, err := s.applyRecoveryImport(s.AppliedIndex+1, p); err == nil {
				t.Fatal("invalid import accepted")
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("failed atomic import leaked owned changes")
			}
		})
	}
}

func TestRecoveryReplicatedImportRestoresAndKeepsOriginalRetryPayload(t *testing.T) {
	t.Parallel()
	s := recoveryFoundingFixture(t)
	p := recoveryImportFixture()
	p.Sources[0].Identity.ClusterID = p.Source.ClusterID
	seed := raftpb.ConfState{Voters: []uint64{5}}
	result, err := foundingCommand(t, &s, p, seed)
	if err != nil || !result.result.Accepted {
		t.Fatal(result, err)
	}
	encoded, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.Recovery == nil || restored.Recovery.RequestDigest == "" {
		t.Fatal("restored import lost exact-request provenance")
	}
	result, err = restored.applyRecoveryImport(restored.AppliedIndex+1, p)
	if err != nil || result.changed {
		t.Fatal(result, err)
	}
	p.Floors[1].NextID++
	if _, err := restored.applyRecoveryImport(restored.AppliedIndex+1, p); err == nil {
		t.Fatal("changed floors matched an old digest")
	}
	if len(restored.RetiredGroups) != 1 || restored.RetiredGroups[0].ID != "retired" {
		t.Fatal("non-reuse tombstone lost")
	}
	if floor := slices.IndexFunc(restored.Allocations, func(row allocationRecord) bool { return row.GroupID == "staged-only" }); floor < 0 || restored.Allocations[floor].NextReplicaID != 50 {
		t.Fatal("staged-only floor was dropped")
	}
	if restored.Target.Group("staged-only") != nil {
		t.Fatal("staged-only group intent was imported")
	}
}

func recoveryImportFixture() model.ImportProposal {
	old := baseDoc()
	old.Version = 1
	accepted := recoverytest.Accepted(model.Paths{StorageDir: "/system"}, "recovery")
	source := accepted.Sources[0]
	source.GroupID, source.Identity.GroupID = "shard0", "shard0"
	return model.ImportProposal{Schema: model.SchemaVersion, RecoveryID: "recovery", PlanDigest: strings.Repeat("a", 64), ExpectedVersion: 1,
		Source:         topology.Identity{NodeName: "n1", ClusterID: old.ClusterID, MemberID: 1},
		Survivor:       storage.Identity{NodeName: "n1", ClusterID: "recovered", GroupID: topology.MetaGroupID, ReplicaID: 5},
		ApprovedSource: old, Sources: []model.SeedSource{source}, Floors: []model.AllocationFloor{{GroupID: topology.MetaGroupID, NextID: 5}, {GroupID: "shard0", NextID: 7}, {GroupID: "staged-only", NextID: 50}, {GroupID: "retired", NextID: 9}},
		Retired: []string{"retired"}, Actor: "operator", Comment: "permanent majority loss", At: time.Unix(110, 0).UTC(), ProposalID: "recovery-import"}
}

func TestRecoveryImportPreparesOneOrdinaryOperationAtomically(t *testing.T) {
	t.Parallel()
	s := recoveryFoundingFixture(t)
	p := recoveryImportFixture()
	p.Sources[0].Identity.ClusterID = p.Source.ClusterID
	s.nativeConf = &raftpb.ConfState{Voters: []uint64{5}}
	result, err := s.applyRecoveryImport(s.AppliedIndex+1, p)
	if err != nil {
		t.Fatal(err)
	}
	if !result.changed || !result.result.Accepted || s.Target.Version != 2 || s.Latest.Version != 2 || s.Recovery == nil {
		t.Fatal(result, s.Recovery)
	}
	if s.Target.ClusterID != p.Survivor.ClusterID || s.Target.Group("shard0").Replicas[0].ReplicaID != 7 || s.Target.MetaGroup().Replicas[0].ReplicaID != 5 || s.importPlacement != nil {
		t.Fatal(s.Target)
	}
	if s.Recovery.RequestDigest == "" || s.Recovery.Phase != "importing" || len(s.Recovery.Groups) != 1 || len(s.Recovery.Policies) != 2 {
		t.Fatal(s.Recovery)
	}
	op := s.indexes.operations[result.result.OperationID]
	found := false
	for _, step := range op.Plan.Steps {
		if step.Kind == topology.StepCreateFoundingGroup && step.GroupID == "shard0" {
			found = true
		}
	}
	if !found || len(s.Operations) != 2 {
		t.Fatal("import bypassed ordinary founding operation", op)
	}
	before, _ := json.Marshal(s)
	repeated, err := s.applyRecoveryImport(s.AppliedIndex+1, p)
	after, _ := json.Marshal(s)
	if err != nil || repeated.changed || repeated.result.OperationID != result.result.OperationID || !bytes.Equal(before, after) {
		t.Fatal("exact retry changed accepted state", repeated, err)
	}
	p.Comment = "changed audit"
	if _, err := s.applyRecoveryImport(s.AppliedIndex+1, p); err == nil {
		t.Fatal("changed import payload replayed as exact")
	}
}
