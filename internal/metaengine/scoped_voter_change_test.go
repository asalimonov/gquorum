//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func scopedVoterChangeFixture(t *testing.T, group string, surge int) (scopedState, CommitVoterChangeProposal) {
	t.Helper()
	rules := topology.Rules{SchemaVersion: 1, Limits: topology.Limits{MaxNodes: 5, MaxReplicas: 5}}
	config := topology.DefaultVoterConfig()
	config.MaxSurgeVoters = surge
	at := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	base := topology.Document{Version: 1, ClusterID: "cluster", CreatedAt: at,
		Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 5}, Groups: []topology.Group{
			{ID: topology.MetaGroupID, NextReplicaID: 6, VoterConfig: &config},
			{ID: "work", NextReplicaID: 6, VoterConfig: &config, Hosting: &topology.HostingRule{Mode: topology.HostingAllEligible}},
		}}
	for i := 1; i <= 5; i++ {
		name := fmt.Sprintf("n%d", i)
		base.Nodes = append(base.Nodes, topology.Node{Name: name, Type: "worker", State: topology.NodeActive})
		role := topology.ReplicaVoter
		if i > 3 {
			role = topology.ReplicaLearner
		}
		for j := range base.Groups {
			base.Groups[j].Replicas = append(base.Groups[j].Replicas,
				topology.Replica{NodeName: name, ReplicaID: uint64(i), Role: role})
		}
	}
	s := scopedState{SchemaVersion: scopedStateSchemaVersion, Rules: rules, Target: base, Latest: base.Clone(),
		AppliedIndex: 10, AppliedTerm: 1, FullyConverged: topology.FullyConverged{Version: 1, At: at}}
	s.Latest.Version, s.Latest.ParentVersion, s.Latest.CreatedAt = 2, 1, at.Add(time.Second-time.Nanosecond)
	other := topology.MetaGroupID
	if group == other {
		other = "work"
	}
	s.Latest.Group(other).Labels = map[string]string{"user": "staged"}
	for _, g := range base.Groups {
		s.Groups = append(s.Groups, groupConvergence{GroupID: g.ID, Declaration: base.Group(g.ID), SourceVersion: 1, CompletedAt: base.CreatedAt})
		s.Allocations = append(s.Allocations, allocationRecord{g.ID, g.NextReplicaID})
	}
	for _, n := range base.Nodes {
		s.Nodes = append(s.Nodes, nodeConvergence{NodeName: n.Name, Declaration: base.Node(n.Name), MemberID: base.MemberID(n.Name), SourceVersion: 1, CompletedAt: base.CreatedAt})
	}
	prepareScopedFixture(t, &s)
	change := topology.VoterChange{GroupID: group, Promote: &topology.ReplicaKey{NodeName: "n4", ReplicaID: 4},
		Demote: &topology.ReplicaKey{NodeName: "n3", ReplicaID: 3}}
	desired, err := VoterChangeDocument(base, change, at.Add(time.Second), "scheduler")
	if err != nil {
		t.Fatal(err)
	}
	desired.Version = 3
	statuses := assumedReadmissionStatuses(base, "")
	for id, status := range statuses {
		status.HealthKnown = true
		status.LiveVoters = map[uint64]bool{1: true, 2: true, 3: false, 4: true, 5: true}
		statuses[id] = status
	}
	input := testPlanningInput(t, restoreMachineFixture(t, s), desired, statuses)
	plan, err := topology.BuildPlan(input, desired, rules)
	if err != nil {
		t.Fatal(err)
	}
	p := CommitVoterChangeProposal{Change: change, Doc: desired, ExpectedVersion: 2, ExpectedTarget: 1,
		Scopes: []ScopeExpectation{{GroupID: group, SourceVersion: 1}}, Plan: plan,
		At: at.Add(time.Second), ProposalID: "replace"}
	return s, p
}

func TestScopedVoterChangeRebasesOnlyIndependentStagedIntent(t *testing.T) {
	t.Parallel()
	for _, group := range []string{topology.MetaGroupID, "work"} {
		t.Run(group, func(t *testing.T) {
			t.Parallel()
			s, p := scopedVoterChangeFixture(t, group, 1)
			body, _ := json.Marshal(p)
			result, err := applyScopedProposal(&s, 11, KindCommitVoterChange, body)
			if err != nil || !result.changed || !result.result.Accepted || s.Target.Version != 3 || s.Latest.Version != 4 ||
				s.Latest.RebasedFrom != 2 || s.indexes.groups[group].Owner != "exec-v3-a1" || len(s.VoterSwitches) != 0 {
				t.Fatal("atomic voter repair failed or fabricated completion", result, err)
			}
			for _, g := range s.Target.Groups {
				if g.ID != group && (g.Labels != nil || s.Latest.Group(g.ID).Labels["user"] != "staged") {
					t.Fatal("staged edit lost or approved")
				}
			}
			snapshot, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			var restored scopedState
			if err := json.Unmarshal(snapshot, &restored); err != nil {
				t.Fatal(err)
			}
			result, err = applyScopedProposal(&restored, 12, KindCommitVoterChange, body)
			if err != nil || result.changed || result.result.Code != proposalVersionConflict {
				t.Fatal("duplicate bypassed captured CAS after restore", result, err)
			}
		})
	}
}

func TestScopedVoterChangeRetainsExistingGuardrails(t *testing.T) {
	t.Parallel()
	for name, corrupt := range map[string]func(*CommitVoterChangeProposal){
		"timestamp":        func(p *CommitVoterChangeProposal) { p.At = p.At.Add(time.Second) },
		"host":             func(p *CommitVoterChangeProposal) { p.Doc.Group("work").Replicas[0].NodeName = "n4" },
		"voter config":     func(p *CommitVoterChangeProposal) { p.Doc.Group("work").VoterConfig.FailureWindow++ },
		"origin":           func(p *CommitVoterChangeProposal) { p.Doc.Origin = topology.OriginOperator },
		"native":           func(p *CommitVoterChangeProposal) { p.Plan.VoterBases[0].Voters = []uint64{1, 2, 4} },
		"missing demotion": func(p *CommitVoterChangeProposal) { p.Change.Demote = nil },
		"guard":            func(p *CommitVoterChangeProposal) { p.Plan.Steps[len(p.Plan.Steps)-1].TransferReplicaID = 99 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s, p := scopedVoterChangeFixture(t, "work", 1)
			corrupt(&p)
			before, _ := json.Marshal(s)
			result, err := s.applyCommitVoterChange(11, p)
			if err == nil && result.result.Accepted || result.changed {
				t.Fatal("voter guard bypassed", result, err)
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("voter refusal partially installed repair")
			}
		})
	}
}

func TestScopedVoterChangeKeepsDemoteFirstAtCapacity(t *testing.T) {
	t.Parallel()
	for _, group := range []string{topology.MetaGroupID, "work"} {
		t.Run(group, func(t *testing.T) {
			t.Parallel()
			s, p := scopedVoterChangeFixture(t, group, 0)
			result, err := s.applyCommitVoterChange(11, p)
			if err != nil || !result.changed || !result.result.Accepted {
				t.Fatal("safe at-capacity guarded role change refused", result, err)
			}
		})
	}
}
