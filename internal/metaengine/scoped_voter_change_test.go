//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func scopedVoterChangeFixture(t *testing.T, group string, surge int) (scopedState, CommitVoterChangeProposal) {
	t.Helper()
	legacy, p, _ := voterChangeFixture(t, group, surge)
	base := legacy.Active()
	base.CreatedAt = p.At.Add(-time.Second)
	s := scopedState{SchemaVersion: scopedStateSchemaVersion, Rules: legacy.rules, Target: base, Latest: base.Clone(),
		AppliedIndex: 10, AppliedTerm: 1, FullyConverged: topology.FullyConverged{Version: 1, At: base.CreatedAt}}
	s.Latest.Version, s.Latest.ParentVersion, s.Latest.CreatedAt = 2, 1, p.At.Add(-time.Nanosecond)
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
	p.ExpectedVersion, p.ExpectedTarget, p.Doc.Version, p.Plan.Version = 2, 1, 3, 3
	p.Scopes = []ScopeExpectation{{GroupID: group, SourceVersion: 1}}
	p.Plan.Blocks = []topology.PlanBlock{{GroupID: group, SourceVersion: 1}}
	for _, step := range p.Plan.Steps {
		p.Plan.Blocks[0].Steps = append(p.Plan.Blocks[0].Steps, step.Seq)
	}
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
