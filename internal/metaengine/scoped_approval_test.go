//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func approvalFixture(t *testing.T, group string) (scopedState, BeginOperationProposal) {
	t.Helper()
	s := scopedGraphFixture()
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = 3, 2
	if group == topology.MetaGroupID {
		s.Latest.Node("n1").Labels = map[string]string{"owner": "jobs"}
	} else {
		s.Latest.Group(group).Labels = map[string]string{"owner": "jobs"}
	}
	var err error
	s.indexes, err = prepareScopedGraph(&s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	row := s.indexes.groups[group]
	p := BeginOperationProposal{Version: 3, Plan: topology.Plan{Version: 3,
		Blocks: []topology.PlanBlock{{GroupID: group, SourceVersion: row.SourceVersion}}},
		Scopes: []ScopeExpectation{{GroupID: group, SourceVersion: row.SourceVersion, Owner: row.Owner}},
		At:     s.Operations[0].UpdatedAt.Add(time.Second), Actor: "admin", Origin: topology.OriginAPI, ProposalID: "approve"}
	return s, p
}

func TestScopedApprovalPreparesOnlyAffectedOwners(t *testing.T) {
	t.Parallel()
	for _, group := range []string{topology.MetaGroupID, "shard0"} {
		t.Run(group, func(t *testing.T) {
			t.Parallel()
			s, p := approvalFixture(t, group)
			before, _ := json.Marshal(s)
			next, err := prepareScopedApproval(&s, s.Latest, p, explicitApproval)
			if err != nil {
				t.Fatal(err)
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) || next.Target.Version != 3 || len(next.Operations) != 3 {
				t.Fatal("approval mutated original state or lost an operation")
			}
			if group == topology.MetaGroupID {
				if next.Groups[1].Owner != s.Groups[1].Owner || !reflect.DeepEqual(next.Operations[:2], s.Operations) {
					t.Fatal("unavailable sibling lost its owner/progress")
				}
			} else if next.Operations[1].Status.State != topology.OperationSuperseded ||
				next.Operations[1].Status.Blocks[0].State != topology.OperationSucceeded {
				t.Fatal("supersession changed the completed sibling")
			}
			next.Groups[0].Declaration.Labels = map[string]string{"changed": "copy"}
			next.Operations[0].Plan.Steps[0].NodeName = "copy"
			after, _ = json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("prepared declarations or operation alias their source")
			}
		})
	}
}

func TestScopedApprovalRefusesForgedScopesWithoutMutation(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*BeginOperationProposal){
		"omitted": func(p *BeginOperationProposal) { p.Plan.Blocks, p.Scopes = nil, nil },
		"extra": func(p *BeginOperationProposal) {
			p.Plan.Blocks = append(p.Plan.Blocks, topology.PlanBlock{GroupID: "extra"})
			p.Scopes = append(p.Scopes, ScopeExpectation{GroupID: "extra"})
		},
		"duplicate": func(p *BeginOperationProposal) {
			p.Plan.Blocks = append(p.Plan.Blocks, p.Plan.Blocks[0])
			p.Scopes = append(p.Scopes, p.Scopes[0])
		},
		"stale source": func(p *BeginOperationProposal) { p.Scopes[0].SourceVersion = 0; p.Plan.Blocks[0].SourceVersion = 0 },
		"stale owner":  func(p *BeginOperationProposal) { p.Scopes[0].Owner = "" },
		"wrong plan":   func(p *BeginOperationProposal) { p.Plan.Version++ },
		"clock":        func(p *BeginOperationProposal) { p.At = p.At.Add(-2 * time.Second) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s, p := approvalFixture(t, "shard0")
			before, _ := json.Marshal(s)
			mutate(&p)
			if _, err := prepareScopedApproval(&s, s.Latest, p, explicitApproval); err == nil {
				t.Fatal("forged capture accepted")
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("refusal changed the graph")
			}
		})
	}
	s, p := approvalFixture(t, "shard0")
	if _, err := prepareScopedApproval(&s, s.Latest, p, automaticApproval); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("repair seized an owned block", err)
	}
}

func TestScopedApprovalDispatchInstallsOnceAfterCompletePreparation(t *testing.T) {
	t.Parallel()
	s, p := approvalFixture(t, "shard0")
	body, _ := json.Marshal(p)
	application, err := applyScopedProposal(&s, 11, "begin_operation", body)
	if err != nil || !application.changed || !application.result.Accepted ||
		application.result.OperatorID != "exec-v3-a1" || s.AppliedIndex != 11 || s.Target.Version != 3 {
		t.Fatal("complete candidate was not installed", application, err)
	}
	before, _ := json.Marshal(s)
	application, err = applyScopedProposal(&s, 12, "begin_operation", body)
	if err != nil || application.changed || !application.result.Accepted || application.result.OperatorID != "exec-v3-a1" {
		t.Fatal("approval replay created another operation", application, err)
	}
	p.Version = 1 // No retained original approval exists for the founding checkpoint.
	body, _ = json.Marshal(p)
	application, err = applyScopedProposal(&s, 13, "begin_operation", body)
	if !errors.Is(err, fault.ErrHistoryUnavailable) || application.changed {
		t.Fatal("stale Latest was not refused precisely", application, err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("replay/refusal changed durable state")
	}
}
