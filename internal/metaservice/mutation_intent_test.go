//go:build !functional

package metaservice

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

func intentForTest(t *testing.T, kind string, input mutationInput) metaengine.Intent {
	t.Helper()
	g := &Group{rules: topology.Rules{SchemaVersion: 1, Limits: topology.Limits{MaxNodes: 5, MaxReplicas: 5}}}
	result, err := g.mutationIntent(MutationOptions{Actor: "operator", Comment: "purpose"}, mutationPolicy{Kind: kind, Input: input})
	if err != nil {
		t.Fatal(err)
	}
	return *result
}

func TestMutationIntentCanonicalSetsAndDefaults(t *testing.T) {
	t.Parallel()
	p := topology.DefaultVoterConfig()
	for name, pair := range map[string]struct {
		kind string
		a, b any
	}{
		"placement":             {"set_placement", topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker", "coordinator"}, NodeSelector: map[string]string{"az": "a", "tier": "hot"}}, topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"coordinator", "worker"}, NodeSelector: map[string]string{"tier": "hot", "az": "a"}}},
		"hosting":               {"set_group_hosting", topology.HostingRule{Mode: topology.HostingExplicit, Nodes: []string{"n2", "n1"}}, topology.HostingRule{Mode: topology.HostingExplicit, Nodes: []string{"n1", "n2"}}},
		"default group":         {"create_group", GroupSpec{ID: "work"}, GroupSpec{ID: "work", VoterConfig: &p, Hosting: &topology.HostingRule{Mode: topology.HostingAllEligible}}},
		"group hosts":           {"create_group", GroupSpec{ID: "work", Nodes: []string{"n2", "n1"}}, GroupSpec{ID: "work", Hosting: &topology.HostingRule{Mode: topology.HostingExplicit, Nodes: []string{"n1", "n2"}}}},
		"generated node fields": {"add_node", topology.Node{Name: "n1", Type: "worker"}, topology.Node{Name: "n1", Type: "worker", State: topology.NodeExcluded, CreatedAt: time.Now(), UpdatedAt: time.Now()}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			a := intentForTest(t, pair.kind, mutationInput{Value: pair.a})
			b := intentForTest(t, pair.kind, mutationInput{Value: pair.b})
			if a.Digest != b.Digest {
				t.Fatal("equivalent arguments differ", a, b)
			}
		})
	}
}

func TestMutationIntentIncludesPurposeAndExactIdentities(t *testing.T) {
	t.Parallel()
	g := &Group{rules: topology.Rules{Limits: topology.Limits{MaxNodes: 5, MaxReplicas: 5}}, identity: topology.Identity{NodeName: "operator"}}
	policy := mutationPolicy{Kind: "force_retire_node", Comment: "purpose", Input: mutationInput{NodeName: "n1", Value: topology.ChangeAudit{Operation: topology.AuditOperationForceRetireNode, TargetNode: "n1", Forced: true, Reason: "lost", StagedBy: "operator"}}}
	opts := MutationOptions{Actor: "operator", Comment: "purpose"}
	original, err := g.mutationIntent(opts, policy)
	if err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*MutationOptions, *mutationPolicy){
		"actor":   func(o *MutationOptions, _ *mutationPolicy) { o.Actor = "different" },
		"comment": func(o *MutationOptions, _ *mutationPolicy) { o.Comment = "different" },
		"reason": func(_ *MutationOptions, p *mutationPolicy) {
			a := p.Input.Value.(topology.ChangeAudit)
			a.Reason = "different"
			p.Input.Value = a
		},
		"forced": func(_ *MutationOptions, p *mutationPolicy) {
			a := p.Input.Value.(topology.ChangeAudit)
			a.Forced = false
			p.Input.Value = a
		},
		"predecessor": func(_ *MutationOptions, p *mutationPolicy) { p.Input.PreviousID = 44 },
		"node":        func(_ *MutationOptions, p *mutationPolicy) { p.Input.NodeName = "n2" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			o, p := opts, policy
			edit(&o, &p)
			got, err := g.mutationIntent(o, p)
			if err != nil || got.Digest == original.Digest {
				t.Fatal("distinct purpose matched", got, err)
			}
		})
	}
	defaults, err := g.mutationIntent(MutationOptions{}, policy)
	if err != nil || defaults.Digest != original.Digest {
		t.Fatal(defaults, err)
	}
	audit := policy.Input.Value.(topology.ChangeAudit)
	audit.StagedAt = time.Now()
	policy.Input.Value = audit
	generated, err := g.mutationIntent(opts, policy)
	if err != nil || generated.Digest != original.Digest {
		t.Fatal("generated audit time participates", generated, err)
	}
	a := intentForTest(t, "set_replica_pinned_learner", mutationInput{GroupID: "work", Value: topology.LearnerPin{Replica: topology.ReplicaKey{NodeName: "n1", ReplicaID: 1}, Pinned: true}})
	b := intentForTest(t, "set_replica_pinned_learner", mutationInput{GroupID: "work", Value: topology.LearnerPin{Replica: topology.ReplicaKey{NodeName: "n1", ReplicaID: 2}, Pinned: true}})
	if a.Digest == b.Digest {
		t.Fatal("explicit replica ID excluded")
	}
	if intentForTest(t, "rollback_to", mutationInput{Version: 1}).Digest == intentForTest(t, "rollback_to", mutationInput{Version: 2}).Digest {
		t.Fatal("requested rollback version excluded")
	}
}

func TestMutationIntentValidatesAndOwnsInputs(t *testing.T) {
	t.Parallel()
	g := &Group{rules: topology.Rules{Limits: topology.Limits{MaxNodes: 5, MaxReplicas: 5}}}
	for name, value := range map[string]any{
		"empty name":            topology.Node{Type: "worker"},
		"bad role":              topology.ReplicaRoleChange{NodeName: "n1", Role: "bad"},
		"duplicate hosts":       topology.HostingRule{Mode: topology.HostingExplicit, Nodes: []string{"n1", "n1"}},
		"empty identity":        topology.LearnerPin{Replica: topology.ReplicaKey{NodeName: "n1"}},
		"duplicate types":       topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker", "worker"}},
		"missing audit purpose": topology.ChangeAudit{StagedBy: "operator"},
		"unsupported type":      42,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := g.mutationIntent(MutationOptions{Actor: "operator"}, mutationPolicy{Kind: "add_node", Input: mutationInput{Value: value}})
			if !errors.Is(err, fault.ErrInvalid) {
				t.Fatal(err)
			}
		})
	}
	rule := topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"worker", "coordinator"}, NodeSelector: map[string]string{"az": "a"}}
	owned, err := normalizeMutationInput(mutationInput{Value: rule}, g.rules)
	if err != nil {
		t.Fatal(err)
	}
	normalized := owned.Value.(topology.PlacementRule)
	normalized.AllowedNodeTypes[0] = "changed"
	normalized.NodeSelector["az"] = "changed"
	if rule.AllowedNodeTypes[0] != "worker" || rule.NodeSelector["az"] != "a" {
		t.Fatal("caller input mutated")
	}
	result := metaengine.ApplyResult{Doc: topology.Document{Version: 2}, OperationID: topology.OperationID(topology.MakeOperationID(2, 1)), RebasedTo: 4}
	replay := mutationReplayResult(result)
	if replay.OperationID != result.OperationID || replay.RebasedTo != 4 || !reflect.DeepEqual(replay.Document, result.Doc) {
		t.Fatal("lost historical result", replay)
	}
}
