//go:build !functional

package topology

import (
	"encoding/json"
	"slices"
	"testing"
)

func scopeDocument() Document {
	doc := planDoc(1, 3, []string{"n1", "n2", "n3"},
		shardOf("a", rep("n1", 1)), shardOf("b", rep("n2", 1)))
	doc.Placement.AllowedNodeTypes = []NodeType{"worker"}
	for i := range doc.Nodes {
		doc.Nodes[i].Labels = map[string]string{"tier": "hot"}
	}
	override := doc.Placement.Clone()
	override.NodeSelector = map[string]string{"tier": "hot"}
	doc.Group("b").Placement = &override
	return doc
}

func TestAffectedGroupsDerivesDeclarationsAndDependencies(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		edit func(*Document)
		want []string
	}{
		{"unchanged", func(*Document) {}, nil},
		{"group labels", func(d *Document) { d.Group("a").Labels = map[string]string{"owner": "jobs"} }, []string{"a"}},
		{"allocation", func(d *Document) { d.Group("a").NextReplicaID++ }, []string{"a"}},
		{"replica role", func(d *Document) { d.Group("a").Replicas[0].Role = ReplicaLearner }, []string{"a"}},
		{"deletion", func(d *Document) { d.Groups = slices.Delete(d.Groups, 1, 2) }, []string{"a"}},
		{"new group", func(d *Document) { d.Groups = append(d.Groups, shardOf("c", rep("n3", 1))) }, []string{"c"}},
		{"cluster rule", func(d *Document) { d.Placement.MaxReplicas++ }, []string{"a"}},
		{"group rule", func(d *Document) { d.Group("b").Placement.MaxReplicas++ }, []string{"b"}},
		{"region", func(d *Document) { d.Node("n1").Region = "elsewhere" }, []string{MetaGroupID}},
		{"unused label", func(d *Document) { d.Node("n1").Labels["owner"] = "jobs" }, []string{MetaGroupID}},
		{"selector", func(d *Document) { d.Node("n2").Labels["tier"] = "cold" }, []string{MetaGroupID, "b"}},
		{"type", func(d *Document) { d.Node("n1").Type = "other" }, []string{MetaGroupID, "a"}},
		{"drain", func(d *Document) { d.Node("n1").State = NodeDraining }, []string{MetaGroupID, "a"}},
		{"reservation", func(d *Document) { d.Node("n1").Labels[LabelReserved] = "true" }, []string{MetaGroupID, "a"}},
		{"member allocation", func(d *Document) { d.MetaGroup().NextReplicaID++ }, []string{MetaGroupID}},
		{"member reseat", func(d *Document) {
			d.MetaGroup().Replicas[2].ReplicaID = 4
			d.MetaGroup().NextReplicaID = 5
		}, []string{MetaGroupID}},
		{"document provenance", func(d *Document) { d.Version++; d.Comment = "audit" }, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before := scopeDocument()
			after := before.Clone()
			tc.edit(&after)
			if got := AffectedGroups(before, after); !slices.Equal(got, tc.want) {
				t.Fatal("wrong declaration/dependency scope", got, tc.want)
			}
		})
	}
}

func TestAffectedGroupsKeepsExactHostAndDomainReads(t *testing.T) {
	t.Parallel()
	before := scopeDocument()
	before.Group("b").Hosting = &HostingRule{Mode: HostingExplicit, Nodes: []string{"n2"}}
	after := before.Clone()
	after.MetaGroup().Replicas[2].ReplicaID = 4
	after.MetaGroup().NextReplicaID = 5
	if got := AffectedGroups(before, after); !slices.Equal(got, []string{MetaGroupID}) {
		t.Fatal("unassigned node became an ownership dependency", got)
	}
	// An assigned host remains an exact dependency even when it is ineligible.
	before.Group("b").Replicas = append(before.Group("b").Replicas, learner("n3", 2))
	after.Group("b").Replicas = slices.Clone(before.Group("b").Replicas)
	before.Group("b").NextReplicaID, after.Group("b").NextReplicaID = 3, 3
	if got := AffectedGroups(before, after); !slices.Equal(got, []string{MetaGroupID, "b"}) {
		t.Fatal("ineligible predecessor lost its exact ownership dependency", got)
	}
	before = scopeDocument()
	config := DefaultVoterConfig()
	config.DistinctVoterLabel = "zone"
	before.Group("a").VoterConfig = &config
	after = before.Clone()
	after.Node("n1").Labels["zone"] = "one"
	if got := AffectedGroups(before, after); !slices.Equal(got, []string{MetaGroupID, "a"}) {
		t.Fatal("domain dependency ignored or spread to unrelated group", got)
	}
}

func TestAffectedGroupsIsDeterministicAndConservative(t *testing.T) {
	t.Parallel()
	before := scopeDocument()
	after := before.Clone()
	after.Group("a").Labels = map[string]string{"owner": "jobs"}
	after.Group("b").NextReplicaID++
	after.Node("n1").Region = "other"
	slices.Reverse(after.Groups)
	slices.Reverse(after.Nodes)
	prior, _ := json.Marshal([]Document{before, after})
	got := AffectedGroups(before, after)
	next, _ := json.Marshal([]Document{before, after})
	if !slices.Equal(got, []string{MetaGroupID, "a", "b"}) || !slices.Equal(prior, next) {
		t.Fatal("scope order or caller ownership changed", got)
	}
	after = before.Clone()
	slices.Reverse(after.Nodes)
	slices.Reverse(after.Groups)
	if got := AffectedGroups(before, after); len(got) != 0 {
		t.Fatal("declaration order alone became a semantic edit", got)
	}
	before.Group("b").Placement.NodeSelector = map[string]string{"bad key": "value"}
	if got := AffectedGroups(before, before); !slices.Equal(got, []string{"b"}) {
		t.Fatal("invalid dependency evaluation was treated as independent", got)
	}
}
