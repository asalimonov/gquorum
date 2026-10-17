//go:build !functional

package topology

import (
	"slices"
	"testing"
)

func TestAffectedGroupsAdmissionDoesNotOwnUnassignedLoads(t *testing.T) {
	t.Parallel()
	for _, nodeType := range []NodeType{"worker", "metadata"} {
		t.Run(string(nodeType), func(t *testing.T) {
			t.Parallel()
			before := scopeDocument()
			after := before.Clone()
			after.Nodes = append(after.Nodes, Node{Name: "n4", Type: nodeType, State: NodeActive,
				Labels: map[string]string{"tier": "hot"}})
			after.MetaGroup().Replicas = append(after.MetaGroup().Replicas, learner("n4", 4))
			after.MetaGroup().NextReplicaID = 5
			if got := AffectedGroups(before, after); !slices.Equal(got, []string{MetaGroupID}) {
				t.Fatal("new candidate captured unrelated load ownership", got)
			}
			if got := AffectedGroups(after, before); !slices.Equal(got, []string{MetaGroupID}) {
				t.Fatal("unassigned node removal captured unrelated load ownership", got)
			}
			if nodeType != "worker" {
				return
			}
			// Materialized hosting still brings its changed group into the operation.
			after.Group("a").Replicas = append(after.Group("a").Replicas, learner("n4", 2))
			after.Group("a").NextReplicaID = 3
			if got := AffectedGroups(before, after); !slices.Equal(got, []string{MetaGroupID, "a"}) {
				t.Fatal("actual new assignment lost its admission scope", got)
			}
		})
	}
}

func TestAffectedGroupsUnassignedChangesRemainReadDependencies(t *testing.T) {
	t.Parallel()
	for _, edit := range []struct {
		name  string
		apply func(*Node)
	}{
		{"selector", func(n *Node) { n.Labels["tier"] = "cold" }},
		{"type", func(n *Node) { n.Type = "metadata" }},
		{"drain", func(n *Node) { n.State = NodeDraining }},
		{"reservation", func(n *Node) { n.Labels[LabelReserved] = "true" }},
		{"domain", func(n *Node) { n.Labels["zone"] = "other" }},
	} {
		t.Run(edit.name, func(t *testing.T) {
			t.Parallel()
			before := scopeDocument()
			config := DefaultVoterConfig()
			config.DistinctVoterLabel = "zone"
			before.Group("a").VoterConfig = &config
			after := before.Clone()
			edit.apply(after.Node("n3"))
			if got := AffectedGroups(before, after); !slices.Equal(got, []string{MetaGroupID}) {
				t.Fatal("candidate read became operation ownership", got)
			}
		})
	}
}
