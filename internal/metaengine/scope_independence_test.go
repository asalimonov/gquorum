//go:build !functional

package metaengine

import (
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func independenceFixture() topology.Document {
	base := baseDoc()
	base.Placement.AllowedNodeTypes = []topology.NodeType{"worker"}
	for i, name := range []string{"n2", "n3"} {
		base.Nodes = append(base.Nodes, topology.Node{Name: name, Type: "worker", State: topology.NodeActive})
		base.MetaGroup().Replicas = append(base.MetaGroup().Replicas,
			topology.Replica{NodeName: name, ReplicaID: uint64(i + 2), Role: topology.ReplicaLearner})
	}
	base.MetaGroup().NextReplicaID = 4
	base.Groups = append(base.Groups, topology.Group{ID: "b", NextReplicaID: 2,
		Replicas: []topology.Replica{{NodeName: "n2", ReplicaID: 1, Role: topology.ReplicaVoter}}})
	for i := range base.Nodes {
		base.Nodes[i].Labels = map[string]string{"tier": "hot", "zone": base.Nodes[i].Name}
	}
	return base
}

func TestRepairIndependenceUsesSemanticCandidateReads(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		stage    func(*topology.Document)
		conflict bool
	}{
		"other group":                {func(d *topology.Document) { d.Group("shard0").Labels = map[string]string{"edit": "yes"} }, false},
		"other allocation":           {func(d *topology.Document) { d.Group("shard0").NextReplicaID++ }, false},
		"same allocation":            {func(d *topology.Document) { d.Group("b").NextReplicaID++ }, true},
		"same group label":           {func(d *topology.Document) { d.Group("b").Labels = map[string]string{"edit": "yes"} }, true},
		"candidate region":           {func(d *topology.Document) { d.Node("n3").Region = "west" }, false},
		"candidate unrelated label":  {func(d *topology.Document) { d.Node("n3").Labels["description"] = "anything" }, false},
		"candidate selector":         {func(d *topology.Document) { d.Node("n3").Labels["tier"] = "cold" }, true},
		"candidate type":             {func(d *topology.Document) { d.Node("n3").Type = "metadata" }, true},
		"candidate domain":           {func(d *topology.Document) { d.Node("n3").Labels["zone"] = "new" }, true},
		"candidate member":           {func(d *topology.Document) { d.MetaGroup().Replicas[2].ReplicaID = 4; d.MetaGroup().NextReplicaID = 5 }, true},
		"missing candidate identity": {func(d *topology.Document) { d.MetaGroup().Replicas = d.MetaGroup().Replicas[:2] }, true},
		"assigned identity":          {func(d *topology.Document) { d.MetaGroup().Replicas[1].ReplicaID = 4; d.MetaGroup().NextReplicaID = 5 }, true},
		"global placement":           {func(d *topology.Document) { d.Placement.MaxReplicas = 2 }, true},
		"foreign cluster":            {func(d *topology.Document) { d.ClusterID = "other" }, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			base := independenceFixture()
			base.Placement.NodeSelector = map[string]string{"tier": "hot"}
			config := topology.DefaultVoterConfig()
			config.DistinctVoterLabel = "zone"
			base.Group("b").VoterConfig = &config
			repair, staged := base.Clone(), base.Clone()
			repair.Group("b").Replicas[0].ReplicaID, repair.Group("b").NextReplicaID = 2, 3
			tc.stage(&staged)
			before := []topology.Document{base.Clone(), staged.Clone(), repair.Clone()}
			err := ValidateRepairIndependence(base, staged, repair)
			if (err != nil) != tc.conflict || err != nil && !errors.Is(err, fault.ErrVersionConflict) {
				t.Fatal("wrong independence decision", err)
			}
			if !reflect.DeepEqual(before, []topology.Document{base, staged, repair}) {
				t.Fatal("dependency evaluation mutated declarations")
			}
		})
	}
}

func TestRepairIndependenceUsesProposedRulesAndIgnoresIneligibleCandidates(t *testing.T) {
	t.Parallel()
	base := independenceFixture()
	repair, staged := base.Clone(), base.Clone()
	// A newly adopted selector reads a previously irrelevant label.
	rule := base.Placement.Clone()
	rule.NodeSelector = map[string]string{"tier": "hot"}
	repair.Group("b").Placement = &rule
	staged.Node("n3").Labels["tier"] = "cold"
	if err := ValidateRepairIndependence(base, staged, repair); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal("new selector dependency was ignored", err)
	}
	// A group override makes the default rule irrelevant to that repair.
	base.Group("b").Placement = &rule
	repair, staged = base.Clone(), base.Clone()
	repair.Group("b").Replicas[0].ReplicaID, repair.Group("b").NextReplicaID = 2, 3
	staged.Placement.MaxReplicas = 2
	if err := ValidateRepairIndependence(base, staged, repair); err != nil {
		t.Fatal("irrelevant default rule blocked repair", err)
	}
	base.Node("n3").Type = "metadata"
	repair, staged = base.Clone(), base.Clone()
	repair.Group("b").Replicas[0].ReplicaID, repair.Group("b").NextReplicaID = 2, 3
	staged.Node("n3").Labels["tier"] = "cold"
	staged.MetaGroup().Replicas[2].ReplicaID, staged.MetaGroup().NextReplicaID = 4, 5
	if err := ValidateRepairIndependence(base, staged, repair); err != nil {
		t.Fatal("unassigned ineligible node became a repair dependency", err)
	}
}

func TestScopedRebaseRejectsRetainedOverlappingEdits(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	s.History[3].Group("live").Labels = map[string]string{"staged": "yes"}
	s.Target.Group("live").Labels = map[string]string{"repair": "yes"}
	index, err := s.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateScopedRebase(&s, index, s.Latest); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("overlapping retained provenance was accepted", err)
	}
}
