//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

// Ports the source's exact repair-candidate edit refusals at the replicated
// boundary, so calling the generic proposal API cannot bypass the builder.
func TestRepairCommandRejectsEditsOutsideOneActiveVoter(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		edit func(*topology.Document)
	}{
		{"placement", func(d *topology.Document) { d.Placement.MaxReplicas++ }},
		{"voter configuration", func(d *topology.Document) { p := topology.DefaultVoterConfig(); d.Group("shard0").VoterConfig = &p }},
		{"hosting", func(d *topology.Document) {
			d.Group("shard0").Hosting = &topology.HostingRule{Mode: topology.HostingAllEligible}
		}},
		{"learner pin", func(d *topology.Document) { d.Group("shard0").Replicas[0].PinnedLearner = true }},
		{"group cardinality", func(d *topology.Document) { d.Groups = append(d.Groups, topology.Group{ID: "extra"}) }},
		{"role", func(d *topology.Document) { d.Group("shard0").Replicas[0].Role = topology.ReplicaLearner }},
		{"node state", func(d *topology.Document) { d.Node("n1").State = topology.NodeDraining }},
		{"node labels", func(d *topology.Document) { d.Node("n1").Labels = map[string]string{"rack": "other"} }},
		{"audit", func(d *topology.Document) { d.Audit = &topology.ChangeAudit{Forced: true} }},
		{"comment", func(d *topology.Document) { d.Comment = "unrelated operator change" }},
		{"allocator skip", func(d *topology.Document) {
			d.Group("shard0").Replicas[0].ReplicaID++
			d.Group("shard0").NextReplicaID++
		}},
		{"meta identity", func(d *topology.Document) { d.MetaGroup().Replicas[0].ReplicaID = 2; d.MetaGroup().NextReplicaID = 3 }},
		{"unrelated timestamp", func(d *topology.Document) { d.MetaGroup().UpdatedAt = time.Now() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, proposal := repairCommitFixture(t)
			m := restoreMachineFixture(t, s)
			tc.edit(&proposal.Doc)
			before, err := m.Snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			result := applyMachine(t, m, proposal)
			var expected scopedState
			if err := json.Unmarshal(before, &expected); err != nil {
				t.Fatal(err)
			}
			expected.AppliedIndex, expected.AppliedTerm = m.AppliedState()
			want, err := json.Marshal(expected)
			if err != nil {
				t.Fatal(err)
			}
			after, err := m.Snapshot(t.Context())
			if err != nil || result.Accepted || !bytes.Equal(want, after) {
				t.Fatal("repair edit escaped replicated refusal", result, err)
			}
		})
	}
}

func TestScopedRepairDerivesLineageFromReplicatedHeads(t *testing.T) {
	t.Parallel()
	s, proposal := repairCommitFixture(t)
	proposal.Doc.Version, proposal.Doc.ParentVersion, proposal.Doc.RebasedFrom = 99, 98, 97
	m := restoreMachineFixture(t, s)
	result := applyMachine(t, m, proposal)
	if !result.Accepted || result.Doc.Version != s.Latest.Version+1 ||
		result.Doc.ParentVersion != s.Target.Version || result.Doc.RebasedFrom != 0 ||
		m.Latest().RebasedFrom != s.Latest.Version {
		t.Fatal("caller selected generated repair ancestry", result)
	}
}
