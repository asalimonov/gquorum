package metaengine

import (
	"reflect"
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
		{"group cardinality", func(d *topology.Document) { d.Groups = append(d.Groups, topology.Group{ID: "extra"}) }},
		{"role", func(d *topology.Document) { d.Group("shard0").Replicas[0].Role = topology.ReplicaLearner }},
		{"node state", func(d *topology.Document) { d.Node("n1").State = topology.NodeDraining }},
		{"node labels", func(d *topology.Document) { d.Node("n1").Labels = map[string]string{"rack": "other"} }},
		{"audit", func(d *topology.Document) { d.Audit = &topology.ChangeAudit{Forced: true} }},
		{"comment", func(d *topology.Document) { d.Comment = "unrelated operator change" }},
		{"lineage", func(d *topology.Document) { d.ParentVersion = 99 }},
		{"allocator skip", func(d *topology.Document) {
			d.Group("shard0").Replicas[0].ReplicaID++
			d.Group("shard0").NextReplicaID++
		}},
		{"meta identity", func(d *topology.Document) { d.MetaGroup().Replicas[0].ReplicaID = 2; d.MetaGroup().NextReplicaID = 3 }},
		{"unrelated timestamp", func(d *topology.Document) { d.MetaGroup().UpdatedAt = time.Now() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sm := newTopologySM(nil, topology.Rules{SchemaVersion: 1, Limits: (topology.Limits{}).WithDefaults()})
			active := apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0}).Doc
			at := time.Now().UTC()
			ex := apply(t, sm, 2, BeginOperatorProposal{Version: 1, Plan: topology.Plan{Version: 1}, At: at})
			apply(t, sm, 3, TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: ex.OperatorID, Version: 1, Attempt: 1, State: topology.OperatorSucceeded, At: at}})
			apply(t, sm, 4, ActivateTargetProposal{OperatorID: ex.OperatorID, Version: 1, Attempt: 1})
			repair, _, err := ReplicaRepairDocument(active, active, "shard0", "n1", 1, at, topology.OriginReconciler, "auto-repair")
			if err != nil {
				t.Fatal(err)
			}
			tc.edit(&repair)
			plan := topology.Plan{Version: 2, Steps: []topology.Step{{Seq: 1, Kind: topology.StepPrepareReplica, GroupID: "shard0", NodeName: "n1", ReplicaID: 2}, {Seq: 2, Kind: topology.StepAddLearner, GroupID: "shard0", NodeName: "n1", ReplicaID: 2}, {Seq: 3, Kind: topology.StepWaitCaughtUp, GroupID: "shard0", NodeName: "n1", ReplicaID: 2}, {Seq: 4, Kind: topology.StepPromoteVoter, GroupID: "shard0", NodeName: "n1", ReplicaID: 2}}}
			result := apply(t, sm, 5, CommitRepairOperatorProposal{Doc: repair, ExpectedVersion: 1, Plan: plan, At: at})
			if result.Accepted || !reflect.DeepEqual(active, sm.Latest()) || !reflect.DeepEqual(active, sm.Target()) || !reflect.DeepEqual(active, sm.Active()) {
				t.Fatal("repair edit escaped replicated refusal", result)
			}
		})
	}
}
