package metaengine

import (
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

// baseDoc returns a minimal structurally valid document (one active node, one
// shard with a single voter). The state machine assigns the version, so the
// Version field here is advisory.
func baseDoc() topology.Document {
	return topology.Document{
		ClusterID: "cluster-abc",
		Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3},
		Nodes: []topology.Node{
			{Name: "n1", Type: "worker", State: topology.NodeActive},
		},
		Groups: []topology.Group{
			{ID: topology.MetaGroupID, NextReplicaID: 2, Replicas: []topology.Replica{
				{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter},
			}},
			{ID: "shard0", NextReplicaID: 2, Replicas: []topology.Replica{
				{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter},
			}},
		},
		ChangedBy: "n1",
	}
}

// mustEncode encodes a proposal or fails the test.
func mustEncode(t *testing.T, p any) []byte {
	t.Helper()
	data, err := Encode(p)
	if err != nil {
		t.Fatalf("Encode(%T): %v", p, err)
	}
	return data
}

// apply applies an encoded proposal at the next index (term 1) or fails.
func apply(t *testing.T, sm *topologySM, index uint64, p any) ApplyResult {
	t.Helper()
	res, err := sm.Apply(index, 1, mustEncode(t, p))
	if err != nil {
		t.Fatalf("Apply(%T): %v", p, err)
	}
	return res
}

func testPlan(version uint64, count int) topology.Plan {
	plan := topology.Plan{Version: version}
	for seq := 1; seq <= count; seq++ {
		plan.Steps = append(plan.Steps, topology.Step{
			Seq: seq, GroupID: "shard0", Kind: topology.StepTransferLeader,
			NodeName: "n1", ReplicaID: 1,
		})
	}
	return plan
}

func beginProposal(version uint64, count int, id string, at time.Time) BeginOperatorProposal {
	return BeginOperatorProposal{
		Version: version, Plan: testPlan(version, count), Actor: "n1",
		At: at, ProposalID: id,
	}
}

func testRules() topology.Rules {
	return topology.Rules{SchemaVersion: 1, Limits: topology.Limits{}.WithDefaults()}
}
