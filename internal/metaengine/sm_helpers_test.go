package metaengine

import (
	"encoding/json"
	"testing"

	"github.com/asalimonov/gquorum/internal/raftengine"
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

func testRules() topology.Rules {
	return topology.Rules{SchemaVersion: 1, Limits: topology.Limits{MaxNodes: 4, MaxReplicas: 5}}
}

func mustMachine(t testing.TB, rules topology.Rules, persister statePersister) *Machine {
	t.Helper()
	m, err := NewMachine(rules, persister)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func restoreMachineFixture(t testing.TB, state scopedState) *Machine {
	t.Helper()
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	m := mustMachine(t, state.Rules, nil)
	if err := m.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	return m
}

// applyMachine exercises the same decode, refusal and apply boundary as Raft.
func applyMachine(t *testing.T, m *Machine, proposal any) ApplyResult {
	t.Helper()
	index, term := m.AppliedState()
	result, err := m.Apply(t.Context(), raftengine.Command{Index: index + 1, Term: max(term, 1), Data: mustEncode(t, proposal)})
	if err != nil {
		t.Fatalf("Apply(%T): %v", proposal, err)
	}
	var applied ApplyResult
	if err := json.Unmarshal(result.Data, &applied); err != nil {
		t.Fatal(err)
	}
	return applied
}

func multiNodeDoc() topology.Document {
	return topology.Document{
		ClusterID: "cluster-meta",
		Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3},
		Nodes: []topology.Node{
			{Name: "node1", Type: "worker", State: topology.NodeActive},
			{Name: "node2", Type: "worker", State: topology.NodeActive},
			{Name: "node3", Type: "worker", State: topology.NodeActive},
		},
		Groups: []topology.Group{
			// The _meta group carries every declared node's member ID; the running
			// raft membership (singleNodeConfig) is only node1 — the two are distinct.
			{ID: topology.MetaGroupID, NextReplicaID: 6, Replicas: []topology.Replica{
				{NodeName: "node1", ReplicaID: 1, Role: topology.ReplicaVoter},
				{NodeName: "node2", ReplicaID: 5, Role: topology.ReplicaVoter},
				{NodeName: "node3", ReplicaID: 3, Role: topology.ReplicaVoter},
			}},
			{ID: "shard0", NextReplicaID: 2, Replicas: []topology.Replica{
				{NodeName: "node1", ReplicaID: 1, Role: topology.ReplicaVoter},
			}},
		},
		ChangedBy: "test",
	}
}
