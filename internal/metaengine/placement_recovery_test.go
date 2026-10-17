//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestCurrentPlacementSnapshotRetainsReplicatedIntentAndReservation(t *testing.T) {
	t.Parallel()
	sm := mustMachine(t, testRules(), nil)
	base := baseDoc()
	base.Version = 1
	base.Node("n1").Labels = map[string]string{
		"az": "one", "network": "public_ip", topology.LabelReserved: "true",
	}
	group := base.Group("shard0")
	group.Labels = map[string]string{"tenant": "paid"}
	group.Placement = &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3,
		NodeSelector: map[string]string{"network": "public_ip"}}
	voterConfig := topology.DefaultVoterConfig()
	voterConfig.TargetVoters = 1
	voterConfig.DistinctVoterLabel = "az"
	group.VoterConfig = &voterConfig
	group.Hosting = &topology.HostingRule{Mode: topology.HostingExplicit, Nodes: []string{"n1"}}
	at := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	state := scopedState{SchemaVersion: scopedStateSchemaVersion, Rules: testRules(),
		Latest: base.Clone(), Target: base.Clone(), History: []topology.Document{base.Clone()},
		FullyConverged: topology.FullyConverged{Version: 1, At: at}}
	for i := range base.Groups {
		g := &base.Groups[i]
		state.Groups = append(state.Groups, groupConvergence{GroupID: g.ID, Declaration: g, SourceVersion: 1, CompletedAt: at})
		state.Allocations = append(state.Allocations, allocationRecord{GroupID: g.ID, NextReplicaID: g.NextReplicaID})
	}
	state.Nodes = []nodeConvergence{{NodeName: "n1", Declaration: base.Node("n1"), SourceVersion: 1, MemberID: 1, CompletedAt: at}}
	sm = restoreMachineFixture(t, state)

	data, err := sm.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restored := mustMachine(t, testRules(), nil)
	if err := restored.Restore(t.Context(), data); err != nil {
		t.Fatalf("restore: %v (cause: %v)", err, errors.Unwrap(err))
	}
	for _, got := range []topology.Document{restored.Latest(), restored.Target()} {
		if !reflect.DeepEqual(got, base) {
			t.Fatalf("snapshot changed exact placement intent: got %+v, want %+v", got, base)
		}
	}
	if restored.scoped.Allocations[1].NextReplicaID != 2 {
		t.Fatal("snapshot lost replica identity high-water mark", restored.scoped.Allocations)
	}

	// An unsupported state version must not partially replace live evidence.
	var obsolete map[string]json.RawMessage
	if err := json.Unmarshal(data, &obsolete); err != nil {
		t.Fatal(err)
	}
	obsolete["schema_version"] = json.RawMessage(`2`)
	damaged, err := json.Marshal(obsolete)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(t.Context(), damaged); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("obsolete snapshot was not classified unsupported", err)
	}
	if !reflect.DeepEqual(restored.Target(), base) || restored.scoped.Allocations[1].NextReplicaID != 2 {
		t.Fatal("rejected snapshot rewrote approved placement or identity")
	}
}
