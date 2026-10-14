//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestCurrentPlacementSnapshotRetainsReplicatedIntentAndReservation(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
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
	sm.latest, sm.target, sm.active = base.Clone(), base.Clone(), base.Clone()
	sm.history = []topology.Document{base.Clone()}
	sm.rememberAllocations(base)

	data, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := newTopologySM(nil, testRules())
	if err := restored.Restore(data); err != nil {
		t.Fatalf("restore: %v (cause: %v)", err, errors.Unwrap(err))
	}
	for _, got := range []topology.Document{restored.latest, restored.target, restored.active} {
		if !reflect.DeepEqual(got, base) {
			t.Fatalf("snapshot changed exact placement intent: got %+v, want %+v", got, base)
		}
	}
	if restored.allocations["shard0"] != 2 {
		t.Fatal("snapshot lost replica identity high-water mark", restored.allocations)
	}

	// An unsupported state version must not partially replace live evidence.
	var obsolete smState
	if err := json.Unmarshal(data, &obsolete); err != nil {
		t.Fatal(err)
	}
	obsolete.SchemaVersion = 2
	damaged, err := json.Marshal(obsolete)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(damaged); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("obsolete snapshot was not classified unsupported", err)
	}
	if !reflect.DeepEqual(restored.active, base) || restored.allocations["shard0"] != 2 {
		t.Fatal("rejected snapshot rewrote approved placement or identity")
	}
}
