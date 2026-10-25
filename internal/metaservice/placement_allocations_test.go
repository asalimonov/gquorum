//go:build !functional

package metaservice

import (
	"math"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func TestPreparedNewGroupsHonorHistoricalAllocationFloors(t *testing.T) {
	t.Parallel()
	before := topology.Document{Groups: []topology.Group{{ID: "existing", NextReplicaID: 4}}}
	prepared := before.Clone()
	prepared.Groups = append(prepared.Groups, topology.Group{ID: "staged-only", NextReplicaID: 3,
		Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1}, {NodeName: "n2", ReplicaID: 2}}})
	existing := *prepared.Group("existing")
	if err := raisePreparedGroupFloors(before, &prepared, map[string]uint64{"existing": 100, "staged-only": 50}); err != nil {
		t.Fatal(err)
	}
	group := prepared.Group("staged-only")
	if group.Replicas[0].ReplicaID != 50 || group.Replicas[1].ReplicaID != 51 || group.NextReplicaID != 52 ||
		!reflect.DeepEqual(existing, *prepared.Group("existing")) {
		t.Fatal("floor reused IDs or rewrote existing identity", prepared)
	}
	if err := raisePreparedGroupFloors(before, &prepared, map[string]uint64{"staged-only": math.MaxUint64}); err == nil {
		t.Fatal("allocation overflow accepted")
	}
}
