//go:build !functional

package metaengine

import (
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestBuildReplicaRepairSameMutationForBothOrigins(t *testing.T) {
	t.Parallel()
	active := baseDoc()
	active.Version = 1
	at := time.Now().UTC()
	auto, id, err := ReplicaRepairDocument(active, active, "shard0", "n1", 1, at, topology.OriginReconciler, "reconciler")
	if err != nil {
		t.Fatal(err)
	}
	operator, other, err := ReplicaRepairDocument(active, active, "shard0", "n1", 1, at, topology.OriginOperator, "operator")
	if err != nil {
		t.Fatal(err)
	}
	if id != other || id != 2 {
		t.Fatal("authorities allocated different identity", id, other)
	}
	auto.Origin, auto.ChangedBy = operator.Origin, operator.ChangedBy
	if !reflect.DeepEqual(auto, operator) {
		t.Fatal("repair differs beyond authority")
	}
	if active.Group("shard0").Replicas[0].ReplicaID != 1 {
		t.Fatal("repair modified Active")
	}
	for _, tuple := range []struct {
		group, node string
		id          uint64
	}{{topology.MetaGroupID, "n1", 1}, {"missing", "n1", 1}, {"shard0", "other", 1}, {"shard0", "n1", 99}} {
		if _, _, err = ReplicaRepairDocument(active, active, tuple.group, tuple.node, tuple.id, at, topology.OriginOperator, "operator"); err == nil {
			t.Fatal("inexact repair accepted", tuple)
		}
	}
}
