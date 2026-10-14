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
	options := ReplicaRepairOptions{GroupID: "shard0", NodeName: "n1", ReplicaID: 1,
		At: at, Origin: topology.OriginReconciler, Actor: "reconciler"}
	auto, err := ReplicaRepairDocument(active, active, options)
	if err != nil {
		t.Fatal(err)
	}
	options.Origin, options.Actor = topology.OriginOperator, "operator"
	operator, err := ReplicaRepairDocument(active, active, options)
	if err != nil {
		t.Fatal(err)
	}
	if auto.ReplicaID != operator.ReplicaID || auto.ReplicaID != 2 {
		t.Fatal("authorities allocated different identity", auto.ReplicaID, operator.ReplicaID)
	}
	auto.Doc.Origin, auto.Doc.ChangedBy = operator.Doc.Origin, operator.Doc.ChangedBy
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
		options.GroupID, options.NodeName, options.ReplicaID = tuple.group, tuple.node, tuple.id
		if _, err = ReplicaRepairDocument(active, active, options); err == nil {
			t.Fatal("inexact repair accepted", tuple)
		}
	}
}
