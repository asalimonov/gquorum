//go:build !functional

package metaservice

import (
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestPlanNativeCandidatesIncludeExactRetainedOwners(t *testing.T) {
	t.Parallel()
	proven := topology.Group{ID: "work", Replicas: []topology.Replica{{NodeName: "a", ReplicaID: 1}}}
	input := topology.PlanInput{Groups: map[string]topology.GroupProgress{"work": {Converged: &proven}},
		Retained: map[string]topology.RetainedGroup{"work": {Replicas: []topology.RetainedReplica{
			{ReplicaKey: topology.ReplicaKey{NodeName: "b", ReplicaID: 2}, Progress: topology.RetainedHosted},
			{ReplicaKey: topology.ReplicaKey{NodeName: "b", ReplicaID: 3}, Progress: topology.RetainedStopped},
		}}}}
	target := topology.Document{ClusterID: "cluster", Groups: []topology.Group{{ID: "work", Replicas: []topology.Replica{
		{NodeName: "a", ReplicaID: 1}, {NodeName: "b", ReplicaID: 4},
	}}}}
	want := []storage.Identity{
		{ClusterID: "cluster", GroupID: "work", NodeName: "a", ReplicaID: 1},
		{ClusterID: "cluster", GroupID: "work", NodeName: "b", ReplicaID: 4},
		{ClusterID: "cluster", GroupID: "work", NodeName: "b", ReplicaID: 2},
		{ClusterID: "cluster", GroupID: "work", NodeName: "b", ReplicaID: 3},
	}
	if got := planNativeCandidates(input, target, "work"); !slices.Equal(got, want) {
		t.Fatal("lost an exact candidate or duplicated convergence", got)
	}
}
