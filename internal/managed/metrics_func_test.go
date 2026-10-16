//go:build functional

package managed

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestMetricsReadsUseOnlyProvenExactAssignments(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	var resolved []load.ReplicaRef
	f.cfg.ResolvePeer = func(_ context.Context, ref load.ReplicaRef) (load.Peer, error) {
		resolved = append(resolved, ref)
		// A name resolver returning the successor cannot satisfy the old replica.
		return load.Peer{Node: topology.Identity{ClusterID: "cluster", NodeName: "b", MemberID: 22, InstanceID: "successor"},
			Replica: load.ReplicaRef{GroupID: ref.GroupID, ReplicaID: 33}}, nil
	}
	h := f.host(t)
	old := storage.Identity{ClusterID: "cluster", GroupID: "work", NodeName: "b", ReplicaID: 2}
	h.metricsPeers = map[storage.Identity]load.Peer{old: {Node: topology.Identity{ClusterID: "cluster", NodeName: "b", MemberID: 2, InstanceID: "old"}, Replica: load.ReplicaRef{GroupID: "work", ReplicaID: 2}}}
	proven := topology.Group{ID: "work", Replicas: []topology.Replica{{NodeName: "b", ReplicaID: 2}}}
	approved := topology.Group{ID: "work", Replicas: []topology.Replica{{NodeName: "b", ReplicaID: 33}}}
	reads := h.MetricsReads(t.Context(), []topology.GroupProgress{
		{GroupID: "work", ConvergedVersion: 7, Converged: &proven, Approved: &approved},
		{GroupID: "new", Approved: &approved},
		{GroupID: "unproven", Converged: &approved},
		{GroupID: topology.MetaGroupID, ConvergedVersion: 8, Converged: &approved},
	})
	if len(reads) != 1 || len(resolved) != 1 || resolved[0] != (load.ReplicaRef{GroupID: "work", ReplicaID: 2}) ||
		!errors.Is(reads[0].Error, fault.ErrIdentityMismatch) || reads[0].Read != nil || reads[0].Identity.InstanceID != "" ||
		reads[0].Identity.ReplicaID != 2 || len(h.metricsPeers) != 0 {
		t.Fatal("approved/unproven or successor identity escaped capture", reads, resolved, h.metricsPeers)
	}
}
