package metaservice

import (
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func planNativeCandidates(input topology.PlanInput, target topology.Document, id string) []storage.Identity {
	var candidates []storage.Identity
	seen := make(map[storage.Identity]bool)
	add := func(node string, replica uint64) {
		identity := storage.Identity{ClusterID: target.ClusterID, GroupID: id, NodeName: node, ReplicaID: replica}
		if !seen[identity] {
			seen[identity] = true
			candidates = append(candidates, identity)
		}
	}
	for _, group := range []*topology.Group{input.Groups[id].Converged, target.Group(id)} {
		if group != nil {
			for _, replica := range group.Replicas {
				add(replica.NodeName, replica.ReplicaID)
			}
		}
	}
	for _, replica := range input.Retained[id].Replicas {
		add(replica.NodeName, replica.ReplicaID)
	}
	return candidates
}
