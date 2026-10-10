package convergence

import "time"

func clusterDoc(lo, hi int, nodeNames []string, shardIDs []string) Document {
	doc := Document{
		Version:   1,
		ClusterID: "test",
		Placement: PlacementRule{MinReplicas: lo, MaxReplicas: hi},
		ChangedBy: "seed",
		CreatedAt: time.Now().UTC(),
	}
	// The _meta control-plane Group carries node member IDs (1..n).
	meta := Group{ID: MetaGroupID, NextReplicaID: uint64(len(nodeNames) + 1)}
	for i, name := range nodeNames {
		doc.Nodes = append(doc.Nodes, Node{Name: name, Type: "worker", State: NodeActive})
		meta.Replicas = append(meta.Replicas, Replica{
			NodeName:  name,
			ReplicaID: uint64(i + 1),
			Role:      ReplicaVoter,
		})
	}
	doc.Groups = append(doc.Groups, meta)
	for _, id := range shardIDs {
		shard := Group{ID: id, NextReplicaID: uint64(len(nodeNames) + 1)}
		for i, name := range nodeNames {
			shard.Replicas = append(shard.Replicas, Replica{
				NodeName:  name,
				ReplicaID: uint64(i + 1),
				Role:      ReplicaVoter,
			})
		}
		doc.Groups = append(doc.Groups, shard)
	}
	return doc
}

func threeNodeDoc() Document {
	return clusterDoc(1, 3, []string{"n1", "n2", "n3"}, []string{"shard0", "shard1"})
}
func removeMetaReplicaOf(doc *Document, name string) { removeFromGroup(doc.MetaGroup(), name) }
func removeReplicasOf(doc *Document, name string) {
	for i := range doc.Groups {
		if doc.Groups[i].ID != MetaGroupID {
			removeFromGroup(&doc.Groups[i], name)
		}
	}
}
func removeFromGroup(group *Group, name string) {
	if group == nil {
		return
	}
	kept := group.Replicas[:0]
	for _, r := range group.Replicas {
		if r.NodeName != name {
			kept = append(kept, r)
		}
	}
	group.Replicas = kept
}
