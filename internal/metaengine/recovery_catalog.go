package metaengine

import (
	"cmp"
	"slices"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type recoveryCatalog struct {
	cluster string
	groups  map[string]*topology.Group
	seen    map[storage.Identity]bool
}

func (catalog *recoveryCatalog) add(group string, r topology.Replica) {
	if r.NodeName == "" || r.ReplicaID == 0 {
		return
	}
	id := storage.Identity{ClusterID: catalog.cluster, GroupID: group, NodeName: r.NodeName, ReplicaID: r.ReplicaID}
	if catalog.seen[id] {
		return
	}
	catalog.seen[id] = true
	g := catalog.groups[group]
	if g == nil {
		g = &topology.Group{ID: group}
		catalog.groups[group] = g
	}
	g.Replicas = append(g.Replicas, r)
}

func (catalog *recoveryCatalog) addDocument(doc topology.Document) {
	for _, g := range doc.Groups {
		for _, r := range g.Replicas {
			catalog.add(g.ID, r)
		}
	}
}

func (catalog *recoveryCatalog) sorted() []topology.Group {
	result := make([]topology.Group, 0, len(catalog.groups))
	for _, group := range catalog.groups {
		slices.SortFunc(group.Replicas, func(a, b topology.Replica) int {
			if order := cmp.Compare(a.NodeName, b.NodeName); order != 0 {
				return order
			}
			return cmp.Compare(a.ReplicaID, b.ReplicaID)
		})
		result = append(result, *group)
	}
	slices.SortFunc(result, func(a, b topology.Group) int { return cmp.Compare(a.ID, b.ID) })
	return result
}
