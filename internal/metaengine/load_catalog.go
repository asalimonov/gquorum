package metaengine

import (
	"cmp"
	"context"
	"slices"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// LoadCatalog returns approved exact routing candidates; it grants no effect.
func (m *Machine) LoadCatalog(ctx context.Context, group string) ([]storage.Identity, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := topology.ValidateLoadID(group); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return scopedLoadCatalog(m.scoped, group), nil
}

func scopedLoadCatalog(s *scopedState, group string) []storage.Identity {
	ids := make(map[storage.Identity]bool)
	addScopedDocumentGroup(ids, s.Target, group)
	if row := s.indexes.groups[group]; row != nil && row.Declaration != nil {
		if doc, found := checkpointDocument(s, s.indexes, *row); found {
			addScopedDocumentGroup(ids, doc, group)
		}
	}
	for _, operation := range s.Operations {
		if doc := s.indexes.documents[operation.Status.Version]; doc.Version != 0 {
			addScopedDocumentGroup(ids, doc, group)
		}
	}
	addScopedGrantTargets(ids, s, group)
	for _, debt := range s.Cleanup {
		if debt.Identity.GroupID == group {
			ids[debt.Identity] = true
		}
	}
	return orderedIdentities(ids)
}

func addScopedDocumentGroup(ids map[storage.Identity]bool, doc topology.Document, group string) {
	if declaration := doc.Group(group); declaration != nil {
		for _, replica := range declaration.Replicas {
			ids[storage.Identity{ClusterID: doc.ClusterID, NodeName: replica.NodeName, GroupID: group,
				ReplicaID: replica.ReplicaID}] = true
		}
	}
}

func addScopedGrantTargets(ids map[storage.Identity]bool, s *scopedState, group string) {
	for _, record := range s.AuthorityRecords {
		if record.Grant.Key.GroupID != group {
			continue
		}
		for _, target := range record.Grant.Targets {
			ids[storage.Identity{ClusterID: record.Grant.Key.ClusterID, NodeName: target.NodeName,
				GroupID: group, ReplicaID: target.ReplicaID}] = true
		}
	}
}

func orderedIdentities(ids map[storage.Identity]bool) []storage.Identity {
	out := make([]storage.Identity, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	slices.SortFunc(out, func(a, b storage.Identity) int {
		if order := cmp.Compare(a.NodeName, b.NodeName); order != 0 {
			return order
		}
		return cmp.Compare(a.ReplicaID, b.ReplicaID)
	})
	return out
}
