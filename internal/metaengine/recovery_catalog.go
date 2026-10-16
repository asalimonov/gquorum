package metaengine

import (
	"cmp"
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// LegacyRecoveryCatalog is the schema3 projection removed at Task78.
// It returns owned approved identity evidence, including older
// interrupted preparations. A group may contain multiple former identities for
// one node; these are cleanup candidates, not a desired membership document.
// Staged-only Latest is never authority for local resource retirement.
func (sm *topologySM) LegacyRecoveryCatalog(ctx context.Context) ([]topology.Group, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	catalog := recoveryCatalog{cluster: sm.target.ClusterID, groups: make(map[string]*topology.Group),
		seen: make(map[storage.Identity]bool)}
	catalog.addDocument(sm.target)
	catalog.addDocument(sm.active)
	var records []*operatorRecord
	for _, op := range sm.operators {
		records = append(records, op)
	}
	slices.SortFunc(records, func(a, b *operatorRecord) int { return cmp.Compare(a.ID, b.ID) })
	proofs := sm.cleanupProofsLocked()
	for _, op := range records {
		if err := catalog.addOperator(sm, op, proofs); err != nil {
			return nil, err
		}
	}
	for _, record := range sm.authority.records {
		for _, target := range record.Grant.Targets {
			catalog.add(record.Grant.Key.GroupID,
				topology.Replica{NodeName: target.NodeName, ReplicaID: target.ReplicaID})
		}
	}
	return catalog.sorted(), nil
}

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

func (catalog *recoveryCatalog) addOperator(sm *topologySM, op *operatorRecord, proofs cleanupProofs) error {
	doc, found := sm.documentLocked(op.Version)
	if found {
		catalog.addDocument(doc)
	}
	for _, step := range op.Steps {
		if step.Kind == topology.StepCreateFoundingGroup && step.GroupID != topology.MetaGroupID &&
			sm.active.Version < op.Version && proofs.groups[step.GroupID] == "" && (!found ||
			doc.Group(step.GroupID) == nil) {
			return fault.ErrHistoryUnavailable
		}
		catalog.add(step.GroupID, topology.Replica{NodeName: step.NodeName, ReplicaID: step.ReplicaID})
	}
	return nil
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
