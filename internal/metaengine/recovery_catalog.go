package metaengine

import (
	"cmp"
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// RecoveryCatalog returns owned approved identity evidence, including older
// interrupted preparations. A group may contain multiple former identities for
// one node; these are cleanup candidates, not a desired membership document.
// Staged-only Latest is never authority for local resource retirement.
func (sm *topologySM) RecoveryCatalog(ctx context.Context) ([]topology.Group, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	groups := make(map[string]*topology.Group)
	seen := make(map[storage.Identity]bool)
	add := func(group string, r topology.Replica) {
		if r.NodeName == "" || r.ReplicaID == 0 {
			return
		}
		id := sm.loadIdentity(group, r.NodeName, r.ReplicaID)
		if seen[id] {
			return
		}
		seen[id] = true
		g := groups[group]
		if g == nil {
			g = &topology.Group{ID: group}
			groups[group] = g
		}
		g.Replicas = append(g.Replicas, r)
	}
	addDoc := func(doc topology.Document) {
		for _, g := range doc.Groups {
			for _, r := range g.Replicas {
				add(g.ID, r)
			}
		}
	}
	addDoc(sm.target)
	addDoc(sm.active)
	var records []*operatorRecord
	for _, op := range sm.operators {
		records = append(records, op)
	}
	slices.SortFunc(records, func(a, b *operatorRecord) int { return cmp.Compare(a.ID, b.ID) })
	proofs := sm.cleanupProofsLocked()
	for _, op := range records {
		doc, found := sm.documentLocked(op.Version)
		if found {
			addDoc(doc)
		}
		for _, step := range op.Steps {
			if step.Kind == topology.StepCreateFoundingGroup && step.GroupID != topology.MetaGroupID &&
				sm.active.Version < op.Version && proofs.groups[step.GroupID] == "" && (!found || doc.Group(step.GroupID) == nil) {
				return nil, fault.ErrHistoryUnavailable
			}
			add(step.GroupID, topology.Replica{NodeName: step.NodeName, ReplicaID: step.ReplicaID})
		}
	}
	for _, record := range sm.authority.records {
		for _, target := range record.Grant.Targets {
			add(record.Grant.Key.GroupID, topology.Replica{NodeName: target.NodeName, ReplicaID: target.ReplicaID})
		}
	}
	result := make([]topology.Group, 0, len(groups))
	for _, group := range groups {
		slices.SortFunc(group.Replicas, func(a, b topology.Replica) int {
			if order := cmp.Compare(a.NodeName, b.NodeName); order != 0 {
				return order
			}
			return cmp.Compare(a.ReplicaID, b.ReplicaID)
		})
		result = append(result, *group)
	}
	slices.SortFunc(result, func(a, b topology.Group) int { return cmp.Compare(a.ID, b.ID) })
	return result, nil
}
