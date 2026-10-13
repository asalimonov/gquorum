package metaengine

import (
	"cmp"
	"context"
	"maps"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// LoadCatalog describes current approved routing/observation candidates. It does
// not grant an external effect; effects require immutable committed grants.
func (sm *topologySM) LoadCatalog(ctx context.Context, group string) ([]storage.Identity, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := topology.ValidateLoadID(group); err != nil {
		return nil, err
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ids := make(map[storage.Identity]bool)
	for _, doc := range []topology.Document{sm.active, sm.target} {
		if g := doc.Group(group); g != nil {
			for _, r := range g.Replicas {
				ids[storage.Identity{ClusterID: doc.ClusterID, NodeName: r.NodeName, GroupID: group, ReplicaID: r.ReplicaID}] = true
			}
		}
	}
	return orderedIdentities(ids), nil
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

// DeletionBase supplies approved historical identities for planning cleanup,
// including interrupted preparations in surviving groups.
// It is private planning input, never a replacement for the public Active view.
func (sm *topologySM) DeletionBase(ctx context.Context, desired topology.Document) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.deletionBaseLocked(desired)
}

// OperatorBase preserves the exact retired identities named by an immutable
// retry even when earlier consumption removed them from new planning input.
func (sm *topologySM) OperatorBase(ctx context.Context, id topology.OperatorID) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	op := sm.operators[id]
	if op == nil || op.Version != sm.target.Version {
		return topology.Document{}, fault.ErrOperatorAuthorization
	}
	return sm.operatorBaseLocked(op)
}

func (sm *topologySM) operatorBaseLocked(op *operatorRecord) (topology.Document, error) {
	base, err := sm.deletionBaseLocked(sm.target)
	if err != nil {
		return topology.Document{}, err
	}
	for _, step := range op.Steps {
		if step.Kind != topology.StepRemoveReplica && step.Kind != topology.StepStopReplica && step.Kind != topology.StepCleanupReplicaState {
			continue
		}
		group := base.Group(step.GroupID)
		if group == nil {
			base.Groups = append(base.Groups, topology.Group{ID: step.GroupID})
			group = &base.Groups[len(base.Groups)-1]
		}
		if !slices.ContainsFunc(group.Replicas, func(r topology.Replica) bool { return r.ReplicaID == step.ReplicaID && r.NodeName == step.NodeName }) {
			group.Replicas = append(group.Replicas, topology.Replica{NodeName: step.NodeName, ReplicaID: step.ReplicaID, Role: topology.ReplicaVoter})
		}
	}
	canonicalizeDocument(&base)
	return base, nil
}

func (sm *topologySM) deletionBaseLocked(desired topology.Document) (topology.Document, error) {
	base := sm.active.Clone()
	groups := make(map[string]bool)
	for _, g := range base.Groups {
		groups[g.ID] = true
	}
	for _, op := range sm.operators {
		for _, step := range op.Steps {
			groups[step.GroupID] = true
		}
	}
	for _, record := range sm.authority.records {
		groups[record.Grant.Key.GroupID] = true
	}
	proofs := sm.cleanupProofsLocked()
	for group := range groups {
		// A deletion already incorporated into Active needs no new operator.
		if desired.Group(group) == nil && base.Group(group) == nil && sm.retired[group] != 0 && sm.active.Version >= sm.retired[group] {
			continue
		}
		ids, err := sm.deletionCatalogLocked(group)
		if err != nil {
			return topology.Document{}, err
		}
		if desired.Group(group) != nil {
			// Readmission retires host resources before their old native members
			// are removed. Preserve their routing identities while the replacement
			// is not yet Active; its gated preparation covers host retirement.
			for id := range proofs.readmitted {
				if id.GroupID == group && desired.MemberID(id.NodeName) != 0 && desired.MemberID(id.NodeName) != sm.active.MemberID(id.NodeName) {
					ids[id] = true
				}
			}
		}
		g := base.Group(group)
		if desired.Group(group) != nil {
			// Desired-only identities do not establish a previously absent group.
			// Retain an historical owner only when it still requires retirement.
			for id := range ids {
				if r := documentReplica(desired, group, id.ReplicaID); r != nil && r.NodeName == id.NodeName {
					delete(ids, id)
				}
			}
			if g == nil && len(ids) == 0 {
				continue
			}
			if g != nil {
				for _, r := range g.Replicas {
					if wanted := documentReplica(desired, group, r.ReplicaID); wanted != nil && wanted.NodeName == r.NodeName {
						ids[sm.loadIdentity(group, r.NodeName, r.ReplicaID)] = true
					}
				}
			}
		}
		if g == nil {
			base.Groups = append(base.Groups, topology.Group{ID: group})
			g = &base.Groups[len(base.Groups)-1]
		}
		previous := g.Replicas
		g.Replicas = nil
		for _, id := range orderedIdentities(ids) {
			r := topology.Replica{NodeName: id.NodeName, ReplicaID: id.ReplicaID, Role: topology.ReplicaVoter}
			for _, old := range previous {
				if old.NodeName == id.NodeName && old.ReplicaID == id.ReplicaID {
					r = old
					break
				}
			}
			g.Replicas = append(g.Replicas, r)
			if group == topology.MetaGroupID && base.Node(id.NodeName) == nil {
				// An admitted member may enter native membership before Active.
				// Keep its approved node attributes for drain/health planning too.
				for _, doc := range sm.authorityDocumentsLocked() {
					if node := doc.Node(id.NodeName); node != nil {
						owned := *node
						owned.Labels = maps.Clone(node.Labels)
						base.Nodes = append(base.Nodes, owned)
						break
					}
				}
			}
		}
	}
	canonicalizeDocument(&base)
	return base, nil
}

func (sm *topologySM) deletionCatalogLocked(group string) (map[storage.Identity]bool, error) {
	ids := make(map[storage.Identity]bool)
	proofs := sm.cleanupProofsLocked()
	if proofs.groups[group] != "" {
		return ids, nil
	}
	known := false
	addGroup := func(doc topology.Document) {
		if g := doc.Group(group); g != nil {
			known = true
			for _, r := range g.Replicas {
				ids[sm.loadIdentity(group, r.NodeName, r.ReplicaID)] = true
			}
		}
	}
	addGroup(sm.active)
	for _, op := range sm.operators {
		if doc, ok := sm.documentLocked(op.Version); ok {
			addGroup(doc)
		}
		for _, step := range op.Steps {
			if step.GroupID != group {
				continue
			}
			known = true
			if step.Kind == topology.StepCreateFoundingGroup && sm.active.Version < op.Version {
				doc, ok := sm.documentLocked(op.Version)
				if !ok || doc.Group(group) == nil {
					return nil, fault.ErrHistoryUnavailable
				}
			}
			if step.NodeName != "" && step.ReplicaID != 0 {
				ids[sm.loadIdentity(group, step.NodeName, step.ReplicaID)] = true
			}
		}
	}
	for _, record := range sm.authority.records {
		if record.Grant.Key.GroupID != group {
			continue
		}
		known = true
		for _, target := range record.Grant.Targets {
			ids[sm.loadIdentity(group, target.NodeName, target.ReplicaID)] = true
		}
	}
	if !known {
		return nil, fault.ErrHistoryUnavailable
	}
	for id := range ids {
		if proofs.covers(id) {
			delete(ids, id)
		}
	}
	return ids, nil
}
