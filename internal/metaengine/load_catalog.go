package metaengine

import (
	"cmp"
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// LoadActionTargets expands a current approval into exact effect targets. A
// deletion includes retained approved preparation lineage, never staged intent.
// Returning owned values under the authorization read boundary prevents callers
// from assembling a mixed catalog out of independently read documents.
func (sm *topologySM) LoadActionTargets(ctx context.Context, a topology.AuthorizedStep) ([]storage.Identity, error) {
	if err := topology.ValidateLoadID(a.GroupID); err != nil {
		return nil, err
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if err := sm.authorizeLocked(ctx, a); err != nil {
		return nil, err
	}
	ids := make(map[storage.Identity]bool)
	add := func(name string, id uint64) {
		if name != "" && id != 0 {
			ids[storage.Identity{ClusterID: sm.target.ClusterID, NodeName: name, GroupID: a.GroupID, ReplicaID: id}] = true
		}
	}
	addGroup := func(doc topology.Document, votersOnly bool) {
		if group := doc.Group(a.GroupID); group != nil {
			for _, r := range group.Replicas {
				if !votersOnly || r.Role == topology.ReplicaVoter {
					add(r.NodeName, r.ReplicaID)
				}
			}
		}
	}
	switch a.Kind {
	case topology.StepCreateFoundingGroup:
		addGroup(sm.target, true)
	case topology.StepDeleteGroupState:
		if sm.target.Group(a.GroupID) != nil {
			return nil, fault.ErrOperatorAuthorization
		}
		ids, err := sm.deletionCatalogLocked(a.GroupID)
		if err != nil {
			return nil, err
		}
		return orderedIdentities(ids), nil
	case topology.StepPrepareReplica, topology.StepAddLearner, topology.StepPromoteVoter, topology.StepDemoteVoter, topology.StepRemoveReplica, topology.StepTransferLeader, topology.StepStopReplica, topology.StepCleanupReplicaState:
		add(a.NodeName, a.ReplicaID)
	default:
		return nil, fault.ErrUnsupported
	}
	if len(ids) == 0 {
		return nil, fault.ErrHistoryUnavailable
	}
	return orderedIdentities(ids), nil
}

// LoadCatalog describes current approved routing/observation candidates. It does
// not grant an external effect; those require LoadActionTargets and Authorize.
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

// DeletionBase supplies approved historical identities only for removed groups.
// It is private planning input, never a replacement for the public Active view.
func (sm *topologySM) DeletionBase(ctx context.Context, desired topology.Document) (topology.Document, error) {
	if err := ctx.Err(); err != nil {
		return topology.Document{}, err
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.deletionBaseLocked(desired)
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
	for group := range groups {
		if group == topology.MetaGroupID || desired.Group(group) != nil {
			continue
		}
		// A deletion already incorporated into Active needs no new operator.
		if base.Group(group) == nil && sm.retired[group] != 0 && sm.active.Version >= sm.retired[group] {
			continue
		}
		ids, err := sm.deletionCatalogLocked(group)
		if err != nil {
			return topology.Document{}, err
		}
		g := base.Group(group)
		if g == nil {
			base.Groups = append(base.Groups, topology.Group{ID: group})
			g = &base.Groups[len(base.Groups)-1]
		}
		g.Replicas = nil
		for _, id := range orderedIdentities(ids) {
			g.Replicas = append(g.Replicas, topology.Replica{NodeName: id.NodeName, ReplicaID: id.ReplicaID, Role: topology.ReplicaVoter})
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
