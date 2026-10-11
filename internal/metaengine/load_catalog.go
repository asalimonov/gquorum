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
		addGroup(sm.active, false)
		approved := make(map[uint64]bool)
		cleaned := make(map[storage.Identity]bool)
		for _, op := range sm.operators {
			if op.Version > a.Version {
				continue
			}
			approved[op.Version] = true
			for _, step := range op.Steps {
				if step.GroupID == a.GroupID {
					add(step.NodeName, step.ReplicaID)
					completed := step.Seq < op.CurrentStep || op.State == topology.OperatorSucceeded && step.Seq == op.CurrentStep
					if completed && step.Kind == topology.StepDeleteGroupState {
						// A prior attempt durably verified whole-group cleanup.
						// Group IDs cannot be reused, so no new incarnation exists.
						return []storage.Identity{}, nil
					}
					if completed && step.Kind == topology.StepCleanupReplicaState {
						cleaned[storage.Identity{ClusterID: sm.target.ClusterID, NodeName: step.NodeName, GroupID: a.GroupID, ReplicaID: step.ReplicaID}] = true
					}
				}
			}
		}
		for _, doc := range sm.history {
			if approved[doc.Version] {
				addGroup(doc, false)
			}
		}
		if len(ids) == 0 {
			return nil, fault.ErrHistoryUnavailable
		}
		for id := range cleaned {
			delete(ids, id)
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
