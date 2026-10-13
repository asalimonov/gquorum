package metaengine

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Cleanup evidence lives in the existing exact Operator plans and bounded
// document history. It must outlive a superseded attempt: an RPC may have
// created application state before the leader recorded its acknowledgement.
type cleanupProofs struct {
	replicas   map[storage.Identity]topology.OperatorID
	groups     map[string]topology.OperatorID
	readmitted map[storage.Identity]topology.OperatorID
}

func completedStep(op *operatorRecord, step topology.Step) bool {
	return step.Seq < op.CurrentStep || op.State == topology.OperatorSucceeded && step.Seq == op.CurrentStep
}

func (sm *topologySM) loadIdentity(group, node string, replica uint64) storage.Identity {
	return storage.Identity{ClusterID: sm.active.ClusterID, GroupID: group, NodeName: node, ReplicaID: replica}
}

func (sm *topologySM) cleanupProofsLocked() cleanupProofs {
	p := cleanupProofs{replicas: make(map[storage.Identity]topology.OperatorID), groups: make(map[string]topology.OperatorID), readmitted: make(map[storage.Identity]topology.OperatorID)}
	for _, op := range sm.operators {
		for _, step := range op.Steps {
			if !completedStep(op, step) {
				continue
			}
			switch step.Kind {
			case topology.StepStopReplica:
				if step.GroupID == topology.MetaGroupID {
					id := sm.loadIdentity(step.GroupID, step.NodeName, step.ReplicaID)
					if old := p.replicas[id]; old == "" || op.ID < old {
						p.replicas[id] = op.ID
					}
				}
			case topology.StepCleanupReplicaState:
				id := sm.loadIdentity(step.GroupID, step.NodeName, step.ReplicaID)
				if old := p.replicas[id]; old == "" || op.ID < old {
					p.replicas[id] = op.ID
				}
			case topology.StepDeleteGroupState:
				if old := p.groups[step.GroupID]; old == "" || op.ID < old {
					p.groups[step.GroupID] = op.ID
				}
			}
		}
	}
	for _, record := range sm.authority.records {
		grant := record.Grant
		if grant.Action != nil && (grant.Action.Kind == topology.StepCleanupReplicaState || grant.Action.Kind == topology.StepDeleteGroupState ||
			grant.Key.GroupID == topology.MetaGroupID && grant.Action.Kind == topology.StepStopReplica) {
			for _, receipt := range record.Effects {
				if receipt.Resource.Kind != topology.AuthorityHost {
					continue
				}
				id := sm.loadIdentity(grant.Key.GroupID, receipt.Target.NodeName, receipt.Target.ReplicaID)
				if old := p.replicas[id]; old == "" || record.Grant.OperatorID < old {
					p.replicas[id] = record.Grant.OperatorID
				}
			}
			if grant.Action.Kind == topology.StepDeleteGroupState && len(grant.Resources) > 0 && !slices.ContainsFunc(grant.Resources, func(resource topology.AuthorityResource) bool {
				return !resourceConsumed(record, resource)
			}) {
				if old := p.groups[grant.Key.GroupID]; old == "" || grant.OperatorID < old {
					p.groups[grant.Key.GroupID] = grant.OperatorID
				}
			}
		}
		if record.Grant.Purpose != topology.AuthorityReadmit {
			continue
		}
		var owner topology.OperatorID
		for _, admission := range sm.admissions {
			if ReadmissionID(*admission) == record.Grant.AdmissionID {
				owner = admission.OperatorID
				break
			}
		}
		if owner == "" {
			continue
		}
		for _, receipt := range record.Effects {
			id := sm.loadIdentity(record.Grant.Key.GroupID, receipt.Target.NodeName, receipt.Target.ReplicaID)
			p.readmitted[id] = owner
			if old := p.replicas[id]; old == "" || owner < old {
				p.replicas[id] = owner
			}
		}
	}
	return p
}

func (p cleanupProofs) covers(id storage.Identity) bool {
	return p.groups[id.GroupID] != "" || p.replicas[id] != ""
}

func (sm *topologySM) documentLocked(version uint64) (topology.Document, bool) {
	for _, doc := range []topology.Document{sm.active, sm.target, sm.latest} {
		if doc.Version == version {
			return doc, true
		}
	}
	for _, doc := range sm.history {
		if doc.Version == version {
			return doc, true
		}
	}
	return topology.Document{}, false
}

func (sm *topologySM) activeContains(id storage.Identity) bool {
	r := documentReplica(sm.active, id.GroupID, id.ReplicaID)
	return r != nil && r.NodeName == id.NodeName
}

func (sm *topologySM) ownershipNeedsDocument(op *operatorRecord, step topology.Step, proofs cleanupProofs) bool {
	if step.Kind == topology.StepPrepareReplica && step.GroupID == topology.MetaGroupID {
		id := sm.loadIdentity(step.GroupID, step.NodeName, step.ReplicaID)
		return !sm.activeContains(id) && !proofs.covers(id)
	}
	if step.Kind != topology.StepCreateFoundingGroup || step.GroupID == topology.MetaGroupID || proofs.groups[step.GroupID] != "" {
		return false
	}
	doc, ok := sm.documentLocked(op.Version)
	if !ok || doc.Group(step.GroupID) == nil {
		return sm.active.Version < op.Version
	}
	for _, r := range doc.Group(step.GroupID).Replicas {
		id := sm.loadIdentity(step.GroupID, r.NodeName, r.ReplicaID)
		if r.Role == topology.ReplicaVoter && !sm.activeContains(id) && !proofs.covers(id) {
			return true
		}
	}
	return false
}

func (sm *topologySM) pinnedOwnershipDocuments(proofs cleanupProofs) map[uint64]bool {
	pins := make(map[uint64]bool)
	for _, op := range sm.operators {
		for _, step := range op.Steps {
			if sm.ownershipNeedsDocument(op, step, proofs) {
				pins[op.Version] = true
			}
		}
	}
	return pins
}

// Reserve one history slot for further edits, including a cleanup approval.
// Backpressure applies to unresolved founding or metadata-preparation documents,
// not cluster size or load-group count; reserve space for a cleanup approval.
func (sm *topologySM) validateCleanupCapacity(plan topology.Plan) error {
	pins := sm.pinnedOwnershipDocuments(sm.cleanupProofsLocked())
	introducesOwnership := false
	for _, step := range plan.Steps {
		if step.Kind == topology.StepCreateFoundingGroup && step.GroupID != topology.MetaGroupID || step.Kind == topology.StepPrepareReplica && step.GroupID == topology.MetaGroupID {
			pins[plan.Version] = true
			introducesOwnership = true
		}
	}
	if introducesOwnership && len(pins) >= historyRetention {
		return fmt.Errorf("unresolved ownership fills document history; complete an earlier retirement first")
	}
	return nil
}

func (sm *topologySM) nextHistoryLocked(doc topology.Document) ([]topology.Document, error) {
	pins := sm.pinnedOwnershipDocuments(sm.cleanupProofsLocked())
	history := append([]topology.Document(nil), sm.history...)
	for version := range pins {
		if slices.ContainsFunc(history, func(d topology.Document) bool { return d.Version == version }) {
			continue
		}
		if old, ok := sm.documentLocked(version); ok {
			history = append(history, old.Clone())
		}
	}
	history = append(history, doc.Clone())
	slices.SortFunc(history, func(a, b topology.Document) int { return cmp.Compare(a.Version, b.Version) })
	for len(history) > historyRetention {
		i := slices.IndexFunc(history, func(d topology.Document) bool { return !pins[d.Version] && d.Version != doc.Version })
		if i < 0 {
			return nil, fmt.Errorf("document history is occupied by unresolved founding cleanup")
		}
		history = slices.Delete(history, i, i+1)
	}
	return history, nil
}

func (sm *topologySM) pruneOperatorsLocked() {
	defer sm.pruneVoterSwitchesLocked()
	proofs := sm.cleanupProofsLocked()
	versions := map[uint64]bool{sm.active.Version: true, sm.target.Version: true}
	for _, doc := range sm.history {
		versions[doc.Version] = true
	}
	keep := make(map[topology.OperatorID]bool)
	for _, op := range sm.operators {
		keep[op.ID] = versions[op.Version] || !op.State.Terminal()
		for _, step := range op.Steps {
			if sm.ownershipNeedsDocument(op, step, proofs) {
				keep[op.ID] = true
			}
			if step.Kind == topology.StepPrepareReplica {
				id := sm.loadIdentity(step.GroupID, step.NodeName, step.ReplicaID)
				if !sm.activeContains(id) && !proofs.covers(id) {
					keep[op.ID] = true
				}
			}
		}
	}
	for _, record := range sm.authority.records {
		if !sm.authority.recordSettled(record) && record.Grant.OperatorID != "" {
			keep[record.Grant.OperatorID] = true
		}
	}
	for id, revocation := range sm.authority.revocations {
		if !revokedComplete(revocation) && sm.operators[id] != nil {
			keep[id] = true
		}
	}
	// Keep cleanup acknowledgements while an older retained source could otherwise
	// resurrect its identity in a deletion catalog. Compute dependencies before
	// removing records, independently of map iteration order.
	var pending []topology.OperatorID
	for id, retained := range keep {
		if retained {
			pending = append(pending, id)
		}
	}
	keepProof := func(id storage.Identity) {
		owner := proofs.groups[id.GroupID]
		if owner == "" {
			owner = proofs.replicas[id]
		}
		if owner != "" && !keep[owner] {
			keep[owner] = true
			pending = append(pending, owner)
		}
	}
	for _, doc := range append(append([]topology.Document(nil), sm.history...), sm.active, sm.target) {
		for _, group := range doc.Groups {
			for _, r := range group.Replicas {
				keepProof(sm.loadIdentity(group.ID, r.NodeName, r.ReplicaID))
			}
		}
	}
	for len(pending) > 0 {
		op := sm.operators[pending[0]]
		pending = pending[1:]
		if op == nil {
			// Retained readmission receipts can outlive their operator.
			continue
		}
		for _, step := range op.Steps {
			keepProof(sm.loadIdentity(step.GroupID, step.NodeName, step.ReplicaID))
		}
	}
	for id := range sm.operators {
		if !keep[id] {
			delete(sm.operators, id)
		}
	}
}
