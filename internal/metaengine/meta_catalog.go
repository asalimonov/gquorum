package metaengine

import "github.com/asalimonov/gquorum/topology"

// Approved intermediate members may be native participants before reaching
// Active. Retain inbound routing until exact native removal is consumed, while
// host retirement uses its separately retained grant even after removal.
func (sm *topologySM) metaMemberOwnersLocked() map[uint64]string {
	routing := metaOwnerCatalog{removed: sm.consumedMetaRemovalsLocked(), owners: make(map[uint64]string)}
	routing.addApprovedDocuments(sm)
	routing.addIntermediateSteps(sm)
	routing.addAuthorityTargets(sm)
	return routing.owners
}

// scopedMetaMemberOwners builds the schema-4 inbound native membership catalog
// from approved, proven and currently executable evidence. It deliberately
// ignores retained document history: a document can outlive its removal proof.
// Task78 wires this read after the scoped state cutover.
func scopedMetaMemberOwners(state *scopedState) (map[uint64]string, error) {
	routing := metaOwnerCatalog{removed: make(map[uint64]bool), owners: make(map[uint64]string)}
	if state == nil {
		return routing.owners, nil
	}
	groupResource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: topology.MetaGroupID}
	routing.addScopedMetaGrants(state, groupResource)
	routing.addScopedMetaCleanup(state)
	routing.addDocument(state.Target)
	var owner topology.OperationID
	for _, row := range state.Groups {
		if row.GroupID == topology.MetaGroupID {
			routing.addGroup(row.Declaration)
			owner = row.Owner
			break
		}
	}
	routing.addScopedOwnerPlan(state, owner)
	for member := range routing.removed {
		delete(routing.owners, member)
	}
	if routing.conflict {
		return nil, invalidScopedState("meta member catalog binds one ID to multiple nodes")
	}
	return routing.owners, nil
}

func (routing *metaOwnerCatalog) addScopedMetaGrants(state *scopedState,
	groupResource topology.AuthorityResource) {
	for i := range state.AuthorityRecords {
		record := &state.AuthorityRecords[i].authorityRecord
		action := record.Grant.Action
		if action != nil && action.GroupID == topology.MetaGroupID && action.Kind == topology.StepRemoveReplica &&
			resourceConsumed(record, groupResource) {
			routing.removed[action.ReplicaID] = true
		}
		if record.Grant.Key.GroupID != topology.MetaGroupID || record.Grant.Purpose == topology.AuthorityEffect &&
			resourceConsumed(record, groupResource) {
			continue
		}
		for _, target := range record.Grant.Targets {
			routing.add(target.MemberID, target.NodeName)
		}
	}
}

func (routing *metaOwnerCatalog) addScopedMetaCleanup(state *scopedState) {
	for _, debt := range state.Cleanup {
		if receipt := debt.Removed; receipt != nil && receipt.Action != nil &&
			receipt.Action.GroupID == topology.MetaGroupID && receipt.Action.Kind == topology.StepRemoveReplica {
			routing.removed[receipt.Action.ReplicaID] = true
		}
	}
}

func (routing *metaOwnerCatalog) addDocument(doc topology.Document) {
	if group := doc.MetaGroup(); group != nil {
		routing.addGroup(group)
	}
}

func (routing *metaOwnerCatalog) addGroup(group *topology.Group) {
	if group == nil {
		return
	}
	for _, replica := range group.Replicas {
		routing.add(replica.ReplicaID, replica.NodeName)
	}
}

func (routing *metaOwnerCatalog) addScopedOwnerPlan(state *scopedState, owner topology.OperationID) {
	for _, operation := range state.Operations {
		if operation.Status.ID != owner || owner == "" {
			continue
		}
		for _, step := range operation.Plan.Steps {
			if step.GroupID == topology.MetaGroupID {
				routing.add(step.ReplicaID, step.NodeName)
			}
		}
		return
	}
}

func (sm *topologySM) consumedMetaRemovalsLocked() map[uint64]bool {
	removed := make(map[uint64]bool)
	for _, op := range sm.operators {
		for _, step := range op.Steps {
			if step.GroupID == topology.MetaGroupID && step.Kind == topology.StepRemoveReplica &&
				completedStep(op, step) {
				removed[step.ReplicaID] = true
			}
		}
	}
	for _, record := range sm.authority.records {
		grant := record.Grant
		if grant.Key.GroupID == topology.MetaGroupID && grant.Action != nil &&
			grant.Action.Kind == topology.StepRemoveReplica &&
			resourceConsumed(record, topology.AuthorityResource{Kind: topology.AuthorityGroup,
				GroupID: topology.MetaGroupID}) {
			removed[grant.Action.ReplicaID] = true
		}
	}
	return removed
}

type metaOwnerCatalog struct {
	removed  map[uint64]bool
	owners   map[uint64]string
	conflict bool
}

func (routing *metaOwnerCatalog) add(member uint64, node string) {
	if member != 0 && node != "" && !routing.removed[member] {
		if owner := routing.owners[member]; owner != "" && owner != node {
			routing.conflict = true
		}
		routing.owners[member] = node
	}
}

func (routing *metaOwnerCatalog) addApprovedDocuments(sm *topologySM) {
	for _, doc := range sm.authorityDocumentsLocked() {
		if doc.Version < sm.active.Version {
			continue
		}
		if meta := doc.MetaGroup(); meta != nil {
			for _, replica := range meta.Replicas {
				routing.add(replica.ReplicaID, replica.NodeName)
			}
		}
	}
}

func (routing *metaOwnerCatalog) addIntermediateSteps(sm *topologySM) {
	for _, op := range sm.operators {
		if op.Version <= sm.active.Version {
			continue
		}
		for _, step := range op.Steps {
			if step.GroupID == topology.MetaGroupID {
				routing.add(step.ReplicaID, step.NodeName)
			}
		}
	}
}

func (routing *metaOwnerCatalog) addAuthorityTargets(sm *topologySM) {
	for _, record := range sm.authority.records {
		if record.Grant.Key.GroupID == topology.MetaGroupID && record.Grant.Action != nil &&
			record.Grant.Action.Version > sm.active.Version {
			for _, target := range record.Grant.Targets {
				routing.add(target.MemberID, target.NodeName)
			}
		}
	}
}
