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
	removed map[uint64]bool
	owners  map[uint64]string
}

func (routing *metaOwnerCatalog) add(member uint64, node string) {
	if member != 0 && node != "" && !routing.removed[member] {
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
