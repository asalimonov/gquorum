package metaengine

import "github.com/asalimonov/gquorum/topology"

// Approved intermediate members may be native participants before reaching
// Active. Retain inbound routing until exact native removal is consumed, while
// host retirement uses its separately retained grant even after removal.
func (sm *topologySM) metaMemberOwnersLocked() map[uint64]string {
	removed := make(map[uint64]bool)
	for _, op := range sm.operators {
		for _, step := range op.Steps {
			if step.GroupID == topology.MetaGroupID && step.Kind == topology.StepRemoveReplica && completedStep(op, step) {
				removed[step.ReplicaID] = true
			}
		}
	}
	for _, record := range sm.authority.records {
		grant := record.Grant
		if grant.Key.GroupID == topology.MetaGroupID && grant.Action != nil && grant.Action.Kind == topology.StepRemoveReplica &&
			resourceConsumed(record, topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: topology.MetaGroupID}) {
			removed[grant.Action.ReplicaID] = true
		}
	}
	owners := make(map[uint64]string)
	add := func(member uint64, node string) {
		if member != 0 && node != "" && !removed[member] {
			owners[member] = node
		}
	}
	for _, doc := range sm.authorityDocumentsLocked() {
		if doc.Version < sm.active.Version {
			continue
		}
		if meta := doc.MetaGroup(); meta != nil {
			for _, replica := range meta.Replicas {
				add(replica.ReplicaID, replica.NodeName)
			}
		}
	}
	for _, op := range sm.operators {
		if op.Version <= sm.active.Version {
			continue
		}
		for _, step := range op.Steps {
			if step.GroupID == topology.MetaGroupID {
				add(step.ReplicaID, step.NodeName)
			}
		}
	}
	for _, record := range sm.authority.records {
		if record.Grant.Key.GroupID == topology.MetaGroupID && record.Grant.Action != nil && record.Grant.Action.Version > sm.active.Version {
			for _, target := range record.Grant.Targets {
				add(target.MemberID, target.NodeName)
			}
		}
	}
	return owners
}
