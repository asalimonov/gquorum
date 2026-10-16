package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// CLEANUP_PROOF: callers hold the FSM lock and select independently retained
// evidence before pruning. A cleanup row does not reference itself. Planning
// and deletion catalogs derive from these same declarations, plans and grants;
// a settled grant still counts whenever another proof keeps it retained.
func (s *scopedState) identityReferencedLocked(id storage.Identity) bool {
	if documentMentionsIdentity(s.Latest, id) || documentMentionsIdentity(s.Target, id) ||
		slices.ContainsFunc(s.History, func(doc topology.Document) bool { return documentMentionsIdentity(doc, id) }) {
		return true
	}
	if id.ClusterID != s.Target.ClusterID {
		return false
	}
	if s.convergenceMentionsIdentity(id) || s.admissionsMentionIdentity(id) {
		return true
	}
	for _, operation := range s.Operations {
		for _, step := range operation.Plan.Steps {
			if stepMentionsIdentity(step, id) {
				return true
			}
		}
	}
	for _, record := range s.AuthorityRecords {
		if grantMentionsIdentity(record.Grant, id) {
			return true
		}
	}
	return false
}

func documentMentionsIdentity(doc topology.Document, id storage.Identity) bool {
	return doc.ClusterID == id.ClusterID && groupHasMember(doc.Group(id.GroupID), id.NodeName, id.ReplicaID)
}

func (s *scopedState) convergenceMentionsIdentity(id storage.Identity) bool {
	for _, row := range s.Groups {
		if row.GroupID == id.GroupID && groupHasMember(row.Declaration, id.NodeName, id.ReplicaID) {
			return true
		}
	}
	if id.GroupID == topology.MetaGroupID {
		return slices.ContainsFunc(s.Nodes, func(row nodeConvergence) bool {
			return row.NodeName == id.NodeName && row.MemberID == id.ReplicaID
		})
	}
	return false
}

func (s *scopedState) admissionsMentionIdentity(id storage.Identity) bool {
	return id.GroupID == topology.MetaGroupID && slices.ContainsFunc(s.Admissions, func(a AdmissionRecord) bool {
		return a.NodeName == id.NodeName && a.MemberID == id.ReplicaID
	})
}

func stepMentionsIdentity(step topology.Step, id storage.Identity) bool {
	if step.GroupID != id.GroupID {
		return false
	}
	return step.Kind == topology.StepCreateFoundingGroup || step.Kind == topology.StepDeleteGroupState ||
		step.NodeName == id.NodeName && step.ReplicaID == id.ReplicaID ||
		step.TransferNodeName == id.NodeName && step.TransferReplicaID == id.ReplicaID
}

func grantMentionsIdentity(grant topology.AuthorityGrant, id storage.Identity) bool {
	if grant.Key.ClusterID != id.ClusterID {
		return false
	}
	return slices.ContainsFunc(grant.Targets, func(target topology.AuthorityTarget) bool {
		return target.NodeName == id.NodeName && (grant.Key.GroupID == id.GroupID && target.ReplicaID == id.ReplicaID ||
			id.GroupID == topology.MetaGroupID && target.MemberID == id.ReplicaID)
	})
}
