package metaengine

import (
	"cmp"
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (r *scopedRetention) roots() {
	s := r.state
	for _, version := range []uint64{s.Latest.Version, s.Target.Version, s.FullyConverged.Version} {
		r.document(version)
	}
	history := slices.Clone(s.History)
	slices.SortFunc(history, func(a, b topology.Document) int { return cmp.Compare(b.Version, a.Version) })
	for _, doc := range history[:min(len(history), historyRetention)] {
		r.document(doc.Version)
		r.operation(s.indexes.first[doc.Version])
	}
	for _, operation := range s.Operations {
		if !operation.Status.State.Terminal() {
			r.operation(operation.Status.ID)
		}
	}
	for _, debt := range s.Cleanup {
		if !cleanupComplete(debt) {
			r.cleanup[debt.Identity] = true
		}
	}
	for _, record := range s.AuthorityRecords {
		if !r.catalog.recordSettled(&record.authorityRecord) || r.hostingRoot(record.Grant) {
			r.grants[record.Grant.Key] = true
		}
	}
	r.checkpointRoots()
	r.admissionRoots()
	r.pendingDeletionRoots()
}

func cleanupComplete(debt cleanupRecord) bool {
	return debt.Retired != nil && (debt.Storage == topology.StorageMemory || debt.Collection != nil)
}

func (r *scopedRetention) checkpointRoots() {
	s := r.state
	for _, row := range s.Groups {
		r.operation(row.Owner)
		if row.Declaration == nil {
			continue
		}
		source, _ := checkpointDocument(s, s.indexes, row)
		witness := r.checkpointWitness(source, row)
		r.document(witness.Version)
	}
	for _, row := range s.Nodes {
		r.operation(row.Owner)
		if row.Declaration == nil {
			continue
		}
		version := row.SourceVersion
		for _, doc := range []topology.Document{s.Target, s.indexes.documents[s.FullyConverged.Version]} {
			if doc.Version != 0 && doc.MemberID(row.NodeName) == row.MemberID &&
				reflect.DeepEqual(doc.Node(row.NodeName), row.Declaration) {
				version = doc.Version
				break
			}
		}
		r.document(version)
	}
}

func (r *scopedRetention) checkpointWitness(source topology.Document, row groupConvergence) topology.Document {
	for _, doc := range []topology.Document{r.state.Target, r.state.indexes.documents[r.state.FullyConverged.Version]} {
		if doc.Version != 0 && reflect.DeepEqual(doc.Group(row.GroupID), row.Declaration) &&
			sameGroupMembers(source, doc, row.Declaration) {
			return doc
		}
	}
	return source
}

func (r *scopedRetention) admissionRoots() {
	s := r.state
	for _, row := range s.Admissions {
		if !row.State.terminal() || row.State == AdmissionCompleted &&
			s.Target.MemberID(row.NodeName) == row.MemberID && !slices.Contains(s.Activated, row.NodeName) {
			r.admissions[admissionKey(row.NodeName, row.InstanceID)] = true
		}
	}
	if founding := s.indexes.documents[1]; founding.Origin == topology.OriginFounding {
		for _, node := range founding.Nodes {
			if !slices.Contains(s.Activated, node.Name) &&
				s.Target.MemberID(node.Name) == founding.MemberID(node.Name) {
				r.operation(s.indexes.first[1]) // Late seed still needs its one founding bind.
			}
		}
	}
}

func (r *scopedRetention) hostingRoot(grant topology.AuthorityGrant) bool {
	action := grant.Action
	if action == nil {
		return false
	}
	row := r.state.indexes.groups[grant.Key.GroupID]
	switch action.Kind {
	case topology.StepCreateFoundingGroup:
		return row == nil || row.SourceVersion == 0 // Includes every issued outcome.
	case topology.StepAddLearner, topology.StepPromoteVoter:
		if !groupHasMember(r.state.Target.Group(action.GroupID), action.NodeName, action.ReplicaID) {
			return false
		}
	case topology.StepPrepareReplica:
	default:
		return false
	}
	for _, target := range grant.Targets {
		// Native targets also contain recipients/reporters; only the action's
		// affected identity needs this pre-convergence establishment proof.
		if target.NodeName != action.NodeName || target.ReplicaID != action.ReplicaID {
			continue
		}
		if r.hostingTarget(grant.Key.GroupID, target) {
			return true
		}
	}
	return false
}

func (r *scopedRetention) hostingTarget(group string, target topology.AuthorityTarget) bool {
	row := r.state.indexes.groups[group]
	if row != nil && row.SourceVersion != 0 && groupHasMember(row.Declaration, target.NodeName, target.ReplicaID) {
		return false
	}
	id := storage.Identity{ClusterID: r.state.Target.ClusterID, GroupID: group,
		NodeName: target.NodeName, ReplicaID: target.ReplicaID}
	if debt := r.state.indexes.cleanup[id]; debt != nil && debt.Retired != nil {
		return false
	}
	return !r.retiredTarget(group, target)
}

func (r *scopedRetention) retiredTarget(group string, target topology.AuthorityTarget) bool {
	for _, record := range r.state.AuthorityRecords {
		grant := record.Grant
		if grant.Purpose == topology.AuthorityReadmit && grant.Key.GroupID == group &&
			slices.Contains(grant.Targets, target) && r.catalog.readmissionBatchConsumed(grant.AdmissionID) {
			return true
		}
	}
	return false
}

func (r *scopedRetention) pendingDeletionRoots() {
	for _, row := range r.state.Groups {
		if !row.Deleting {
			continue
		}
		r.deletionGroup(row.GroupID)
	}
}

func (r *scopedRetention) deletionGroup(group string) {
	for _, doc := range r.state.indexes.documents {
		if doc.Group(group) != nil {
			r.document(doc.Version)
			r.operation(r.state.indexes.first[doc.Version])
		}
	}
	for _, record := range r.state.AuthorityRecords {
		if record.Grant.Key.GroupID == group {
			r.grants[record.Grant.Key] = true
		}
	}
	for _, debt := range r.state.Cleanup {
		if debt.Identity.GroupID == group {
			r.cleanup[debt.Identity] = true
		}
	}
}
