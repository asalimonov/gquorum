package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// CLEANUP_PROOF: select independent roots before traversing proof references.
// Complete debt and settled grants cannot retain each other without a root.
// Sets grow monotonically; source slices, rather than map order, drive closure.
// These sets describe actual retained rows, including successful retry siblings.
type scopedRetention struct {
	state       *scopedState
	catalog     authorityCatalog
	documents   map[uint64]bool
	versions    map[uint64]bool
	grants      map[topology.AuthorityKey]bool
	admissions  map[string]bool
	cleanup     map[storage.Identity]bool
	revocations map[int]bool
}

func newScopedRetention(state *scopedState) *scopedRetention {
	return &scopedRetention{state: state, catalog: state.authorityCatalog(),
		documents: map[uint64]bool{}, versions: map[uint64]bool{},
		grants: map[topology.AuthorityKey]bool{}, admissions: map[string]bool{},
		cleanup: map[storage.Identity]bool{}, revocations: map[int]bool{}}
}

// Prune only an owned candidate. Failure leaves the complete original graph
// intact; no caller may publish a partially compacted state. Live installation
// remains isolated with the rest of scoped proposal dispatch until Task 78.
func (s *scopedState) pruneRetainedProofs() error {
	retention := newScopedRetention(s)
	retention.roots()
	for before := -1; before != retention.size(); {
		before = retention.size()
		retention.closeDocumentsAndOperations()
		retention.closeCleanup()
		retention.closeAdmissions()
		retention.closeAuthority()
		retention.closeRetiredMembers()
		candidate := retention.selected()
		for _, debt := range s.Cleanup {
			if candidate.identityReferencedLocked(debt.Identity) {
				retention.cleanup[debt.Identity] = true
			}
		}
	}
	next := retention.selected()
	next.pruneScopedVoterSwitches()
	var err error
	next.indexes, err = prepareScopedGraph(&next, next.Rules)
	if err != nil {
		return err
	}
	*s = next
	return nil
}

func (r *scopedRetention) size() int {
	return len(r.documents) + len(r.versions) + len(r.grants) + len(r.admissions) + len(r.cleanup) + len(r.revocations)
}

func (r *scopedRetention) operation(id topology.OperationID) {
	if operation := r.state.indexes.operations[id]; operation != nil {
		r.versions[operation.Status.Version] = true // All attempts, never only a retry.
	}
}

func (r *scopedRetention) document(version uint64) {
	if version != 0 && r.state.indexes.documents[version].Version != 0 {
		r.documents[version] = true
	}
}

func retainedRows[T any](rows []T, keep func(T) bool) []T {
	return slices.DeleteFunc(slices.Clone(rows), func(row T) bool { return !keep(row) })
}

func (r *scopedRetention) selected() scopedState {
	next := *r.state
	next.History = retainedRows(next.History, func(doc topology.Document) bool { return r.documents[doc.Version] })
	next.Operations = retainedRows(next.Operations, func(op topology.Operation) bool {
		return r.versions[op.Status.Version]
	})
	next.AuthorityRecords = retainedRows(next.AuthorityRecords, func(row scopedAuthorityRecord) bool {
		return r.grants[row.Grant.Key]
	})
	next.Admissions = retainedRows(next.Admissions, func(row AdmissionRecord) bool {
		return r.admissions[admissionKey(row.NodeName, row.InstanceID)]
	})
	next.Cleanup = retainedRows(next.Cleanup, func(row cleanupRecord) bool { return r.cleanup[row.Identity] })
	next.Intents = retainedRows(next.Intents, func(row Intent) bool { return r.documents[row.Version] })
	next.Revocations = nil
	for i, row := range r.state.Revocations {
		if r.revocations[i] {
			next.Revocations = append(next.Revocations, row)
		}
	}
	// Allocation floors, retired IDs and authority high-water keys are permanent.
	return next
}

func (s *scopedState) pruneScopedVoterSwitches() {
	var retained []topology.VoterSwitchState
	for _, row := range s.VoterSwitches {
		if s.Target.Group(row.GroupID) == nil && s.Latest.Group(row.GroupID) == nil &&
			!slices.ContainsFunc(s.Groups, func(g groupConvergence) bool {
				return g.GroupID == row.GroupID && g.Declaration != nil
			}) {
			continue
		}
		row.Replicas = retainedRows(row.Replicas, func(change topology.RoleChangeRecord) bool {
			id := storage.Identity{ClusterID: s.Target.ClusterID, GroupID: row.GroupID,
				NodeName: change.NodeName, ReplicaID: change.ReplicaID}
			return change == row.Last || s.identityReferencedLocked(id) ||
				slices.ContainsFunc(s.Cleanup, func(debt cleanupRecord) bool { return debt.Identity == id })
		})
		retained = append(retained, row)
	}
	s.VoterSwitches = retained
}
