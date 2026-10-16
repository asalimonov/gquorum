package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

const cleanupAuthorityClass = "cleanup"

// Reuse immutable grant/evidence validation without legacy Active, whole-plan
// completion or the old global debt limit. Scoped completion is checked separately.
func restoreScopedAuthority(state *scopedState, index *scopedIndexes) (authorityCatalog, error) {
	restore, err := restoreScopedAuthorityRecords(state)
	if err != nil {
		return authorityCatalog{}, err
	}
	if err := restore.restoreFloors(); err != nil {
		return authorityCatalog{}, err
	}
	if err := restore.catalog.validateFencePredecessors(); err != nil {
		return authorityCatalog{}, err
	}
	if !validAuthorityBoundaries(restore.catalog) {
		return authorityCatalog{}, invalidAuthorityCatalog()
	}
	for _, record := range state.AuthorityRecords {
		if err := validateScopedGrant(record, index, restore.catalog); err != nil {
			return authorityCatalog{}, err
		}
		if record.Class == cleanupAuthorityClass && !validCleanupRevisions(record.Grant, restore.catalog) {
			return authorityCatalog{}, invalidScopedState("cleanup revision preceded reconciliation")
		}
	}
	for _, record := range state.Revocations {
		if err := validateScopedRevocation(state, record, index, restore.catalog); err != nil {
			return authorityCatalog{}, err
		}
	}
	return restore.catalog, nil
}

func validCleanupRevisions(grant topology.AuthorityGrant, catalog authorityCatalog) bool {
	for _, older := range catalog.records {
		if older.Grant.Action == nil || *older.Grant.Action != *grant.Action ||
			older.Grant.Key.Revision >= grant.Key.Revision {
			continue
		}
		for _, target := range grant.Targets {
			resource := topology.AuthorityResource{Kind: topology.AuthorityHost,
				GroupID: grant.Key.GroupID, NodeName: target.NodeName}
			if slices.Contains(older.Grant.Targets, target) && !catalog.resourceSettled(older, resource) {
				return false
			}
		}
	}
	return true
}

func validateScopedGrant(record scopedAuthorityRecord, index *scopedIndexes, catalog authorityCatalog) error {
	grant := record.Grant
	if grant.Purpose != topology.AuthorityEffect {
		if record.Class != "execution" {
			return invalidScopedState("non-effect authority cannot be detached cleanup")
		}
		return nil
	}
	action := *grant.Action // Grant.Validate has already required it.
	operation := index.operations[topology.OperationID(action.OperatorID)]
	if operation != nil {
		return validateRetainedScopedGrant(record, operation, index)
	}
	// An archived execution grant supplies historical evidence only after settlement.
	// Unfinished execution still requires its immutable operation/plan.
	if record.Class != "execution" || !catalog.recordSettled(&record.authorityRecord) {
		return invalidScopedState("unfinished authority lost its execution owner")
	}
	return nil
}

func validateRetainedScopedGrant(record scopedAuthorityRecord,
	operation *topology.Operation, index *scopedIndexes) error {
	action := *record.Grant.Action
	if action.StepSeq > len(operation.Plan.Steps) || !action.Matches(topology.OperatorID(operation.Status.ID),
		operation.Status.Attempt, operation.Status.Version, operation.Plan.Steps[action.StepSeq-1]) {
		return invalidScopedState("authority action differs from its retained immutable plan")
	}
	want := "execution"
	for _, block := range operation.Plan.Blocks {
		if slices.Contains(block.Cleanup, action.StepSeq) {
			want = cleanupAuthorityClass
		}
	}
	if record.Class != want {
		return invalidScopedState("authority class contradicts its immutable partition")
	}
	if want == cleanupAuthorityClass {
		return validateCleanupGrantDebt(record.Grant, index)
	}
	return nil
}

func validateCleanupGrantDebt(grant topology.AuthorityGrant, index *scopedIndexes) error {
	kind := grant.Action.Kind
	if kind != topology.StepStopReplica && kind != topology.StepCleanupReplicaState &&
		kind != topology.StepDeleteGroupState {
		return invalidScopedState("cleanup authority contains a native/preparation action")
	}
	for _, target := range grant.Targets {
		id := storage.Identity{ClusterID: grant.Key.ClusterID, GroupID: grant.Key.GroupID,
			NodeName: target.NodeName, ReplicaID: target.ReplicaID}
		debt := index.cleanup[id]
		if debt == nil || debt.MemberID != target.MemberID || !cleanupRetainsAction(debt, *grant.Action) {
			return invalidScopedState("cleanup authority lost exact retained debt")
		}
	}
	return nil
}

func cleanupRetainsAction(debt *cleanupRecord, action topology.AuthorizedStep) bool {
	for _, retained := range []*topology.AuthorizedStep{debt.StopAction, debt.RetireAction} {
		if retained != nil && *retained == action {
			return true
		}
	}
	bound := debt.StopAction
	// Earlier cleanup stops remain historical authority after a required/deferred
	// rebind. Execution still resolves only the debt's current exact action.
	return bound != nil && debt.RetireAction != nil && action.Kind == topology.StepStopReplica &&
		action.GroupID == debt.Identity.GroupID && action.NodeName == debt.Identity.NodeName &&
		action.ReplicaID == debt.Identity.ReplicaID && action.Version >= debt.RetireAction.Version &&
		(action.Version < bound.Version || action.Version == bound.Version && action.Attempt < bound.Attempt)
}

func validateScopedRevocation(state *scopedState, record scopedRevocationRecord,
	index *scopedIndexes, catalog authorityCatalog) error {
	if record.OperationID == "" || topology.ValidateID(record.GroupID) != nil {
		return invalidScopedState("revocation lacks its exact operation/group scope")
	}
	containsGroup := func(block topology.PlanBlock) bool { return block.GroupID == record.GroupID }
	if operation := index.operations[record.OperationID]; operation != nil &&
		!slices.ContainsFunc(operation.Plan.Blocks, containsGroup) && !state.readmissionRevocationScope(record) {
		return invalidScopedState("revocation names a group outside its operation")
	}
	for i, obligation := range record.Obligations {
		if obligation.Key.GroupID != record.GroupID || obligation.Resource.GroupID != record.GroupID ||
			i > 0 && compareFenceObligations(record.Obligations[i-1], obligation) >= 0 {
			return invalidScopedState("revocation obligations require sorted unique exact group resources")
		}
	}
	return catalog.validateRestoredObligations(record.Obligations)
}

func (s *scopedState) readmissionRevocationScope(record scopedRevocationRecord) bool {
	return slices.ContainsFunc(s.AuthorityRecords, func(r scopedAuthorityRecord) bool {
		return r.Grant.Purpose == topology.AuthorityReadmit && r.Grant.Key.GroupID == record.GroupID &&
			s.readmissionOwner(r.Grant.AdmissionID) == record.OperationID
	})
}

func restoreScopedAuthorityRecords(state *scopedState) (authorityRestore, error) {
	restore := authorityRestore{state: smState{Latest: state.Latest, AppliedIndex: state.AppliedIndex,
		Admissions: state.Admissions, AuthorityFloors: state.AuthorityFloors}, catalog: newAuthorityCatalog(),
		requests: map[string]bool{}, readmissionRevisions: map[string]uint64{},
		readmissionMembers: map[string]uint64{}, readmissionMeta: map[string]bool{},
		cleanupActions: make(map[topology.AuthorizedStep]bool)}
	for _, record := range state.AuthorityRecords {
		if record.Class == cleanupAuthorityClass && record.Grant.Action != nil {
			restore.cleanupActions[*record.Grant.Action] = true
		}
	}
	for _, record := range state.AuthorityRecords {
		if err := restore.restoreRecord(record.authorityRecord); err != nil {
			return authorityRestore{}, err
		}
	}
	for id := range restore.readmissionMembers {
		if !restore.readmissionMeta[id] {
			return authorityRestore{}, invalidAuthorityCatalog()
		}
	}
	return restore, nil
}
