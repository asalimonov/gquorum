package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Reuse immutable grant/evidence validation without legacy Active, whole-plan
// completion or the old global debt limit. Scoped completion is checked separately.
func restoreScopedAuthority(state *scopedState, index *scopedIndexes) (authorityCatalog, error) {
	restore := authorityRestore{state: smState{Latest: state.Latest, AppliedIndex: state.AppliedIndex,
		Admissions: state.Admissions, AuthorityFloors: state.AuthorityFloors}, catalog: newAuthorityCatalog(),
		requests: map[string]bool{}, readmissionRevisions: map[string]uint64{},
		readmissionMembers: map[string]uint64{}, readmissionMeta: map[string]bool{}}
	for _, record := range state.AuthorityRecords {
		if err := restore.restoreRecord(record.authorityRecord); err != nil {
			return authorityCatalog{}, err
		}
	}
	for id := range restore.readmissionMembers {
		if !restore.readmissionMeta[id] {
			return authorityCatalog{}, invalidAuthorityCatalog()
		}
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
	}
	for _, record := range state.Revocations {
		if err := validateScopedRevocation(record, index, restore.catalog); err != nil {
			return authorityCatalog{}, err
		}
	}
	return restore.catalog, nil
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
			want = "cleanup"
		}
	}
	if record.Class != want {
		return invalidScopedState("authority class contradicts its immutable partition")
	}
	if want == "cleanup" {
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
	return false
}

func validateScopedRevocation(record scopedRevocationRecord, index *scopedIndexes, catalog authorityCatalog) error {
	if record.OperationID == "" || topology.ValidateID(record.GroupID) != nil {
		return invalidScopedState("revocation lacks its exact operation/group scope")
	}
	containsGroup := func(block topology.PlanBlock) bool { return block.GroupID == record.GroupID }
	if operation := index.operations[record.OperationID]; operation != nil &&
		!slices.ContainsFunc(operation.Plan.Blocks, containsGroup) {
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
