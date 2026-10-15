package metaengine

import (
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Validation never publishes derived indexes or mutates the candidate. Complete
// convergence/authority validation precedes live installation at Task 66.
func prepareScopedGraph(state *scopedState, rules topology.Rules) (*scopedIndexes, error) {
	if state.SchemaVersion != scopedStateSchemaVersion {
		return nil, fault.ErrUnsupported
	}
	if rules.SchemaVersion != 1 || rules.Limits.Validate() != nil || !reflect.DeepEqual(state.Rules, rules) {
		return nil, fault.New(fault.IdentityMismatch, "persisted cluster rules differ", nil)
	}
	index, err := state.buildIndexes()
	if err != nil {
		return nil, err
	}
	if err := validateScopedDocuments(state, index, rules); err != nil {
		return nil, err
	}
	if err := validateScopedOperations(state, index); err != nil {
		return nil, err
	}
	if err := validateScopedConvergence(state, index); err != nil {
		return nil, err
	}
	if err := validateScopedIdentities(state, index); err != nil {
		return nil, err
	}
	if err := validateScopedIntents(state, index); err != nil {
		return nil, err
	}
	catalog, err := restoreScopedAuthority(state, index)
	if err != nil {
		return nil, err
	}
	if err := validateScopedCleanup(state, index, catalog); err != nil {
		return nil, err
	}
	if err := validateScopedAdmissions(state, index, catalog); err != nil {
		return nil, err
	}
	if err := validateScopedCompletion(state, index, catalog); err != nil {
		return nil, err
	}
	if err := validateScopedVoterSwitches(state, index, catalog); err != nil {
		return nil, err
	}
	return index, nil
}

func invalidScopedState(message string) error {
	return fault.New(fault.Unreadable, message, nil)
}

func validateScopedDocuments(state *scopedState, index *scopedIndexes, rules topology.Rules) error {
	if state.Target.Version > state.Latest.Version || (state.AppliedIndex == 0) != (state.AppliedTerm == 0) {
		return invalidScopedState("invalid document/applied boundaries")
	}
	for _, doc := range []*topology.Document{&state.Latest, &state.Target} {
		if err := doc.Validate(rules); err != nil {
			return fault.New(fault.Unreadable, "invalid materialized document", err)
		}
		if doc.Version == 0 && (doc.ParentVersion != 0 || doc.RebasedFrom != 0 ||
			len(doc.Nodes) != 0 || len(doc.Groups) != 0) {
			return invalidScopedState("unversioned document carries declarations or ancestry")
		}
	}
	if err := validateScopedHistory(state, index, rules); err != nil {
		return err
	}
	marker := state.FullyConverged
	if marker.Version > state.Target.Version || marker.Version != 0 && index.documents[marker.Version].Version == 0 ||
		(marker.Version == 0) != marker.At.IsZero() {
		return invalidScopedState("fully converged marker lacks retained document evidence")
	}
	// Historical completion need not equal current Target or current proven rows.
	return nil
}

func validateScopedHistory(state *scopedState, index *scopedIndexes, rules topology.Rules) error {
	for _, doc := range state.History {
		if doc.Version == 0 {
			return invalidScopedState("history contains an unversioned document")
		}
	}
	for _, doc := range index.documents {
		if err := doc.Validate(rules); err != nil {
			return fault.New(fault.Unreadable, "invalid retained document", err)
		}
		if doc.ClusterID != state.Latest.ClusterID || doc.Version > state.Latest.Version ||
			doc.ParentVersion >= doc.Version {
			return invalidScopedState("invalid retained document identity or ancestry")
		}
		if doc.RebasedFrom != 0 {
			if err := validateScopedRebase(state, index, doc); err != nil {
				return err
			}
		}
	}
	return nil
}

func scopedApprovedVersion(state *scopedState, version uint64) bool {
	if version == 0 {
		return false
	}
	if version == state.Target.Version || version == state.FullyConverged.Version {
		return true
	}
	for _, row := range state.Groups {
		if row.SourceVersion == version {
			return true
		}
	}
	for _, row := range state.Nodes {
		if row.SourceVersion == version {
			return true
		}
	}
	for _, operation := range state.Operations {
		if operation.Status.Version == version || slices.ContainsFunc(operation.Plan.Blocks,
			func(block topology.PlanBlock) bool { return block.SourceVersion == version }) {
			return true
		}
	}
	return false
}

func validateScopedRebase(state *scopedState, index *scopedIndexes, doc topology.Document) error {
	source := index.documents[doc.RebasedFrom]
	if doc.Version <= 2 || source.Version == 0 || doc.RebasedFrom != doc.Version-2 ||
		doc.ParentVersion != doc.Version-1 {
		return invalidScopedState("rebase lacks exact staged source and repair ancestry")
	}
	if repair, found := index.documents[doc.ParentVersion]; found {
		if err := validateScopedRebaseBase(state, index, source, repair); err != nil {
			return err
		}
	}
	if scopedApprovedVersion(state, source.Version) || scopedActionVersion(state, source.Version) {
		return invalidScopedState("rebased staged source carries approval or action authority")
	}
	return nil
}

func scopedActionVersion(state *scopedState, version uint64) bool {
	if scopedVoterVersion(state, version) {
		return true
	}
	for _, record := range state.AuthorityRecords {
		if record.Grant.Action != nil && record.Grant.Action.Version == version {
			return true
		}
	}
	for _, debt := range state.Cleanup {
		actions := []*topology.AuthorizedStep{debt.StopAction, debt.RetireAction}
		if debt.Removed != nil {
			actions = append(actions, debt.Removed.Action)
		}
		for _, action := range actions {
			if action != nil && action.Version == version {
				return true
			}
		}
	}
	return false
}

func validateScopedRebaseBase(state *scopedState, index *scopedIndexes, source, repair topology.Document) error {
	version := source.ParentVersion
	for version > repair.ParentVersion {
		if scopedApprovedVersion(state, version) || scopedActionVersion(state, version) {
			return invalidScopedState("repair parent skips an approved staged ancestor")
		}
		doc, found := index.documents[version]
		if !found {
			return nil // Compaction retained no evidence contradicting this parent.
		}
		if doc.ParentVersion >= version {
			return invalidScopedState("staged ancestry does not decrease")
		}
		version = doc.ParentVersion
	}
	if version != repair.ParentVersion {
		return invalidScopedState("repair parent is not an ancestor of the staged source")
	}
	return nil
}
