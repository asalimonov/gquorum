package metaengine

import (
	"reflect"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Validation never publishes derived indexes or mutates the candidate. Complete
// convergence/authority validation precedes live installation at Task 58.
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
	if _, err := restoreScopedAuthority(state, index); err != nil {
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
		if doc.RebasedFrom != 0 && (index.documents[doc.RebasedFrom].Version == 0 ||
			doc.RebasedFrom >= doc.ParentVersion) {
			return invalidScopedState("rebase lacks its original staged source or repair parent")
		}
	}
	return nil
}
