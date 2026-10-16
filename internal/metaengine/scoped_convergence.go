package metaengine

import (
	"reflect"
	"strings"

	"github.com/asalimonov/gquorum/topology"
)

func validateScopedConvergence(state *scopedState, index *scopedIndexes) error {
	retired := make(map[string]uint64, len(state.RetiredGroups))
	for _, row := range state.RetiredGroups {
		if row.Version > state.Target.Version {
			return invalidScopedState("retirement is newer than approved intent")
		}
		retired[row.ID] = row.Version
	}
	for _, row := range state.Groups {
		if err := validateScopedGroupRow(state, index, retired, row); err != nil {
			return err
		}
	}
	if err := validateScopedOwners(state, index); err != nil {
		return err
	}
	return validateNodeCheckpoints(state, index)
}

func ownsScopedGroup(operation *topology.Operation, group string, source uint64) bool {
	for i, block := range operation.Status.Blocks {
		if block.GroupID == group {
			return operation.Plan.Blocks[i].SourceVersion == source && !block.State.Terminal()
		}
	}
	return false
}

func validateGroupCheckpoint(state *scopedState, row groupConvergence,
	index *scopedIndexes, retired map[string]uint64) error {
	if row.SourceVersion == 0 {
		if row.Declaration != nil {
			return invalidScopedState("unproven group carries a declaration")
		}
		return nil
	}
	if source, found := index.documents[row.SourceVersion]; found &&
		!reflect.DeepEqual(source.Group(row.GroupID), row.Declaration) {
		return invalidScopedState("group checkpoint contradicts its retained source")
	}
	if row.Declaration == nil {
		if row.Deleting || retired[row.GroupID] == 0 || retired[row.GroupID] > row.SourceVersion {
			return invalidScopedState("group tombstone lacks permanent retirement")
		}
		return nil
	}
	if retired[row.GroupID] != 0 && row.SourceVersion >= retired[row.GroupID] {
		return invalidScopedState("live checkpoint follows permanent group retirement")
	}
	if row.Declaration.ID != row.GroupID {
		return invalidScopedState("group checkpoint names a different declaration")
	}
	if _, found := index.documents[row.SourceVersion]; found {
		return nil
	}
	return validateCompactedGroup(state, index, row)
}

func validateCompactedGroup(state *scopedState, index *scopedIndexes, row groupConvergence) error {
	// A compacted source is safe when another approved retained document supplies
	// the identical declaration. Otherwise keep the original verification context.
	if _, found := checkpointDocument(state, index, row); found {
		return nil
	}
	return invalidScopedState("group checkpoint lacks independently valid declaration evidence")
}

func validateNodeCheckpoints(state *scopedState, index *scopedIndexes) error {
	for _, row := range state.Nodes {
		if row.SourceVersion > state.Target.Version || strings.TrimSpace(row.NodeName) == "" ||
			strings.ContainsRune(row.NodeName, 0) || (row.SourceVersion == 0) != row.CompletedAt.IsZero() {
			return invalidScopedState("invalid node checkpoint identity or source")
		}
		if row.SourceVersion == 0 && (row.Declaration != nil || row.MemberID != 0) ||
			row.Declaration == nil && row.MemberID != 0 {
			return invalidScopedState("node checkpoint lacks exact proven membership")
		}
		if err := validateNodeDeclaration(state, row, index); err != nil {
			return err
		}
		if err := validateNodeOwner(row, index); err != nil {
			return err
		}
	}
	return nil
}

func validateNodeDeclaration(state *scopedState, row nodeConvergence, index *scopedIndexes) error {
	if row.SourceVersion == 0 {
		return nil
	}
	if source, found := index.documents[row.SourceVersion]; found {
		if !reflect.DeepEqual(source.Node(row.NodeName), row.Declaration) ||
			source.MemberID(row.NodeName) != row.MemberID {
			return invalidScopedState("node checkpoint contradicts its retained source")
		}
		return nil
	}
	// Proven absence has no current membership; old effects retain their exact identity.
	if row.Declaration == nil {
		return nil
	}
	for _, doc := range index.documents {
		if doc.MemberID(row.NodeName) == row.MemberID && scopedApprovedVersion(state, doc.Version) &&
			reflect.DeepEqual(doc.Node(row.NodeName), row.Declaration) {
			return nil
		}
	}
	return invalidScopedState("node checkpoint lacks independently valid declaration evidence")
}

func validateScopedOwners(state *scopedState, index *scopedIndexes) error {
	for _, group := range state.Target.Groups {
		if index.groups[group.ID] == nil {
			return invalidScopedState("approved group lacks a convergence record")
		}
	}
	for _, node := range state.Target.Nodes {
		if index.nodes[node.Name] == nil {
			return invalidScopedState("approved node lacks a convergence record")
		}
	}
	for _, operation := range state.Operations {
		if err := validateOperationOwner(operation, index); err != nil {
			return err
		}
	}
	return nil
}

func validateScopedGroupRow(state *scopedState, index *scopedIndexes,
	retired map[string]uint64, row groupConvergence) error {
	if row.SourceVersion > state.Target.Version || topology.ValidateID(row.GroupID) != nil ||
		(row.SourceVersion == 0) != row.CompletedAt.IsZero() {
		return invalidScopedState("invalid group checkpoint identity or source")
	}
	if row.Deleting && state.Target.Group(row.GroupID) != nil ||
		!row.Deleting && row.Declaration != nil && state.Target.Group(row.GroupID) == nil {
		return invalidScopedState("deletion state contradicts current approved presence")
	}
	if err := validateGroupCheckpoint(state, row, index, retired); err != nil {
		return err
	}
	if row.Owner != "" {
		op := index.operations[row.Owner]
		if op == nil || !ownsScopedGroup(op, row.GroupID, row.SourceVersion) {
			return invalidScopedState("group owner lacks its exact nonterminal block")
		}
		approved := index.documents[op.Status.Version]
		if row.Deleting != (approved.Group(row.GroupID) == nil) {
			return invalidScopedState("deletion flag contradicts owning approval")
		}
	}
	return nil
}

func validateNodeOwner(row nodeConvergence, index *scopedIndexes) error {
	if row.Owner == "" {
		return nil
	}
	op := index.operations[row.Owner]
	if op == nil {
		return invalidScopedState("node owner is missing")
	}
	if row.SourceVersion >= op.Status.Version || len(op.Plan.Blocks) == 0 {
		return invalidScopedState("node owner lacks pending declaration scope")
	}
	if op.Plan.Blocks[0].GroupID != topology.MetaGroupID || op.Status.State.Terminal() {
		return invalidScopedState("node owner lacks its retained admission/declaration scope")
	}
	return nil
}

func validateOperationOwner(operation topology.Operation, index *scopedIndexes) error {
	for i, block := range operation.Status.Blocks {
		row := index.groups[block.GroupID]
		if block.State == topology.OperationSucceeded &&
			(row == nil || row.SourceVersion < operation.Status.Version) {
			return invalidScopedState("successful block lost its convergence checkpoint")
		}
		if block.State.Terminal() {
			continue
		}
		scope := ScopeExpectation{GroupID: block.GroupID, SourceVersion: operation.Plan.Blocks[i].SourceVersion,
			Owner: operation.Status.ID}
		if !scope.matches(row) {
			return invalidScopedState("nonterminal block lacks exclusive group ownership")
		}
	}
	return nil
}
