package metaengine

// TODO(not planned): negotiate mixed-version deletion semantics or downgrade new tombstones.

// DeletionSemantics binds approval interpretation to its committed command.
type DeletionSemantics uint8

const (
	// DeletionSemanticsLegacy preserves approvals written before detached deletion.
	DeletionSemanticsLegacy DeletionSemantics = iota
	// DeletionSemanticsDetached completes deletion after retaining exact cleanup.
	DeletionSemanticsDetached
)
