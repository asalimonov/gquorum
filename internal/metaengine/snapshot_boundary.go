package metaengine

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
)

// ValidateRestoredSnapshot prevents cached application positions from suppressing
// committed metadata replay or importing metadata beyond the native snapshot.
func (m *Machine) ValidateRestoredSnapshot(ctx context.Context, index, term uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	state := m.scoped
	if state == nil {
		return fault.ErrUnreadable
	}
	// Fresh initialization and the synthetic native founding checkpoint precede
	// the first metadata command. Every later snapshot carries its exact boundary.
	if index <= 1 && state.AppliedIndex == 0 && state.AppliedTerm == 0 && state.Latest.Version == 0 &&
		state.Target.Version == 0 {
		return nil
	}
	if state.AppliedIndex != index || state.AppliedTerm != term {
		return fault.New(fault.Unreadable, "metadata snapshot contradicts native applied boundary", nil)
	}
	return nil
}
