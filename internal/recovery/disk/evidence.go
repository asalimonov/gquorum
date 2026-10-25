package disk

import (
	"context"
	"errors"
	"os"

	"github.com/asalimonov/gquorum/fault"
)

// HasLiveEvidence preserves every unknown/partial entry as storage evidence.
// Archive directories require the system journal proof; a standalone scan keeps
// them as evidence rather than guessing that their data can be ignored.
func HasLiveEvidence(ctx context.Context, root string) (bool, error) {
	return liveEvidence(ctx, root, nil)
}

// HasLiveEvidence checks this exact root and excludes only system-validated
// history. Standalone WAL owners have no such proof and preserve all archives.
func (l *Lease) HasLiveEvidence(ctx context.Context) (bool, error) {
	if err := l.CheckRoot(); err != nil {
		return false, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false, fault.ErrStopped
	}
	if l.history != nil {
		if err := checkWALNamespace(ctx, l.path, l.history); err != nil {
			return false, err
		}
	}
	return liveEvidence(ctx, l.path, l.history)
}

func liveEvidence(ctx context.Context, root string, history map[string]bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if entry.Name() != LockName && !(history != nil && entry.Name() == Namespace) {
			return true, nil
		}
	}
	return false, nil
}
