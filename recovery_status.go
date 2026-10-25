package gquorum

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	"github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/recovery"
)

// Status reads historical operation progress; an empty ID selects the current operation.
func (r *Recovery) Status(ctx context.Context, id string) (recovery.Status, error) {
	if err := ctx.Err(); err != nil {
		return recovery.Status{}, err
	}
	paths, err := r.readOnlyPaths()
	if err != nil {
		return recovery.Status{}, statusRefusal(err)
	}
	record, err := recoverydisk.Read(ctx, paths, id)
	if record.Request.RecoveryID == "" {
		return recovery.Status{}, statusRefusal(err)
	}
	return recoverymodel.StatusFromManifest(record), statusRefusal(err)
}

// List reads all recognized operation reports in descending acceptance order.
func (r *Recovery) List(ctx context.Context) ([]recovery.Status, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	paths, err := r.readOnlyPaths()
	if err != nil {
		return nil, statusRefusal(err)
	}
	records, err := recoverydisk.List(ctx, paths)
	statuses := make([]recovery.Status, 0, len(records))
	for _, record := range records {
		statuses = append(statuses, recoverymodel.StatusFromManifest(record))
	}
	return statuses, statusRefusal(err)
}
func (r *Recovery) readOnlyPaths() (recoverymodel.Paths, error) {
	root, err := filepath.Abs(r.source.Paths.StorageDir)
	if err != nil {
		return recoverymodel.Paths{}, err
	}
	// WAL mounts and current live identity are deliberately irrelevant to history.
	return recoverymodel.Paths{StorageDir: root}, nil
}
func statusRefusal(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, fault.ErrHistoryUnavailable) {
		return recoveryRefusal("operation_not_found", "recovery operation is unavailable", err)
	}
	cause := fault.New(fault.Unreadable, "invalid recovery namespace", err)
	return recoveryRefusal("invalid_namespace", "recovery history contains invalid or conflicting evidence", cause)
}
