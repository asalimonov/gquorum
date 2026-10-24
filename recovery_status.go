package gquorum

import (
	"context"

	"github.com/asalimonov/gquorum/recovery"
)

// Status reads historical operation progress; an empty ID selects the current operation.
func (*Recovery) Status(ctx context.Context, _ string) (recovery.Status, error) {
	return recovery.Status{}, pendingRecovery(ctx, "status")
}

// List reads all recognized operation reports in descending acceptance order.
func (*Recovery) List(ctx context.Context) ([]recovery.Status, error) {
	return nil, pendingRecovery(ctx, "list")
}
