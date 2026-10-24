package gquorum

import (
	"context"

	"github.com/asalimonov/gquorum/recovery"
)

// Approve durably accepts the exact inspected request without moving source data.
func (*Recovery) Approve(ctx context.Context, _ recovery.Request,
	_ recovery.Acceptance) (recovery.Operation, error) {
	return recovery.Operation{}, pendingRecovery(ctx, "approve")
}
