package gquorum

import (
	"context"

	"github.com/asalimonov/gquorum/recovery"
)

// Cancel undoes application preparation only before library archiving begins.
func (*Recovery) Cancel(ctx context.Context, _, _, _ string) (recovery.Status, error) {
	return recovery.Status{}, pendingRecovery(ctx, "cancel")
}
