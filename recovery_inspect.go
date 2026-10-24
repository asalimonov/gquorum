package gquorum

import (
	"context"

	"github.com/asalimonov/gquorum/recovery"
)

// Inspect reads source evidence and builds a reviewable recovery report.
func (*Recovery) Inspect(ctx context.Context, _ recovery.Request) (recovery.Plan, error) {
	return recovery.Plan{}, pendingRecovery(ctx, "inspect")
}
