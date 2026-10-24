package gquorum

import (
	"context"

	"github.com/asalimonov/gquorum/recovery"
)

// Run executes or resumes one accepted operation under exclusive ownership.
func (*Recovery) Run(ctx context.Context, _ string) (recovery.Status, error) {
	return recovery.Status{}, pendingRecovery(ctx, "run")
}
