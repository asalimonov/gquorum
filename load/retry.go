package load

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
)

// ProposeFunc submits one proposal. Group.Propose and Leadership.Propose both
// satisfy it.
type ProposeFunc func(context.Context, Proposal) (Result, error)

// ProposeRetry resubmits the exact proposal bytes and RequestID while errors
// are
// fault.Retryable and ctx is live. A timed-out attempt can still commit, so the
// command must be idempotent or guarded by compare-and-swap in the machine.
func ProposeRetry(ctx context.Context, propose ProposeFunc, p Proposal, o fault.RetryOptions) (Result, error) {
	if propose == nil {
		return Result{}, fault.ErrInvalid
	}
	return fault.Retry(ctx, o, func(attempt context.Context) (Result, error) { return propose(attempt, p) })
}
