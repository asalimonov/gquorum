package fault

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

const (
	defaultAttemptTimeout = 3 * time.Second
	defaultInitialBackoff = 100 * time.Millisecond
)

// Retryable reports transient conditions where repeating the same request may
// succeed: leadership changes, replicas that are not ready yet, unavailable or
// overloaded services, a missing controller, and an expired attempt deadline.
// Caller cancellation is not retryable.
func Retryable(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrNotLeader) ||
		errors.Is(err, ErrReplicaNotReady) ||
		errors.Is(err, ErrUnavailable) || errors.Is(err, ErrOverloaded) || errors.Is(err, ErrNoController)
}

// RetryOptions bounds Retry. Zero fields select the defaults: three-second
// attempts and backoff from 100 milliseconds up to one second.
type RetryOptions struct {
	AttemptTimeout time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

func (o RetryOptions) resolved() RetryOptions {
	if o.AttemptTimeout <= 0 {
		o.AttemptTimeout = defaultAttemptTimeout
	}
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = defaultInitialBackoff
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = time.Second
	}
	o.MaxBackoff = max(o.MaxBackoff, o.InitialBackoff)
	return o
}

// Retry calls call until it succeeds, returns an error that is not Retryable,
// or
// ctx ends. Each attempt receives a child context bounded by AttemptTimeout.
// After caller cancellation Retry returns ctx.Err(). Retrying does not undo an
// earlier attempt: a timed-out write may still have taken effect, so callers
// retry only idempotent or compare-and-swap operations.
func Retry[T any](ctx context.Context, o RetryOptions, call func(context.Context) (T, error)) (T, error) {
	settings := o.resolved()
	delay := settings.InitialBackoff
	for {
		attempt, cancel := context.WithTimeout(ctx, settings.AttemptTimeout)
		value, err := call(attempt)
		cancel()
		if err == nil {
			return value, nil
		}
		var zero T
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		if !Retryable(err) {
			return zero, err
		}
		//nolint:gosec // Jitter only spreads retries; it does not make a security decision.
		wait := min(delay+time.Duration(rand.Int64N(int64(delay/4)+1)), settings.MaxBackoff)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, settings.MaxBackoff)
	}
}
