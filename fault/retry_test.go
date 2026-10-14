//go:build !functional

package fault

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

var fastRetry = RetryOptions{AttemptTimeout: time.Second, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}

func TestRetryableClassification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{ErrNotLeader, true},
		{fmt.Errorf("wrapped: %w", ErrReplicaNotReady), true},
		{New(Unavailable, "down", nil), true},
		{ErrOverloaded, true},
		{ErrNoController, true},
		{context.DeadlineExceeded, true},
		{context.Canceled, false},
		{ErrInvalid, false},
		{ErrConflict, false},
		{errors.New("plain"), false},
	}
	for _, c := range cases {
		if got := Retryable(c.err); got != c.want {
			t.Errorf("Retryable(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

func TestRetryStopsOnSuccessOrPermanentError(t *testing.T) {
	t.Parallel()
	calls := 0
	value, err := Retry(context.Background(), fastRetry, func(context.Context) (int, error) {
		calls++
		if calls < 3 {
			return 0, ErrNotLeader
		}
		return 7, nil
	})
	if err != nil || value != 7 || calls != 3 {
		t.Fatalf("value=%d err=%v calls=%d", value, err, calls)
	}
	calls = 0
	_, err = Retry(context.Background(), fastRetry, func(context.Context) (int, error) {
		calls++
		return 0, ErrInvalid
	})
	if !errors.Is(err, ErrInvalid) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestRetryReturnsCallerCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	_, err := Retry(ctx, fastRetry, func(context.Context) (struct{}, error) {
		calls++
		if calls == 2 {
			cancel()
		}
		return struct{}{}, ErrUnavailable
	})
	if !errors.Is(err, context.Canceled) || calls != 2 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestRetryBoundsEachAttempt(t *testing.T) {
	t.Parallel()
	options := fastRetry
	options.AttemptTimeout = 5 * time.Millisecond
	calls := 0
	_, err := Retry(context.Background(), options, func(attempt context.Context) (int, error) {
		calls++
		if calls == 1 {
			<-attempt.Done()
			return 0, attempt.Err()
		}
		return 1, nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}
