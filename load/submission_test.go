//go:build !functional

package load_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
)

func TestSubmissionCanResubmitRequiresProvenRefusal(t *testing.T) {
	t.Parallel()
	transient := []error{
		fault.ErrNotLeader, fault.ErrReplicaNotReady, fault.ErrUnavailable,
		fault.ErrOverloaded, fault.ErrNoController, context.DeadlineExceeded,
	}
	for _, err := range transient {
		for _, d := range []load.SubmissionDisposition{
			load.SubmissionIndeterminate, load.SubmissionNotSubmitted,
			load.SubmissionApplied, 255,
		} {
			s := load.Submission{Disposition: d}
			got := s.CanResubmit(fmt.Errorf("wrapped: %w", err))
			if want := d == load.SubmissionNotSubmitted; got != want {
				t.Fatalf("disposition=%d error=%v: retry=%v, want %v", d, err, got, want)
			}
		}
	}
	var zero load.Submission
	if zero.Disposition != load.SubmissionIndeterminate || zero.CanResubmit(fault.ErrUnavailable) {
		t.Fatal("zero receipt is not conservative", zero)
	}
}

func TestSubmissionTerminalCauseDominatesTransientError(t *testing.T) {
	t.Parallel()
	s := load.Submission{Disposition: load.SubmissionNotSubmitted}
	for _, code := range []fault.Code{
		fault.Stopped, fault.Invalid, fault.Unreadable, fault.IdentityMismatch, fault.Unsupported,
	} {
		for _, cause := range []error{fault.ErrNotLeader, fault.ErrUnavailable, context.DeadlineExceeded} {
			err := fault.New(code, "terminal replica failure", cause)
			if !fault.Retryable(err) {
				t.Fatal("generic transient classification unexpectedly changed", err)
			}
			if s.CanResubmit(fmt.Errorf("adapter: %w", err)) {
				t.Fatal("receipt policy retried a terminal error", err)
			}
		}
	}
	if s.CanResubmit(errors.Join(fault.ErrUnavailable, context.Canceled)) {
		t.Fatal("caller cancellation did not dominate")
	}
}

func TestSubmissionPermanentFailuresAndAppliedResultsDoNotRetry(t *testing.T) {
	t.Parallel()
	refused := load.Submission{Disposition: load.SubmissionNotSubmitted}
	for _, err := range []error{nil, errors.New("ordinary failure"), fault.ErrVersionConflict,
		context.Canceled, load.ErrResultUnavailable} {
		if refused.CanResubmit(err) {
			t.Fatal("permanent failure retried", err)
		}
	}
	for _, available := range []bool{false, true} {
		applied := load.Submission{Disposition: load.SubmissionApplied, ResultAvailable: available,
			Result: load.Result{Code: "insufficient_budget"}}
		if applied.CanResubmit(context.DeadlineExceeded) || applied.CanResubmit(fault.ErrUnavailable) {
			t.Fatal("applied business rejection or delivery failure retried", applied)
		}
	}
}
