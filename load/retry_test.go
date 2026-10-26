//go:build !functional

package load_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
)

func retrySubmission(id string, attempt int, d load.SubmissionDisposition) load.Submission {
	s := load.Submission{RequestID: id, AttemptID: fmt.Sprint(attempt), Disposition: d}
	if d == load.SubmissionApplied {
		s.Identity = storage.Identity{ClusterID: "c", NodeName: "n", GroupID: "g", ReplicaID: 1}
		s.RunID, s.Index, s.Term, s.ResultAvailable = "run", uint64(attempt), 2, true
		s.Result = load.Result{Code: "ok"}
	}
	return s
}

func fastProposalRetry() load.ProposalRetryOptions {
	return load.ProposalRetryOptions{InitialBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond}
}

func TestProposeRetryResubmitsExactProposal(t *testing.T) {
	t.Parallel()
	p := load.Proposal{RequestID: "create/1", Data: []byte("payload")}
	ctx := context.Background()
	calls := 0
	propose := func(call context.Context, input load.Proposal) (load.Submission, error) {
		calls++
		if call != ctx {
			t.Fatal("helper changed the caller context")
		}
		if _, ok := call.Deadline(); ok {
			t.Fatal("helper added an attempt deadline")
		}
		if input.RequestID != p.RequestID || !bytes.Equal(input.Data, []byte("payload")) {
			t.Fatal("proposal changed between attempts", input)
		}
		input.Data[0] = 'x'
		if calls < 3 {
			return retrySubmission(input.RequestID, calls, load.SubmissionNotSubmitted), fault.ErrUnavailable
		}
		return retrySubmission(input.RequestID, calls, load.SubmissionApplied), nil
	}
	out, err := load.ProposeRetry(ctx, propose, p, fastProposalRetry())
	if err != nil || out.Result.Code != "ok" || calls != 3 || out.AttemptID != "3" || string(p.Data) != "payload" {
		t.Fatal(out, err, calls, p)
	}
}

func TestProposeRetryStopsUnknownAndApplied(t *testing.T) {
	t.Parallel()
	for _, d := range []load.SubmissionDisposition{load.SubmissionIndeterminate, load.SubmissionApplied} {
		for _, available := range []bool{false, true} {
			calls := 0
			out, err := load.ProposeRetry(t.Context(), func(_ context.Context, p load.Proposal) (load.Submission, error) {
				calls++
				s := retrySubmission(p.RequestID, calls, d)
				if d == load.SubmissionApplied {
					s.ResultAvailable = available
					if !available {
						s.Result = load.Result{}
					}
				}
				return s, context.DeadlineExceeded
			}, load.Proposal{RequestID: "stable"}, fastProposalRetry())
			if calls != 1 || out.Disposition != d || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("uncertainty/application retried", out, err, calls)
			}
		}
	}
}

func TestProposeRetryPreflightAndLimits(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"", " \t", "bad\x00id", string([]byte{0xff})} {
		calls := 0
		out, err := load.ProposeRetry(t.Context(), func(context.Context, load.Proposal) (load.Submission, error) {
			calls++
			return load.Submission{}, nil
		}, load.Proposal{RequestID: id}, fastProposalRetry())
		if calls != 0 || !errors.Is(err, fault.ErrInvalid) || out.Disposition != load.SubmissionNotSubmitted || out.AttemptID != "" {
			t.Fatal(out, err, calls)
		}
	}
	for _, options := range []load.ProposalRetryOptions{{MaxAttempts: -1}, {InitialBackoff: -1}, {MaxBackoff: -1}} {
		if _, err := load.ProposeRetry(t.Context(), nil, load.Proposal{RequestID: "id"}, options); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(err)
		}
	}
	if _, err := load.ProposeRetry(t.Context(), nil, load.Proposal{RequestID: "id"}, fastProposalRetry()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	for _, limit := range []int{0, 1, 4} {
		calls := 0
		o := fastProposalRetry()
		o.MaxAttempts = limit
		out, err := load.ProposeRetry(t.Context(), func(_ context.Context, p load.Proposal) (load.Submission, error) {
			calls++
			return retrySubmission(p.RequestID, calls, load.SubmissionNotSubmitted), fault.ErrOverloaded
		}, load.Proposal{RequestID: "id"}, o)
		want := limit
		if want == 0 {
			want = 3
		}
		if calls != want || !errors.Is(err, fault.ErrOverloaded) || out.AttemptID != fmt.Sprint(want) {
			t.Fatal(out, err, calls, want)
		}
	}
}

func TestProposeRetryConservativeCallbackEvidence(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		s           load.Submission
		err         error
		disposition load.SubmissionDisposition
		want        error
	}{
		{"ordinary-error", load.Submission{}, fault.ErrUnavailable, load.SubmissionIndeterminate, fault.ErrUnavailable},
		{"missing-success", load.Submission{}, nil, load.SubmissionIndeterminate, fault.ErrUnreadable},
		{"wrong-id", retrySubmission("other", 1, load.SubmissionApplied), nil, load.SubmissionIndeterminate, fault.ErrIdentityMismatch},
		{"unknown-number", retrySubmission("id", 1, 255), fault.ErrUnavailable, load.SubmissionIndeterminate, fault.ErrUnreadable},
		{"refusal-with-nil", retrySubmission("id", 1, load.SubmissionNotSubmitted), nil, load.SubmissionIndeterminate, fault.ErrUnreadable},
	}
	applied := retrySubmission("id", 1, load.SubmissionApplied)
	applied.ResultAvailable, applied.Result = false, load.Result{}
	cases = append(cases, struct {
		name        string
		s           load.Submission
		err         error
		disposition load.SubmissionDisposition
		want        error
	}{
		"applied-no-result", applied, nil, load.SubmissionApplied, load.ErrResultUnavailable})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			out, err := load.ProposeRetry(t.Context(), func(context.Context, load.Proposal) (load.Submission, error) {
				calls++
				return tc.s, tc.err
			}, load.Proposal{RequestID: "id"}, fastProposalRetry())
			if calls != 1 || out.Disposition != tc.disposition || !errors.Is(err, tc.want) || out.RequestID != "id" {
				t.Fatal(out, err, calls)
			}
		})
	}
}

func TestProposeRetryCancellationPreservesLastReceipt(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	called := make(chan struct{})
	done := make(chan struct{})
	var out load.Submission
	var err error
	go func() {
		defer close(done)
		out, err = load.ProposeRetry(ctx, func(_ context.Context, p load.Proposal) (load.Submission, error) {
			close(called)
			return retrySubmission(p.RequestID, 1, load.SubmissionNotSubmitted), fault.ErrUnavailable
		}, load.Proposal{RequestID: "id"}, load.ProposalRetryOptions{InitialBackoff: time.Hour})
	}()
	<-called
	cancel()
	<-done
	if !errors.Is(err, context.Canceled) || out.AttemptID != "1" || out.Disposition != load.SubmissionNotSubmitted {
		t.Fatal(out, err)
	}
	calls := 0
	out, err = load.ProposeRetry(ctx, func(context.Context, load.Proposal) (load.Submission, error) {
		calls++
		return load.Submission{}, nil
	}, load.Proposal{RequestID: "id"}, fastProposalRetry())
	if calls != 0 || out.AttemptID != "" || out.Disposition != load.SubmissionNotSubmitted || !errors.Is(err, context.Canceled) {
		t.Fatal(out, err, calls)
	}
}
