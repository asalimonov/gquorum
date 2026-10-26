package load

import (
	"bytes"
	"context"
	mathrand "math/rand/v2"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
)

const (
	defaultProposalAttempts   = 3
	defaultProposalBackoff    = 100 * time.Millisecond
	defaultProposalMaxBackoff = time.Second
)

// ProposeFunc submits one attempt and validates any external evidence before
// returning it. Group.Propose and Leadership.Propose satisfy this contract.
// A custom callback owns logical targeting and must not hide uncertain retries.
type ProposeFunc func(context.Context, Proposal) (Submission, error)

// ProposalRetryOptions bounds sequential proven-refusal retries. Zero selects
// three attempts and backoff from 100 milliseconds up to one second. There is
// no additional attempt deadline; supply a caller deadline for an overall budget.
// The defaults need not span a leader election. These are not concurrency limits.
// TODO(not planned): a finite-deadline retry mode is a separate additive policy.
type ProposalRetryOptions struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// ProposeRetry repeats only attempts proven not submitted with transient errors.
// It always stops on Applied or Indeterminate, regardless of the error code.
// Every prior attempt was NotSubmitted, so the final attempt's receipt suffices.
// Separate calls retain their own scope; their uncertainty cannot be erased by
// this helper. Never substitute generic fault.Retry for these write semantics.
func ProposeRetry(ctx context.Context, propose ProposeFunc, p Proposal, raw ProposalRetryOptions) (Submission, error) {
	last := Submission{RequestID: p.RequestID, Disposition: SubmissionNotSubmitted}
	o, err := resolveProposalRetryOptions(raw)
	if err != nil {
		return last, err
	}
	if propose == nil {
		return last, fault.ErrInvalid
	}
	if err := validateRequestID(p.RequestID); err != nil {
		return last, err
	}
	if err := ctx.Err(); err != nil {
		return last, err
	}
	immutable := Proposal{RequestID: p.RequestID, Data: bytes.Clone(p.Data)}
	delay := o.InitialBackoff
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return last, err
		}
		s, err := propose(ctx, Proposal{RequestID: immutable.RequestID, Data: bytes.Clone(immutable.Data)})
		s, err = normalizeSubmission(immutable, s, err)
		last = s
		if s.Disposition == SubmissionApplied {
			return last, err
		}
		if ctx.Err() != nil {
			return last, ctx.Err()
		}
		if attempt+1 >= o.MaxAttempts || !s.CanResubmit(err) {
			return last, err
		}
		wait, next := proposalBackoff(delay, o.MaxBackoff)
		if err := waitProposalRetry(ctx, wait); err != nil {
			return last, err
		}
		delay = next
	}
}

func proposalBackoff(delay, maximum time.Duration) (wait, next time.Duration) {
	//nolint:gosec // Jitter spreads safe retries; it makes no security decision.
	jitter := time.Duration(mathrand.Int64N(int64(delay/4) + 1))
	wait = delay + min(jitter, maximum-delay)
	if delay >= maximum-delay {
		next = maximum
	} else {
		next = delay * 2
	}
	return wait, next
}

func resolveProposalRetryOptions(o ProposalRetryOptions) (ProposalRetryOptions, error) {
	if o.MaxAttempts < 0 || o.InitialBackoff < 0 || o.MaxBackoff < 0 {
		return o, fault.ErrInvalid
	}
	if o.MaxAttempts == 0 {
		o.MaxAttempts = defaultProposalAttempts
	}
	if o.InitialBackoff == 0 {
		o.InitialBackoff = defaultProposalBackoff
	}
	if o.MaxBackoff == 0 {
		o.MaxBackoff = defaultProposalMaxBackoff
	}
	o.MaxBackoff = max(o.MaxBackoff, o.InitialBackoff)
	return o, nil
}

func waitProposalRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func unknownSubmission(s Submission, requestID string) Submission {
	s.RequestID, s.Disposition = requestID, SubmissionIndeterminate
	s.Index, s.Term, s.ResultAvailable, s.Result = 0, 0, false, Result{}
	return s
}

func normalizeSubmission(p Proposal, s Submission, err error) (Submission, error) {
	empty := s.AttemptID == "" && s.RequestID == "" && s.Identity == (storage.Identity{}) &&
		s.RunID == "" && s.Disposition == SubmissionIndeterminate && s.Index == 0 && s.Term == 0 &&
		!s.ResultAvailable && s.Result.Code == "" && len(s.Result.Data) == 0
	if empty && err != nil {
		return Submission{RequestID: p.RequestID}, err
	}
	if empty && err == nil {
		return Submission{RequestID: p.RequestID}, fault.ErrUnreadable
	}
	if s.RequestID != p.RequestID {
		return unknownSubmission(s, p.RequestID), fault.ErrIdentityMismatch
	}
	if !consistentSubmission(s) {
		return unknownSubmission(s, p.RequestID), fault.New(fault.Unreadable, "invalid proposal receipt", err)
	}
	if s.Disposition == SubmissionApplied && !s.ResultAvailable && err == nil {
		return s, ErrResultUnavailable
	}
	if err == nil && s.Disposition != SubmissionApplied {
		return unknownSubmission(s, p.RequestID), fault.ErrUnreadable
	}
	return s, err
}

func consistentSubmission(s Submission) bool {
	if !s.ResultAvailable && (s.Result.Code != "" || len(s.Result.Data) != 0) {
		return false
	}
	switch s.Disposition {
	case SubmissionIndeterminate:
		return s.Index == 0 && s.Term == 0 && !s.ResultAvailable
	case SubmissionNotSubmitted:
		return s.AttemptID != "" && s.Index == 0 && s.Term == 0 && !s.ResultAvailable
	case SubmissionApplied:
		return s.AttemptID != "" && s.RunID != "" && s.Index != 0 && s.Term != 0 &&
			s.Identity.ClusterID != "" && s.Identity.NodeName != "" &&
			s.Identity.GroupID != "" && s.Identity.ReplicaID != 0
	default:
		return false
	}
}
