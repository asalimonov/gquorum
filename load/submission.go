package load

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
)

// SubmissionDisposition describes what is known about one proposal attempt.
// An unrecognized value must be treated as indeterminate.
type SubmissionDisposition uint8

// Submission dispositions distinguish observed application from a proven
// refusal and an outcome for which the caller has no conclusive evidence.
const (
	SubmissionIndeterminate SubmissionDisposition = iota
	SubmissionNotSubmitted
	SubmissionApplied
)

// Submission describes one attempt at an exact replica. It is an observation,
// not an authorization or a guarantee about other calls with the same RequestID.
// A zero value is indeterminate. Application deduplication remains caller-owned.
type Submission struct {
	// AttemptID is an opaque library correlation, distinct from RequestID.
	AttemptID string
	// RequestID is the unchanged caller-owned logical operation identity.
	RequestID string
	// Identity and RunID identify the targeted replica and engine incarnation
	// when known. They never redirect a stopped handle into its successor.
	Identity    storage.Identity
	RunID       string
	Disposition SubmissionDisposition
	// Index and Term are meaningful only for Applied and come from the entry
	// actually applied, not from a status observation.
	Index uint64
	Term  uint64
	// Result contains the complete business response only when ResultAvailable
	// is true. Applied does not imply business acceptance; inspect Result.Code.
	Result          Result
	ResultAvailable bool
}

// ErrResultUnavailable means application is known but its complete business
// result is unavailable. Inspect the Submission even when an error is returned.
var ErrResultUnavailable = errors.New("proposal result unavailable")

// CanResubmit reports whether the library's conservative policy permits another
// attempt: this attempt was proven not submitted and its failure is transient.
// Terminal classifications dominate transient wrapped causes. The caller must
// still check its context and retry budget. This does not prove that a logical
// operation never executed in a separate or concurrent call.
func (s Submission) CanResubmit(err error) bool {
	if s.Disposition != SubmissionNotSubmitted {
		return false
	}
	for _, terminal := range []error{
		fault.ErrStopped, fault.ErrInvalid, fault.ErrUnreadable,
		fault.ErrIdentityMismatch, fault.ErrUnsupported, context.Canceled,
	} {
		if errors.Is(err, terminal) {
			return false
		}
	}
	return fault.Retryable(err)
}
