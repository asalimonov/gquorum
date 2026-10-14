// Package fault defines stable errors shared by runtime and transport adapters.
package fault

import "fmt"

// Code identifies a stable failure classification.
type Code string

// Failure codes classify validation, authorization, availability and lifecycle
// errors.
const (
	Invalid               Code = "invalid"
	VersionConflict       Code = "version_conflict"
	HistoryUnavailable    Code = "history_unavailable"
	NotLeader             Code = "not_leader"
	ReplicaNotReady       Code = "replica_not_ready"
	Unavailable           Code = "unavailable"
	NoController          Code = "no_controller"
	UnsafeMembership      Code = "unsafe_membership"
	OperatorAuthorization Code = "execution_authorization"
	AuthorityFenced       Code = "authority_fenced"
	AuthorityPending      Code = "authority_pending"
	ReplanRequired        Code = "replan_required"
	IdentityMismatch      Code = "identity_mismatch"
	Unreadable            Code = "unreadable"
	GroupRetired          Code = "group_retired"
	Overloaded            Code = "overloaded"
	Stopped               Code = "stopped"
	ShutdownTimeout       Code = "shutdown_timeout"
	Unsupported           Code = "unsupported"
	ReadmissionRequired   Code = "readmission_required"
	// Conflict is a local classification for a desired state that differs from
	// existing or pending topology. It is never sent on the wire.
	Conflict Code = "conflict"
)

// Error carries a classified failure, optional cause and version-conflict
// details.
type Error struct {
	Code            Code
	Message         string
	Cause           error
	ExpectedVersion uint64
	CurrentVersion  uint64
}

func (e *Error) Error() string {
	if e.Message == "" {
		return string(e.Code)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}
func (e *Error) Unwrap() error { return e.Cause }

// Is matches classified errors by code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && e.Code == t.Code
}

// New constructs a classified error with an optional underlying cause.
func New(code Code, message string, cause error) *Error {
	return &Error{Code: code, Message: message, Cause: cause}
}

// Sentinel errors support code-based matching with errors.Is.
var (
	ErrInvalid               = New(Invalid, "", nil)
	ErrVersionConflict       = New(VersionConflict, "", nil)
	ErrHistoryUnavailable    = New(HistoryUnavailable, "", nil)
	ErrNotLeader             = New(NotLeader, "", nil)
	ErrReplicaNotReady       = New(ReplicaNotReady, "", nil)
	ErrUnavailable           = New(Unavailable, "", nil)
	ErrNoController          = New(NoController, "", nil)
	ErrUnsafeMembership      = New(UnsafeMembership, "", nil)
	ErrOperatorAuthorization = New(OperatorAuthorization, "", nil)
	ErrAuthorityFenced       = New(AuthorityFenced, "", nil)
	ErrAuthorityPending      = New(AuthorityPending, "", nil)
	ErrReplanRequired        = New(ReplanRequired, "", nil)
	ErrIdentityMismatch      = New(IdentityMismatch, "", nil)
	ErrUnreadable            = New(Unreadable, "", nil)
	ErrGroupRetired          = New(GroupRetired, "", nil)
	ErrOverloaded            = New(Overloaded, "", nil)
	ErrStopped               = New(Stopped, "", nil)
	ErrShutdownTimeout       = New(ShutdownTimeout, "", nil)
	ErrUnsupported           = New(Unsupported, "", nil)
	ErrReadmissionRequired   = New(ReadmissionRequired, "", nil)
	ErrConflict              = New(Conflict, "", nil)
)
