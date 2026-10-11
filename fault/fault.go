// Package fault defines stable errors shared by runtime and transport adapters.
package fault

import "fmt"

type Code string

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
	IdentityMismatch      Code = "identity_mismatch"
	Unreadable            Code = "unreadable"
	GroupRetired          Code = "group_retired"
	Overloaded            Code = "overloaded"
	Stopped               Code = "stopped"
	ShutdownTimeout       Code = "shutdown_timeout"
	Unsupported           Code = "unsupported"
	ReadmissionRequired   Code = "readmission_required"
)

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
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && e.Code == t.Code
}
func New(code Code, message string, cause error) *Error {
	return &Error{Code: code, Message: message, Cause: cause}
}

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
	ErrIdentityMismatch      = New(IdentityMismatch, "", nil)
	ErrUnreadable            = New(Unreadable, "", nil)
	ErrGroupRetired          = New(GroupRetired, "", nil)
	ErrOverloaded            = New(Overloaded, "", nil)
	ErrStopped               = New(Stopped, "", nil)
	ErrShutdownTimeout       = New(ShutdownTimeout, "", nil)
	ErrUnsupported           = New(Unsupported, "", nil)
	ErrReadmissionRequired   = New(ReadmissionRequired, "", nil)
)
