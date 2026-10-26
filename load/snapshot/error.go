package snapshot

import (
	"fmt"

	"github.com/asalimonov/gquorum/fault"
)

// Stable transfer reasons distinguish permanent refusals from transient pressure.
const (
	ReasonSizeLimit        = "size_limit"
	ReasonFrameLimit       = "frame_limit"
	ReasonCapacity         = "capacity"
	ReasonGroupBusy        = "group_busy"
	ReasonUnsupported      = "unsupported"
	ReasonMalformed        = "malformed"
	ReasonChecksum         = "checksum"
	ReasonIdentity         = "identity"
	ReasonTransferConflict = "transfer_conflict"
	ReasonRestartRequired  = "restart_required"
	ReasonObsolete         = "obsolete"
	ReasonProgressTimeout  = "progress_timeout"
	ReasonTotalTimeout     = "total_timeout"
	ReasonStopped          = "stopped"
	ReasonIngressFailed    = "ingress_failed"
)

// TransferError preserves the violated size dimension and classified cause.
// Size and Limit are bytes, or zero when not applicable. Unknown reasons unwrap
// as Unavailable, never as successful delivery or proven installation.
type TransferError struct {
	Reason      string
	Size, Limit uint64
	Cause       error
}

func (e *TransferError) Error() string {
	message := "snapshot transfer: " + e.Reason
	if e.Size != 0 || e.Limit != 0 {
		message += fmt.Sprintf(" (size=%d limit=%d)", e.Size, e.Limit)
	}
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

// Unwrap preserves Cause below the reason's stable fault classification.
func (e *TransferError) Unwrap() error {
	var code fault.Code
	switch e.Reason {
	case ReasonSizeLimit, ReasonFrameLimit, ReasonMalformed, ReasonTransferConflict:
		code = fault.Invalid
	case ReasonUnsupported:
		code = fault.Unsupported
	case ReasonCapacity, ReasonGroupBusy:
		code = fault.Overloaded
	case ReasonIdentity:
		code = fault.IdentityMismatch
	case ReasonChecksum:
		code = fault.Unreadable
	case ReasonStopped:
		code = fault.Stopped
	default:
		code = fault.Unavailable
	}
	return fault.New(code, "snapshot transfer: "+e.Reason, e.Cause)
}
