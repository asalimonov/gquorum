package recovery

import (
	"math"
	"net"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
)

// StartOptions contains only values needed to validate a startup transformation.
type StartOptions struct {
	Identity         topology.Identity
	Node             topology.Node
	AdvertiseAddress string
	JoinSeeds        []string
}

func invalidRecoveryStart(message string) error {
	return recoveryRefusal("invalid_start_options", message, fault.ErrInvalid)
}

// ValidateStart checks the reported phase, identity and bootstrap inputs without I/O.
func ValidateStart(status recovery.Status, opts StartOptions) error {
	identity := status.Identity
	if identity.NodeName == "" || opts.Node.Type == "" ||
		identity.NodeName != opts.Identity.NodeName || identity.NodeName != opts.Node.Name {
		return invalidRecoveryStart("reported identity must match the local declaration")
	}
	if strings.TrimSpace(identity.ClusterID) == "" || strings.TrimSpace(opts.Identity.InstanceID) == "" {
		return invalidRecoveryStart("destination and a new process identity are required")
	}
	if opts.Node.State != "" && opts.Node.State != topology.NodeActive {
		return invalidRecoveryStart("recovery startup requires an active local declaration")
	}
	switch status.Mode {
	case recovery.Rebuild:
		return validateRebuildStart(status, opts)
	case recovery.ResetForJoin:
		return validateResetStart(status, opts)
	default:
		return invalidRecoveryStart("unknown recovery mode")
	}
}

func validateRebuildStart(status recovery.Status, opts StartOptions) error {
	if status.Phase != recovery.AwaitingStart && status.Phase != recovery.Importing &&
		status.Phase != recovery.Validating {
		return invalidRecoveryStart("rebuild is not at a startable phase")
	}
	if status.Identity.MemberID == 0 || status.Identity.MemberID == math.MaxUint64 {
		return invalidRecoveryStart("rebuild requires its reported positive founding member")
	}
	if _, _, err := net.SplitHostPort(opts.AdvertiseAddress); err != nil {
		return invalidRecoveryStart("rebuild requires a configured advertised metadata address")
	}
	return nil
}

func validateResetStart(status recovery.Status, opts StartOptions) error {
	if status.Phase != recovery.JoinReady || status.Identity.MemberID != 0 || len(opts.JoinSeeds) == 0 {
		return invalidRecoveryStart("reset requires JoinReady, fresh admission and configured join seeds")
	}
	for _, seed := range opts.JoinSeeds {
		if _, _, err := net.SplitHostPort(seed); err != nil {
			return invalidRecoveryStart("invalid recovery join seed")
		}
	}
	return nil
}
