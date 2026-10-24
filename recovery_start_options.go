package gquorum

import (
	"bytes"
	"maps"
	"math"
	"net"
	"slices"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
)

// RecoveryStartOptions copies startup settings and applies the reported durable
// identity and bootstrap mode. It performs no I/O and is not startup authority.
// The caller supplies a fresh InstanceID and recreates consumed dependencies.
func RecoveryStartOptions(status recovery.Status, opts NodeOptions) (NodeOptions, error) {
	if err := validateRecoveryStart(status, opts); err != nil {
		return NodeOptions{}, err
	}
	out := cloneRecoveryStartOptions(opts)
	identity := status.Identity
	out.Identity.ClusterID, out.Identity.MemberID = identity.ClusterID, identity.MemberID
	if out.Node.State == "" {
		out.Node.State = topology.NodeActive
	}
	if status.Mode == recovery.Rebuild {
		founder := out.Node
		founder.Labels = maps.Clone(out.Node.Labels)
		out.Bootstrap = BootstrapOptions{Founders: []FoundingMember{{
			Node: founder, MemberID: identity.MemberID, Address: out.Meta.Network.AdvertiseAddress,
		}}}
	} else {
		out.Bootstrap.Join = true
		out.Bootstrap.Founders = nil
	}
	return out, nil
}

func invalidRecoveryStart(message string) error {
	return recoveryRefusal("invalid_start_options", message, fault.ErrInvalid)
}

func validateRecoveryStart(status recovery.Status, opts NodeOptions) error {
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

func validateRebuildStart(status recovery.Status, opts NodeOptions) error {
	if status.Phase != recovery.AwaitingStart && status.Phase != recovery.Importing &&
		status.Phase != recovery.Validating {
		return invalidRecoveryStart("rebuild is not at a startable phase")
	}
	if status.Identity.MemberID == 0 || status.Identity.MemberID == math.MaxUint64 {
		return invalidRecoveryStart("rebuild requires its reported positive founding member")
	}
	if _, _, err := net.SplitHostPort(opts.Meta.Network.AdvertiseAddress); err != nil {
		return invalidRecoveryStart("rebuild requires a configured advertised metadata address")
	}
	return nil
}

func validateResetStart(status recovery.Status, opts NodeOptions) error {
	if status.Phase != recovery.JoinReady || status.Identity.MemberID != 0 || len(opts.Bootstrap.JoinSeeds) == 0 {
		return invalidRecoveryStart("reset requires JoinReady, fresh admission and configured join seeds")
	}
	for _, seed := range opts.Bootstrap.JoinSeeds {
		if _, _, err := net.SplitHostPort(seed); err != nil {
			return invalidRecoveryStart("invalid recovery join seed")
		}
	}
	return nil
}

func cloneRecoveryStartOptions(opts NodeOptions) NodeOptions {
	out := opts
	out.Node.Labels = maps.Clone(opts.Node.Labels)
	out.Placement = opts.Placement.Clone()
	out.Bootstrap.JoinSeeds = slices.Clone(opts.Bootstrap.JoinSeeds)
	out.Bootstrap.Founders = slices.Clone(opts.Bootstrap.Founders)
	for i := range out.Bootstrap.Founders {
		out.Bootstrap.Founders[i].Node.Labels = maps.Clone(opts.Bootstrap.Founders[i].Node.Labels)
	}
	if opts.Loads.Storage.WAL != nil {
		wal := *opts.Loads.Storage.WAL
		if wal.Prewrite != nil {
			value := *wal.Prewrite
			wal.Prewrite = &value
		}
		out.Loads.Storage.WAL = &wal
	}
	if opts.Meta.VoterConfig != nil {
		config := *opts.Meta.VoterConfig
		out.Meta.VoterConfig = &config
	}
	out.Meta.Network.ClusterCredential = bytes.Clone(opts.Meta.Network.ClusterCredential)
	if opts.Meta.Network.TLS != nil {
		out.Meta.Network.TLS = opts.Meta.Network.TLS.Clone()
	}
	if opts.Meta.Network.ClientTLS != nil {
		out.Meta.Network.ClientTLS = opts.Meta.Network.ClientTLS.Clone()
	}
	return out
}
