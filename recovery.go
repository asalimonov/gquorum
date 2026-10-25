package gquorum

import (
	"context"
	"maps"

	recoverycore "github.com/asalimonov/gquorum/internal/recovery"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
)

// Recovery configures offline inspection and archival; each call owns its resources.
type Recovery struct{ core recoverycore.Recovery }

// NewRecovery copies recovery settings without I/O or application callbacks.
func NewRecovery(opts NodeOptions, options ...recovery.Options) (*Recovery, error) {
	config := recoverycore.Config{
		Source: model.SourceOptions{Paths: model.Paths{StorageDir: opts.StorageDir},
			NodeName: opts.Identity.NodeName, ClusterID: opts.Identity.ClusterID},
		BorrowedStorage: opts.Loads.Storage.Store != nil || opts.Loads.Storage.Retirement != nil,
		Probe: recoverycore.ProbeOptions{Resolver: opts.Meta.Network.Resolver,
			Credential: opts.Meta.Network.ClusterCredential, ClientTLS: opts.Meta.Network.ClientTLS,
			AllowPlaintext: opts.Meta.Network.AllowPlaintext, AdvertiseAddress: opts.Meta.Network.AdvertiseAddress},
	}
	if opts.Loads.Storage.WAL != nil {
		config.WALConfigured = true
		config.Source.Paths.LoadWALDir = opts.Loads.Storage.WAL.RootDir
	}
	core, err := recoverycore.New(config, options...)
	if err != nil {
		return nil, err
	}
	return &Recovery{core: *core}, nil
}

// Inspect reads source evidence without changing it.
func (r *Recovery) Inspect(ctx context.Context, req recovery.Request) (recovery.Plan, error) {
	return r.core.Inspect(ctx, req)
}

// Approve durably accepts the inspected request without moving source data.
func (r *Recovery) Approve(ctx context.Context, req recovery.Request,
	a recovery.Acceptance) (recovery.Operation, error) {
	return r.core.Approve(ctx, req, a)
}

// Run executes or resumes the accepted archival operation.
func (r *Recovery) Run(ctx context.Context, id string) (recovery.Status, error) {
	return r.core.Run(ctx, id)
}

// Cancel records cancellation and undoes application preparation before archiving.
func (r *Recovery) Cancel(ctx context.Context, id, actor, reason string) (recovery.Status, error) {
	return r.core.Cancel(ctx, id, actor, reason)
}

// Status reads historical progress; an empty ID selects the current operation.
// A valid report may accompany an unrelated namespace error.
func (r *Recovery) Status(ctx context.Context, id string) (recovery.Status, error) {
	return r.core.Status(ctx, id)
}

// List returns valid historical reports alongside any namespace diagnostics.
func (r *Recovery) List(ctx context.Context) ([]recovery.Status, error) { return r.core.List(ctx) }

// RecoveryStartOptions copies settings and applies the reported identity/bootstrap.
// The caller supplies fresh process resources; Node validates durable authorization.
func RecoveryStartOptions(status recovery.Status, opts NodeOptions) (NodeOptions, error) {
	start := recoverycore.StartOptions{Identity: opts.Identity, Node: opts.Node,
		AdvertiseAddress: opts.Meta.Network.AdvertiseAddress, JoinSeeds: opts.Bootstrap.JoinSeeds}
	if err := recoverycore.ValidateStart(status, start); err != nil {
		return NodeOptions{}, err
	}
	out := cloneNodeOptions(opts)
	out.Identity.ClusterID, out.Identity.MemberID = status.Identity.ClusterID, status.Identity.MemberID
	if out.Node.State == "" {
		out.Node.State = topology.NodeActive
	}
	if status.Mode == recovery.Rebuild {
		founder := out.Node
		founder.Labels = maps.Clone(out.Node.Labels)
		out.Bootstrap = BootstrapOptions{Founders: []FoundingMember{{
			Node: founder, MemberID: status.Identity.MemberID, Address: out.Meta.Network.AdvertiseAddress,
		}}}
	} else {
		out.Bootstrap.Join = true
		out.Bootstrap.Founders = nil
	}
	return out, nil
}
