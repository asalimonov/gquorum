package gquorum

import (
	"context"
	"maps"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metaservice"
	recoverycore "github.com/asalimonov/gquorum/internal/recovery"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	recoveryruntime "github.com/asalimonov/gquorum/internal/recovery/runtime"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
)

// Recovery inspects, accepts and archives retained state after permanent majority
// loss. The implementation is internal; the handle has no Close lifetime and
// each operation owns its resources. Join Node shutdown before Inspect, Approve,
// Run or Cancel. Status and List remain available while Node is running.
type Recovery interface {
	// Inspect reads source evidence without changing it.
	Inspect(context.Context, recovery.Request) (recovery.Plan, error)
	// Approve durably accepts the inspected request without moving source data.
	Approve(context.Context, recovery.Request, recovery.Acceptance) (recovery.Operation, error)
	// Run executes or resumes the accepted archival operation.
	Run(context.Context, string) (recovery.Status, error)
	// Cancel records cancellation and undoes application preparation before archiving.
	Cancel(context.Context, string, string, string) (recovery.Status, error)
	// Status reads historical progress; an empty ID selects the current operation.
	// A valid report may accompany an unrelated namespace error.
	Status(context.Context, string) (recovery.Status, error)
	// List returns valid historical reports alongside any namespace diagnostics.
	List(context.Context) ([]recovery.Status, error)
}

// Recovery returns a controller using the Node's copied source configuration.
// It performs no I/O and is available before Start, during participation and after
// Stop. It neither stops nor starts Node; source ownership still gates operations.
func (n *node) Recovery(options ...recovery.Options) (Recovery, error) {
	return NewRecovery(n.opts, options...)
}

// NewRecovery returns a recovery controller for standalone maintenance.
// It copies only recovery settings without I/O or application callbacks; runtime
// dependencies such as a listener, load factory and load transport are not required.
func NewRecovery(opts NodeOptions, options ...recovery.Options) (Recovery, error) {
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
	return core, nil
}

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

func (n *node) prepareRecoveryStart(ctx context.Context) error {
	config := recoveryruntime.StartupConfig{
		Meta: &n.metaOpts, LimitsExplicit: n.limitsExplicit,
		BorrowedStorage: n.opts.Loads.Storage.Store != nil || n.opts.Loads.Storage.Retirement != nil,
		LoadFactory:     n.opts.Loads.Factory != nil,
		WALConfigured:   n.opts.Loads.Storage.WAL != nil, StartedAt: n.startedAt,
	}
	journal, err := recoveryruntime.PrepareStartup(ctx, n.storageOwner, config)
	if err != nil || journal == nil {
		return err
	}
	manifest := journal.View()
	n.mu.Lock()
	n.recoveryJournal, n.recoveryManifest = journal, &manifest
	n.mu.Unlock()
	return nil
}
func (n *node) resumeRecoveryAuthority() (bool, error) {
	resumed, err := recoveryruntime.CheckResumeAuthority(n.ctx, n.recoveryManifest, n.opts.authorityDBPath())
	if !resumed || err != nil {
		return resumed, err
	}
	if err := n.inspectFreshLoadStorage(); err != nil {
		return true, err
	}
	identity := n.recoveryManifest.Identity
	identity.InstanceID = n.opts.Identity.InstanceID
	n.authorityFreshness = authoritystore.RetainedEvidence
	return true, n.acquireAuthority(identity)
}
func (n *node) resumePinnedRecoveryFounding() (bool, error) {
	onlyPin, err := recoveryruntime.CheckPinnedFounding(n.recoveryManifest, n.opts.metaDBPath())
	if err != nil || !onlyPin {
		return false, err
	}
	resumed, err := n.resumeRecoveryAuthority()
	if resumed && err == nil {
		n.metaOpts.RecoveryResumeFounding = true
	}
	return resumed, err
}
func (n *node) recoveryMutationAllowed() error {
	n.mu.RLock()
	manifest, meta := n.recoveryManifest, n.meta
	n.mu.RUnlock()
	if manifest != nil && manifest.Request.Mode == model.Rebuild {
		if meta == nil {
			return fault.New(fault.OperationAuthorization, "recovery_in_progress", nil)
		}
		if meta.RecoveryInProgress() {
			return fault.New(fault.OperationAuthorization, "recovery_in_progress", nil)
		}
	}
	return nil
}

func (n *node) startRecoveryCoordinator(meta *metaservice.Group) error {
	n.mu.RLock()
	journal, manifest, host := n.recoveryJournal, n.recoveryManifest, n.host
	n.mu.RUnlock()
	if manifest == nil || manifest.Request.Mode != model.Rebuild {
		return nil
	}
	var port recoveryruntime.HostPort
	if host != nil {
		port = host
	}
	var metaPort recoveryruntime.MetaPort = meta
	coordinator, err := recoveryruntime.New(recoveryruntime.Config{
		Manifest: journal.View(), Journal: journal, Meta: metaPort, Host: port, OnChange: n.signal,
	})
	if err != nil {
		return err
	}
	n.mu.Lock()
	n.recoveryCoordinator = coordinator
	n.mu.Unlock()
	return coordinator.Start(n.ctx)
}

func (n *node) prepareRecoveryReadiness(identity topology.Identity) error {
	n.mu.RLock()
	coordinator := n.recoveryCoordinator
	n.mu.RUnlock()
	if coordinator == nil {
		return nil
	}
	if err := coordinator.Error(); err != nil {
		return err
	}
	if !coordinator.DataReady() {
		return fault.ErrReplicaNotReady
	}
	return coordinator.ReadyToPublish(n.ctx, identity)
}

func (n *node) commitRecoveryReadiness(identity topology.Identity) error {
	n.mu.RLock()
	coordinator := n.recoveryCoordinator
	stopped := n.stopping || n.fenced || n.pending != nil
	n.mu.RUnlock()
	if stopped {
		return fault.ErrStopped
	}
	if coordinator == nil {
		return nil
	}
	if err := coordinator.CommitLocalCompletion(n.ctx, identity); err != nil {
		return err
	}
	if err := coordinator.Stop(context.WithoutCancel(n.ctx)); err != nil {
		return err
	}
	n.mu.Lock()
	if n.recoveryCoordinator == coordinator {
		n.recoveryCoordinator, n.recoveryManifest = nil, nil
	}
	n.mu.Unlock()
	return nil
}

func joinNodeControl(ctx context.Context, meta *metaservice.Group, lifecycle *lifecycleRunner,
	coordinator *recoveryruntime.Coordinator) error {
	if coordinator != nil {
		if err := coordinator.Stop(ctx); err != nil {
			return err
		}
	}
	if meta != nil {
		if err := meta.Quiesce(ctx); err != nil {
			return err
		}
	}
	if lifecycle != nil {
		if err := lifecycle.Stop(ctx); err != nil {
			return err
		}
	}
	return nil
}
