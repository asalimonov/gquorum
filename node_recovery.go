package gquorum

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	recoveryruntime "github.com/asalimonov/gquorum/internal/recovery/runtime"
	"github.com/asalimonov/gquorum/topology"
)

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
