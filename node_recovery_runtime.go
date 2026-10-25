package gquorum

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaservice"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/recoveryruntime"
	"github.com/asalimonov/gquorum/topology"
)

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
	if n.recoveryMetaPort != nil {
		metaPort = n.recoveryMetaPort(metaPort)
	}
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
