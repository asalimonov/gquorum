package gquorum

import (
	"context"
	"errors"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	"github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/walbackend"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/storage/memstore"
	"github.com/asalimonov/gquorum/storage/walstore"
	"github.com/asalimonov/gquorum/topology"
)

func (n *node) Start(parent context.Context) error {
	n.startGate.Lock()
	defer n.startGate.Unlock()
	accepted, err := n.acceptNodeStart(parent)
	if err != nil || !accepted {
		return err
	}
	n.publishObservation()

	if err := n.checkAuthorityPath(); err != nil {
		return n.fail(err)
	}
	if err := n.acquireStorageOwner(); err != nil {
		return n.fail(err)
	}
	if err := n.inspectAuthority(); err != nil {
		return n.fail(err)
	}
	m, err := metaservice.New(n.runtimeMetaOptions())
	if err != nil {
		return n.fail(err)
	}
	n.mu.Lock()
	n.meta = m
	n.mu.Unlock()
	if err := n.ctx.Err(); err != nil {
		return n.fail(err)
	}
	if err := n.openLoadResources(m); err != nil {
		return n.fail(err)
	}
	// Meta has a separate serving context so owner cancellation cannot close
	// its database ahead of managed replicas. The joined teardown calls Quiesce
	// first.
	n.wg.Add(1)
	go n.removalLoop()
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h != nil {
		if err := h.Start(n.ctx); err != nil {
			return n.fail(err)
		}
	}
	// Start the owner before exposing metadata control ingress. An immediate
	// readmission fence may otherwise retire a Host whose Start is still
	// pending.
	if err := m.Start(context.WithoutCancel(n.ctx)); err != nil {
		return n.fail(err)
	}
	if err := n.startRecoveryCoordinator(m); err != nil {
		return n.fail(err)
	}
	n.wg.Add(1)
	go n.monitor()
	return nil
}

func (n *node) acquireStorageOwner() error {
	paths := recoverymodel.Paths{StorageDir: n.opts.StorageDir}
	if wal := n.opts.Loads.Storage.WAL; wal != nil {
		paths.LoadWALDir = wal.RootDir
	}
	owner, err := recoverydisk.Acquire(n.ctx, paths)
	if err != nil {
		return err
	}
	n.mu.Lock()
	n.storageOwner = owner
	n.mu.Unlock()
	return n.prepareRecoveryStart(n.ctx)
}

func (n *node) acceptNodeStart(parent context.Context) (bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopping || n.terminal != nil {
		return false, fault.ErrStopped
	}
	if n.state != NodeNew {
		return false, nil
	}
	if err := parent.Err(); err != nil {
		return false, err
	}
	n.ctx, n.cancel = context.WithCancel(parent)
	n.state, n.startedAt = NodeStarting, time.Now().UTC()
	n.lifecycle = newLifecycleRunner(n.ctx, n.opts.Lifecycle.Hooks, n.opts.CallbackTimeout)
	n.wg.Add(1)
	go func() { defer n.wg.Done(); <-n.ctx.Done(); n.RequestStop() }()
	return true, nil
}

// runtimeMetaOptions binds metadata callbacks to this exact Node owner.
func (n *node) runtimeMetaOptions() metaservice.Options {
	meta := n.metaOpts
	meta.BeforeOpenEngine = n.acquireMetaAuthority
	meta.AfterOpenEngine = n.markMetaAcquired
	meta.RequestOwnerStop = n.metaRequestedStop
	meta.OnApplied = n.signal
	meta.OnSelfRemoved = n.onRemoved
	meta.OnReadmission = n.readmit
	meta.ReadmissionIdentity = n.readmissionIdentity
	meta.ReadmissionReceipts = n.collectReadmissionReceipts
	meta.InstallLocalFence = n.installAuthorityFence
	meta.ApplyLocalMetaAction = n.applyMetaHostAction
	if n.opts.Loads.Factory != nil {
		meta.LoadGroups = nodeEffects{node: n}
		meta.HeldLoads = n.heldLoads
		meta.LoadMetricsReads = n.loadMetricsReads
		meta.LocalLoadMetrics = n.localLoadMetrics
	}
	return meta
}

func (n *node) openLoadResources(m *metaservice.Group) error {
	if n.opts.Loads.Factory == nil {
		return nil
	}
	store, retirement := n.opts.Loads.Storage.Store, n.opts.Loads.Storage.Retirement
	if o := n.opts.Loads.Storage.WAL; o != nil {
		lease, err := n.storageOwner.TakeWALLease()
		if err != nil {
			return err
		}
		owned, err := walbackend.OpenWithLease(walstore.Options{
			RootDir:      o.RootDir,
			NodeName:     m.Identity().NodeName,
			ClusterID:    m.Identity().ClusterID,
			SegmentBytes: o.SegmentBytes,
			Prewrite:     o.Prewrite,
			DisableFSync: o.DisableFSync,
			Logger:       n.opts.Logger,
		}, lease)
		if err != nil {
			return errors.Join(err, n.storageOwner.ReturnWALLease(lease))
		}
		n.mu.Lock()
		n.ownedWAL = owned
		n.mu.Unlock()
		store, retirement = owned, owned
	}
	// Groups created with topology.StorageMemory keep their Raft state
	// here.
	memory, err := memstore.New(memstore.Options{NodeName: m.Identity().NodeName,
		ClusterID: m.Identity().ClusterID})
	if err != nil {
		return err
	}
	n.mu.Lock()
	n.store, n.retirement, n.ownedMemory = store, retirement, memory
	n.mu.Unlock()
	h, err := n.newHost(m)
	if err != nil {
		return err
	}
	n.mu.Lock()
	n.host = h
	n.mu.Unlock()
	return nil
}

func (n *node) fail(err error) error {
	n.mu.Lock()
	if n.terminal == nil {
		n.terminal = err
	}
	n.state = NodeFailed
	n.mu.Unlock()
	n.readyOnce.Do(func() { close(n.readySignal) })
	n.publishObservation()
	n.RequestStop()
	return err
}

func (n *node) WaitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-n.readySignal:
	case <-n.stopped:
	}
	n.mu.RLock()
	err, ready, stopped, m := n.terminal, n.ready, n.stopping || n.fenced, n.meta
	n.mu.RUnlock()
	if err != nil {
		return err
	}
	if stopped {
		return fault.ErrStopped
	}
	if m != nil {
		observed, err := m.Lifecycle()
		removal := observed.Removal
		if err != nil {
			return err
		}
		if removal != nil {
			return fault.ErrStopped
		}
	}
	if !ready {
		return fault.ErrReplicaNotReady
	}
	return nil
}

func (n *node) monitor() {
	defer n.wg.Done()
	tick := time.NewTicker(n.opts.Meta.Timing.TickInterval)
	defer tick.Stop()
	for {
		if n.ctx.Err() != nil {
			return
		}
		n.mu.RLock()
		m, ready := n.meta, n.ready
		n.mu.RUnlock()
		observed, terminal := m.Lifecycle()
		removal := observed.Removal
		if terminal != nil {
			_ = n.fail(terminal)
			return
		}
		if removal == nil {
			n.updateReadmittedHost(m)
		}
		if removal == nil && !ready {
			if err := n.checkReadiness(m); err != nil {
				_ = n.fail(err)
				return
			}
		}
		n.publishObservation()
		select {
		case <-n.ctx.Done():
			return
		case <-n.wake:
		case <-tick.C:
		}
	}
}

func (n *node) updateReadmittedHost(m *metaservice.Group) {
	if err := n.replaceReadmittedHost(m); err != nil {
		n.recordError(err)
	}
}

func (n *node) checkReadiness(m *metaservice.Group) error {
	in := m.Readiness(n.startedAt)
	if err := n.prepareRecoveryReadiness(in.Identity); err != nil {
		n.recordError(err)
		return nil
	}
	decision, err := n.lifecycle.Readiness(n.ctx, in)
	if err != nil {
		n.recordError(err)
		return nil
	}
	switch decision.State {
	case lifecycle.Failed:
		return fault.New(fault.Unavailable, "startup readiness failed: "+decision.Reason, nil)
	case lifecycle.Ready:
		n.completeReadiness(m, in.Identity)
	default: // Keep waiting for other readiness states.
	}
	return nil
}

func (n *node) completeReadiness(m *metaservice.Group, expected topology.Identity) {
	// Recheck mandatory state after caller code, outside any lock.
	current := m.Readiness(n.startedAt)
	if current.Identity != expected || !current.Admitted || !current.Recovered || !current.SessionLive {
		return
	}
	if err := n.commitRecoveryReadiness(expected); err != nil {
		n.recordError(err)
		return
	}
	n.mu.Lock()
	if !n.stopping && !n.fenced && n.pending == nil {
		n.ready, n.state, n.lastError = true, NodeReady, nil
	}
	completed := n.ready
	n.mu.Unlock()
	if completed {
		n.readyOnce.Do(func() { close(n.readySignal) })
	}
}

func (n *node) recordError(err error) {
	if n.ctx.Err() != nil {
		return
	}
	n.mu.Lock()
	n.lastError = err
	n.mu.Unlock()
}

func (n *node) onRemoved(context.Context, string) error {
	n.mu.Lock()
	n.fenced, n.ready = true, false
	if !n.stopping {
		n.state = NodeFenced
	}
	h := n.host
	n.mu.Unlock()
	if h != nil {
		h.Fence()
	}
	n.readyOnce.Do(func() { close(n.readySignal) })
	select {
	case n.removal <- struct{}{}:
	default:
	}
	return nil
}

func (n *node) removalLoop() {
	defer n.wg.Done()
	select {
	case <-n.ctx.Done():
		return
	case <-n.removal:
	}
	tick := time.NewTicker(n.opts.Meta.Timing.TickInterval)
	defer tick.Stop()
	for n.ctx.Err() == nil {
		n.mu.RLock()
		m := n.meta
		n.mu.RUnlock()
		observed, _ := m.Lifecycle()
		removal := observed.Removal
		if removal != nil && n.handleRemoval(m, *removal) {
			return
		}
		select {
		case <-n.ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (n *node) handleRemoval(m *metaservice.Group, removal topology.SelfRemovalStatus) bool {
	n.mu.RLock()
	h := n.host
	current := n.meta == m && n.pending == nil && !n.stopping
	n.mu.RUnlock()
	id := m.Identity()
	if !current || id.MemberID != removal.MemberID {
		return false
	}
	// Lifecycle hooks cannot keep a removed Raft participant running. Complete
	// the mandatory fence first; failures retain ownership and retry on the
	// monitor.
	if err := m.FenceParticipation(n.ctx); err != nil {
		n.recordError(err)
		return false
	}
	if h != nil {
		if err := h.FenceParticipation(n.ctx); err != nil {
			n.recordError(err)
			return false
		}
	}
	if err := n.confirmMetaStop(n.ctx, m); err != nil {
		n.recordError(err)
		return false
	}
	decision, err := n.lifecycle.Removal(n.ctx, lifecycle.RemovalInput{Identity: id, Removal: removal,
		Fenced: true})
	if err != nil {
		n.recordError(err)
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	// Readmission may have acquired ownership while the proof read or hooks
	// callback waited. An old-member decision cannot stop a replacement.
	sameOwner := n.pending == nil && n.meta == m && m.Identity() == id
	if !sameOwner || n.stopping {
		return false
	}
	n.lastError = nil
	if decision.Action == lifecycle.StopNode {
		n.requestStopLocked(stopOnce)
	}
	return true
}

func (n *node) metaRequestedStop() {
	n.mu.RLock()
	m := n.meta
	n.mu.RUnlock()
	if m != nil {
		_, err := m.Lifecycle()
		if err != nil {
			_ = n.fail(err)
			return
		}
	}
	n.RequestStop()
}

type nodeStopAttempt struct {
	done chan struct{}
	err  error
}

type stopRequestMode uint8

const (
	stopOnce stopRequestMode = iota
	retryStop
)

// RequestStop is safe inside callbacks: it closes admission and returns without
// waiting for the same callback to be joined by the teardown coordinator.
func (n *node) RequestStop() { n.beginStop(stopOnce) }

func (n *node) beginStop(mode stopRequestMode) *nodeStopAttempt {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.requestStopLocked(mode)
}

func (n *node) requestStopLocked(mode stopRequestMode) *nodeStopAttempt {
	if !n.stopping {
		n.stopping = true
		if n.terminal == nil {
			n.state = NodeStopping
		}
		if n.cancel != nil {
			n.cancel()
		}
		n.stopAttempt = &nodeStopAttempt{done: make(chan struct{})}
		n.readyOnce.Do(func() { close(n.readySignal) })
		go n.teardown(n.stopAttempt)
	} else if mode == retryStop {
		select {
		case <-n.stopAttempt.done:
			if n.stopAttempt.err != nil {
				n.stopAttempt = &nodeStopAttempt{done: make(chan struct{})}
				n.retryStop <- n.stopAttempt
			}
		default:
		}
	}
	return n.stopAttempt
}

func (n *node) Stop(ctx context.Context) error {
	attempt := n.beginStop(retryStop)
	select {
	case <-attempt.done:
		return attempt.err
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "node teardown has not completed", ctx.Err())
	}
}

func (n *node) teardown(initialAttempt *nodeStopAttempt) {
	n.publishObservation()
	attempt := initialAttempt
	for {
		err := n.teardownResources()
		n.mu.Lock()
		attempt.err = err
		if err != nil {
			n.lastError = err
		} else {
			n.lastError = nil
			if n.terminal == nil {
				n.state = NodeStopped
			}
			close(n.stopped)
		}
		n.mu.Unlock()
		if err == nil {
			n.finishObservations()
		} else {
			n.publishObservation()
		}
		n.mu.Lock()
		close(attempt.done)
		n.mu.Unlock()
		if err == nil {
			return
		}
		attempt = <-n.retryStop
	}
}

func (n *node) teardownResources() error {
	n.startGate.Lock()
	n.mu.RLock()
	m, p, coordinator := n.meta, n.lifecycle, n.recoveryCoordinator
	n.mu.RUnlock()
	n.startGate.Unlock()
	ctx := context.Background()
	if err := joinNodeControl(ctx, m, p, coordinator); err != nil {
		return err
	}
	n.wg.Wait()
	n.mu.RLock()
	h, owned, memory := n.host, n.ownedWAL, n.ownedMemory
	n.mu.RUnlock()
	if h != nil {
		if err := h.Stop(ctx); err != nil {
			return err
		}
	}
	if err := closeLoadStores(memory, owned); err != nil {
		return err
	}
	if m != nil {
		if err := m.Stop(ctx); err != nil {
			return err
		}
	}
	if err := n.closeAuthority(); err != nil {
		return err
	}
	if n.storageOwner != nil {
		return n.storageOwner.Close()
	}
	return nil
}

func closeLoadStores(memory *memstore.Store, owned *walstore.Store) error {
	if memory != nil {
		if err := memory.Close(); err != nil {
			return err
		}
	}
	if owned != nil {
		if err := owned.Close(); err != nil {
			return err
		}
	}
	return nil
}

var _ Node = (*node)(nil)
