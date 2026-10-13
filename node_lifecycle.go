package gquorum

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/policy"
	"github.com/asalimonov/gquorum/storage/walstore"
	"github.com/asalimonov/gquorum/topology"
)

func (n *node) Start(parent context.Context) error {
	n.startGate.Lock()
	defer n.startGate.Unlock()
	n.mu.Lock()
	if n.stopping || n.terminal != nil {
		n.mu.Unlock()
		return fault.ErrStopped
	}
	if n.state != NodeNew {
		n.mu.Unlock()
		return nil
	}
	if err := parent.Err(); err != nil {
		n.mu.Unlock()
		return err
	}
	n.ctx, n.cancel = context.WithCancel(parent)
	n.state, n.startedAt = NodeStarting, time.Now().UTC()
	n.policies = newLifecycleRunner(n.ctx, n.opts.Policies.Lifecycle, n.opts.Policies.CallbackTimeout)
	n.wg.Add(1)
	go func() { defer n.wg.Done(); <-n.ctx.Done(); n.RequestStop() }()
	n.mu.Unlock()

	if err := n.checkAuthorityPath(); err != nil {
		return n.fail(err)
	}
	if err := n.inspectAuthority(); err != nil {
		return n.fail(err)
	}
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
	m, err := metaservice.New(meta)
	if err != nil {
		return n.fail(err)
	}
	n.mu.Lock()
	n.meta = m
	n.mu.Unlock()
	if err := n.ctx.Err(); err != nil {
		return n.fail(err)
	}
	if n.opts.Loads.Factory != nil {
		store, retirement := n.opts.Loads.Storage.Store, n.opts.Loads.Storage.Retirement
		if o := n.opts.Loads.Storage.WAL; o != nil {
			owned, err := walstore.Open(walstore.Options{RootDir: o.RootDir, NodeName: m.Identity().NodeName, ClusterID: m.Identity().ClusterID, SegmentBytes: o.SegmentBytes, Prewrite: o.Prewrite, Logger: n.opts.Logger})
			if err != nil {
				return n.fail(err)
			}
			n.mu.Lock()
			n.ownedWAL = owned
			n.mu.Unlock()
			store, retirement = owned, owned
		}
		n.mu.Lock()
		n.store, n.retirement = store, retirement
		n.mu.Unlock()
		h, err := n.newHost(m)
		if err != nil {
			return n.fail(err)
		}
		n.mu.Lock()
		n.host = h
		n.mu.Unlock()
	}
	// Meta has a separate serving context so owner cancellation cannot close its
	// database ahead of managed replicas. The joined teardown calls Quiesce first.
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
	// readmission fence may otherwise retire a Host whose Start is still pending.
	if err := m.Start(context.WithoutCancel(n.ctx)); err != nil {
		return n.fail(err)
	}
	n.wg.Add(1)
	go n.monitor()
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
		_, err, removal := m.Lifecycle()
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
		_, terminal, removal := m.Lifecycle()
		if terminal != nil {
			n.fail(terminal)
			return
		}
		if removal == nil {
			if err := n.replaceReadmittedHost(m); err != nil {
				n.recordError(err)
			}
			if !ready {
				in := m.Readiness(n.startedAt)
				decision, err := n.policies.Readiness(n.ctx, in)
				if err != nil {
					n.recordError(err)
				} else {
					switch decision.State {
					case policy.Failed:
						n.fail(fault.New(fault.Unavailable, "startup policy failed: "+decision.Reason, nil))
						return
					case policy.Ready:
						// Recheck mandatory state after caller code, outside any lock.
						current := m.Readiness(n.startedAt)
						if current.Identity == in.Identity && current.Admitted && current.Recovered && current.SessionLive {
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
					}
				}
			}
		}
		select {
		case <-n.ctx.Done():
			return
		case <-n.wake:
		case <-tick.C:
		}
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
		_, _, removal := m.Lifecycle()
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
	// Policies cannot keep a removed Raft participant running. Complete the
	// mandatory fence first; failures retain ownership and retry on the monitor.
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
	decision, err := n.policies.Removal(n.ctx, policy.RemovalInput{Identity: id, Removal: removal, Fenced: true})
	if err != nil {
		n.recordError(err)
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	// Readmission may have acquired ownership while the proof read or policy
	// callback waited. An old-member decision cannot stop a replacement.
	if n.pending != nil || n.meta != m || m.Identity() != id || n.stopping {
		return false
	}
	n.lastError = nil
	if decision.Action == policy.StopNode {
		n.requestStopLocked(false)
	}
	return true
}

func (n *node) metaRequestedStop() {
	n.mu.RLock()
	m := n.meta
	n.mu.RUnlock()
	if m != nil {
		_, err, _ := m.Lifecycle()
		if err != nil {
			n.fail(err)
			return
		}
	}
	n.RequestStop()
}

type nodeStopAttempt struct {
	done chan struct{}
	err  error
}

// RequestStop is safe inside callbacks: it closes admission and returns without
// waiting for the same callback to be joined by the teardown coordinator.
func (n *node) RequestStop() { n.requestStop(false) }

func (n *node) requestStop(retry bool) *nodeStopAttempt {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.requestStopLocked(retry)
}

func (n *node) requestStopLocked(retry bool) *nodeStopAttempt {
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
	} else if retry {
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
	attempt := n.requestStop(true)
	select {
	case <-attempt.done:
		return attempt.err
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "node teardown has not completed", ctx.Err())
	}
}

func (n *node) teardown(attempt *nodeStopAttempt) {
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
	m, p := n.meta, n.policies
	n.mu.RUnlock()
	n.startGate.Unlock()
	ctx := context.Background()
	if m != nil {
		if err := m.Quiesce(ctx); err != nil {
			return err
		}
	}
	if p != nil {
		if err := p.Stop(ctx); err != nil {
			return err
		}
	}
	n.wg.Wait()
	n.mu.RLock()
	h, owned := n.host, n.ownedWAL
	n.mu.RUnlock()
	if h != nil {
		if err := h.Stop(ctx); err != nil {
			return err
		}
	}
	if owned != nil {
		if err := owned.Close(); err != nil {
			return err
		}
	}
	if m != nil {
		if err := m.Stop(ctx); err != nil {
			return err
		}
	}
	return n.closeAuthority()
}

var _ Node = (*node)(nil)
