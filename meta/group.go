package meta

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/convergence"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/internal/observe"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/internal/reconcile"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type Group struct {
	opts                 Options
	rules                topology.Rules
	mu                   sync.RWMutex
	startGate            sync.Mutex
	lifecycle            string
	terminal             error
	identity             Identity
	machine              *metaengine.Machine
	engine               atomic.Pointer[raftengine.Group]
	store                *metastore.Store
	transport            *metatransport.Transport
	coordinator          *membership.Coordinator
	convergence          *convergence.ConvergenceEngine
	reconciler           *reconcile.Reconciler
	observer             *observe.Watch
	recorderMu           sync.Mutex
	lastExecution        *topology.ExecutionStatus
	lastDrained          map[string]bool
	leases               *sessionManager
	parked               parkedRegistry
	watches              *watchRegistry
	leaseGate            sync.Mutex
	addresses            map[string]string
	joinMembers          map[uint64]string
	recoveryDocuments    []topology.Document
	joinDone, joinReady  bool
	joinAdmission        metaengine.AdmissionRecord
	joinLeader           string
	admissionToken       chan struct{}
	ctx                  context.Context
	cancel               context.CancelFunc
	workCtx              context.Context
	workCancel           context.CancelFunc
	quiescing, rpcClosed bool
	quiesced             chan struct{}
	quiesceOnce          sync.Once
	effectWG, rpcWG      sync.WaitGroup
	memberSeen           bool
	selfRemoval          *topology.SelfRemovalStatus
	removalSignal        chan struct{}
	removalWG            sync.WaitGroup
	started              bool
	ready, stopped       chan struct{}
	readyOnce, stopOnce  sync.Once
	wg                   sync.WaitGroup
	sessionMu            sync.Mutex
	sessionToken         chan struct{}
	ownSession           string
	ownDeadline          time.Time
	sessionPaused        bool
	lastDeactivated      string
	serveOverride        int
}

// New constructs a meta group without starting network or Raft workers. An
// existing pinned identity wins when MemberID is zero; an explicit conflicting
// identity fails. The supplied listener transfers to Group on success.
func New(input Options) (*Group, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	opts := input.defaults()
	pin, exists, err := metastore.LoadIdentity(opts.DBPath)
	if err != nil {
		return nil, err
	}
	if exists {
		if pin.NodeName != opts.Identity.NodeName || pin.ClusterID != opts.Identity.ClusterID || input.Identity.MemberID != 0 && input.Identity.MemberID != pin.MemberID {
			return nil, fault.ErrIdentityMismatch
		}
		opts.Identity.MemberID = pin.MemberID
	}
	if opts.Join && !exists && opts.Identity.MemberID != 0 {
		return nil, fault.New(fault.IdentityMismatch, "a retained joining identity requires a pinned store; fresh joiners must request an identity", nil)
	}
	if !opts.Join && !exists && len(opts.Founders) == 0 {
		return nil, fault.New(fault.Invalid, "fresh meta storage requires founding members", nil)
	}
	g := &Group{opts: opts, rules: topology.Rules{SchemaVersion: 1, Limits: opts.Limits}, identity: opts.Identity, lifecycle: "constructed", coordinator: membership.NewCoordinator(), leases: newSessionManager(opts.SessionTTL), addresses: make(map[string]string), joinMembers: make(map[uint64]string), admissionToken: make(chan struct{}, 1), ready: make(chan struct{}), stopped: make(chan struct{}), quiesced: make(chan struct{}), removalSignal: make(chan struct{}, 1), sessionToken: make(chan struct{}, 1)}
	for _, founder := range opts.Founders {
		g.addresses[founder.Node.Name] = founder.Address
	}
	g.addresses[opts.Identity.NodeName] = opts.Network.AdvertiseAddress
	g.machine = metaengine.NewMachine(g.rules, nil)
	g.watches = newWatchRegistry(opts.MaxWatchers)
	g.reconciler, err = reconcile.New(reconcile.Config{Host: reconciliationHost{g}, Settings: opts.Reconciliation, Logger: opts.Logger, OnTransition: g.recordReconciliation})
	if err != nil {
		return nil, err
	}
	g.observer, err = observe.New(observe.Config{Source: observationSource{g}, Settings: opts.Observation, Logger: opts.Logger, OnGroupTransition: g.recordGroupHealth, OnNodeTransition: g.recordNodeHealth})
	if err != nil {
		return nil, err
	}
	// The transport refers back to this group through its meta-only handler seam.
	network, err := metatransport.New(metatransport.Options{Header: &metapb.Header{ClusterId: opts.Identity.ClusterID, NodeName: opts.Identity.NodeName, InstanceId: opts.Identity.InstanceID, MemberId: opts.Identity.MemberID, AdvertiseAddress: opts.Network.AdvertiseAddress}, Listener: opts.Network.Listener, TLS: opts.Network.TLS, ClientTLS: opts.Network.ClientTLS, AllowPlaintext: opts.Network.AllowPlaintext, Credential: opts.Network.ClusterCredential, Handler: g, Resolve: g.resolveMember, SendTimeout: opts.Timing.SendTimeout, SnapshotTimeout: 10 * time.Second, RetryInitial: opts.Timing.PeerRetryInitial, RetryMax: opts.Timing.PeerRetryMax, MaxSnapshotBytes: opts.Network.MaxSnapshotBytes, OnUnreachable: func(id uint64) {
		if engine := g.engine.Load(); engine != nil {
			engine.ReportUnreachable(id)
		}
	}, OnSnapshot: func(id uint64, status raft.SnapshotStatus) {
		if engine := g.engine.Load(); engine != nil {
			engine.ReportSnapshot(id, status)
		}
	}})
	if err != nil {
		return nil, err
	}
	g.transport = network
	g.refreshAddressHints()
	if opts.Identity.MemberID != 0 {
		mode := raftengine.Joining
		var peers []raft.Peer
		if !exists && !opts.Join {
			mode = raftengine.Founding
			for _, founder := range opts.Founders {
				peers = append(peers, raft.Peer{ID: founder.MemberID})
			}
		}
		if err = g.openEngine(opts.Identity, mode, peers); err != nil {
			return nil, err
		}
	}
	g.convergence = convergence.NewConvergenceEngine(convergence.ConvergenceConfig{Store: g, Rules: g.rules, Coordinator: executionCoordinator{g}, Groups: groupControllers{g}, OwnsGroup: func(string) bool { return true }, Interval: g.opts.Timing.TickInterval, MaxRetryInterval: time.Second, FinalValidator: g.finalTarget})
	return g, nil
}
func (g *Group) openEngine(identity Identity, mode raftengine.Mode, peers []raft.Peer) error {
	store, err := metastore.Open(g.opts.DBPath)
	if err != nil {
		return err
	}
	// Validate the persisted rule pin even when no Raft snapshot was produced.
	// This materialized state is never used to skip committed-log replay.
	var recoveryDocuments []topology.Document
	materialized, err := store.LoadState()
	if err != nil {
		store.Close()
		return err
	}
	if len(materialized) > 0 {
		check := metaengine.NewMachine(g.rules, nil)
		if err = check.Restore(context.Background(), materialized); err != nil {
			store.Close()
			return err
		}
		latest, target, active := check.Views()
		recoveryDocuments = []topology.Document{active, target, latest}
	}
	override, err := store.LoadOverride()
	if err != nil || override < 0 || override > g.rules.Limits.MaxNodes {
		store.Close()
		if err != nil {
			return err
		}
		return fault.New(fault.Unreadable, "invalid persisted local serving override", nil)
	}
	seen, err := store.MemberSeen()
	if err != nil {
		store.Close()
		return err
	}
	machine := metaengine.NewMachine(g.rules, store)
	machine.OnApplied = func(index uint64, cs raftpb.ConfState) {
		latest, target, active := machine.Views()
		g.watches.publish(latest, target, active)
		g.recordApplied(machine, active)
		g.observer.Wake()
		g.observeRemoval(identity, target, cs, index)
	}
	engine, err := raftengine.NewMeta(raftengine.Options{GroupID: topology.MetaGroupID, NodeName: identity.NodeName, ClusterID: identity.ClusterID, InstanceID: identity.InstanceID, ReplicaID: identity.MemberID, Mode: mode, Peers: peers, Timing: raftengine.Timing{TickInterval: g.opts.Timing.TickInterval, ElectionTicks: g.opts.Timing.ElectionTicks, HeartbeatTicks: g.opts.Timing.HeartbeatTicks}, Store: &metastore.Adapter{Store: store}, Machine: machine, Transport: g.transport, Events: g.opts.Events, Logger: g.opts.Logger, SnapshotThreshold: g.opts.SnapshotThreshold})
	if err != nil {
		store.Close()
		return err
	}
	g.mu.Lock()
	g.store = store
	g.serveOverride = override
	g.recoveryDocuments = recoveryDocuments
	g.machine = machine
	g.identity = identity
	g.memberSeen = seen || containsMember(engine.Status().ConfState, identity.MemberID)
	g.selfRemoval = nil
	g.mu.Unlock()
	g.engine.Store(engine)
	g.transport.SetMemberID(identity.MemberID)
	return nil
}
func (g *Group) Start(parent context.Context) error {
	g.startGate.Lock()
	defer g.startGate.Unlock()
	if err := parent.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	if g.started || g.quiescing || g.lifecycle == "stopped" || g.lifecycle == "stopping" {
		g.mu.Unlock()
		return fault.ErrStopped
	}
	g.started = true
	g.lifecycle = "starting"
	g.ctx, g.cancel = context.WithCancel(parent)
	g.workCtx, g.workCancel = context.WithCancel(g.ctx)
	g.wg.Add(1)
	g.mu.Unlock()
	if err := g.transport.Start(g.ctx); err != nil {
		g.wg.Done()
		return g.startFailed(err)
	}
	if engine := g.engine.Load(); engine != nil && !g.opts.Join {
		if err := engine.Start(g.ctx); err != nil {
			g.wg.Done()
			return g.startFailed(err)
		}
	}
	g.convergence.Start(g.workCtx)
	if err := g.reconciler.Start(g.workCtx); err != nil {
		g.wg.Done()
		return g.startFailed(err)
	}
	if err := g.observer.Start(g.workCtx); err != nil {
		g.wg.Done()
		return g.startFailed(err)
	}
	if g.opts.OnSelfRemoved != nil {
		g.removalWG.Add(1)
		go g.notifyRemoval(g.ctx)
	}
	g.wg.Add(1)
	go g.runParkedSweep()
	go g.run()
	return nil
}
func (g *Group) fail(err error) {
	g.mu.Lock()
	if g.terminal == nil {
		g.terminal = err
	}
	g.lifecycle = "failed"
	cancel := g.cancel
	g.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (g *Group) WaitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.stopped:
		g.mu.RLock()
		err := g.terminal
		g.mu.RUnlock()
		if err != nil {
			return err
		}
		return fault.ErrStopped
	case <-g.ready:
	}
	g.mu.RLock()
	err := g.terminal
	stopped := g.selfRemoval != nil || g.quiescing || g.lifecycle == "stopped" || g.lifecycle == "stopping" || g.ctx != nil && g.ctx.Err() != nil
	g.mu.RUnlock()
	if err == nil && stopped {
		return fault.ErrStopped
	}
	return err
}
func (g *Group) Rules() topology.Rules { return g.rules }
func (g *Group) IsLeader() bool {
	g.mu.RLock()
	running := g.started && g.selfRemoval == nil && g.ctx != nil && g.ctx.Err() == nil
	g.mu.RUnlock()
	if !running {
		return false
	}
	engine := g.engine.Load()
	return engine != nil && engine.Error() == nil && engine.IsLeader()
}
func (g *Group) state() *metaengine.Machine { g.mu.RLock(); defer g.mu.RUnlock(); return g.machine }
func (g *Group) currentIdentity() Identity  { g.mu.RLock(); defer g.mu.RUnlock(); return g.identity }
func (g *Group) resolveNode(ctx context.Context, name string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if g.opts.Network.Resolver != nil {
		address, err := g.opts.Network.Resolver.Resolve(ctx, name)
		if err == nil && address != "" {
			return address, nil
		}
	}
	g.mu.RLock()
	address := g.addresses[name]
	g.mu.RUnlock()
	if address == "" {
		return "", fault.New(fault.Unavailable, fmt.Sprintf("meta address for node %q is unknown", name), nil)
	}
	return address, nil
}
func (g *Group) memberNode(id uint64) string {
	name, established := g.state().MemberOwner(id)
	if name != "" || established {
		return name
	}
	g.mu.RLock()
	joined := g.joinMembers[id]
	g.mu.RUnlock()
	if joined != "" {
		return joined
	}
	for _, founder := range g.opts.Founders {
		if founder.MemberID == id {
			return founder.Node.Name
		}
	}
	return ""
}

func (g *Group) resolveMember(ctx context.Context, id uint64) (string, error) {
	name := g.memberNode(id)
	// Ready messages can still carry a retired peer's final commit when Active
	// advances before the asynchronous sender drains. Retained documents may
	// resolve that destination, but never authorize incoming retired identities.
	if name == "" {
		for _, doc := range g.state().History(0, 0) {
			if group := doc.MetaGroup(); group != nil {
				for _, replica := range group.Replicas {
					if replica.ReplicaID == id {
						name = replica.NodeName
						break
					}
				}
			}
			if name != "" {
				break
			}
		}
	}
	if name == "" {
		return "", fault.ErrIdentityMismatch
	}
	return g.resolveNode(ctx, name)
}
func (g *Group) ensureRunning(ctx context.Context) error { return g.ensureRuntime(ctx, false) }
func (g *Group) ensureRuntime(ctx context.Context, shutdown bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.ensureRuntimeLocked(shutdown)
}

func (g *Group) ensureRuntimeLocked(shutdown bool) error {
	if g.terminal != nil {
		return g.terminal
	}
	if !g.started || g.ctx == nil || g.ctx.Err() != nil || (g.quiescing || g.selfRemoval != nil) && !shutdown {
		return fault.ErrStopped
	}
	return nil
}
func (g *Group) terminalCode() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var err *fault.Error
	if errors.As(g.terminal, &err) {
		return string(err.Code)
	}
	if g.terminal != nil {
		return string(fault.Unavailable)
	}
	if g.selfRemoval != nil {
		return string(fault.IdentityMismatch)
	}
	return ""
}

func (g *Group) activeMemberID(name string) uint64 {
	return g.state().ActiveMemberID(name)
}

func (g *Group) sessionLive() bool {
	if g.ensureRunning(context.Background()) != nil {
		return false
	}
	g.sessionMu.Lock()
	defer g.sessionMu.Unlock()
	return g.ownSession != "" && time.Now().Before(g.ownDeadline)
}

func (g *Group) OwnSessionLive() bool { return g.sessionLive() }
