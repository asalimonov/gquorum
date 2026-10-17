package metaservice

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/internal/observe"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/internal/raftengine"
	internalscheduling "github.com/asalimonov/gquorum/internal/scheduling"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// Group owns the node's private meta service, consensus engine and protocol
// orchestration.
type Group struct {
	opts                 Options
	rules                topology.Rules
	mu                   sync.RWMutex
	startGate            sync.Mutex
	lifecycle            string
	terminal             error
	closeErr             error
	firstBootstrap       bool
	foundingRoles        map[string]topology.ReplicaRole
	controlOnly          bool
	lastLoadStatus       map[string]topology.GroupStatus
	identity             topology.Identity
	machine              *metaengine.Machine
	engine               atomic.Pointer[raftengine.Group]
	store                *metastore.Store
	transport            *metatransport.Transport
	coordinator          *membership.Coordinator
	controller           *operator.Controller
	scheduler            *internalscheduling.Scheduler
	observer             *observe.Watch
	collector            *observe.MetricsCollector
	recorderMu           sync.Mutex
	lastOperations       map[topology.OperationID]map[string]topology.BlockStatus
	lastDrained          map[string]bool
	leases               *sessionManager
	parked               parkedRegistry
	watches              *watchRegistry
	leaseGate            sync.Mutex
	addresses            map[string]string
	joinMembers          map[uint64]string
	recoveryGroups       []topology.Group
	recoveryVersion      uint64
	readmissionGrant     *Readmission
	joinDone, joinReady  bool
	joinAdmission        metaengine.AdmissionRecord
	joinReceipts         []topology.EffectReceipt
	joinLeader           string
	admissionToken       chan struct{}
	ctx                  context.Context
	cancel               context.CancelFunc
	workCtx              context.Context
	workCancel           context.CancelFunc
	quiescing, rpcClosed bool
	quiesced             chan struct{}
	quiesceOnce          sync.Once
	participationFenced  chan struct{}
	participationOnce    sync.Once
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

type runtimeAccess uint8

const (
	ordinaryRuntime runtimeAccess = iota
	shutdownRuntime
)

// New constructs a meta group without starting network or Raft workers. An
// existing pinned identity wins when MemberID is zero; an explicit conflicting
// identity fails. The supplied listener transfers to Group on success.
func New(input Options) (*Group, error) {
	startup, err := prepareMetaStartup(input)
	if err != nil {
		return nil, err
	}
	g, err := startup.constructGroup()
	if err != nil {
		return nil, err
	}
	if err := g.configureServices(); err != nil {
		return nil, err
	}
	if err := g.configureTransport(); err != nil {
		return nil, err
	}
	if err := g.openInitialEngine(startup); err != nil {
		return nil, err
	}
	return g, nil
}

type metaStartup struct {
	options       Options
	pinFound      bool
	foundingRoles map[string]topology.ReplicaRole
}

func prepareMetaStartup(input Options) (metaStartup, error) {
	startup, err := inspectMetaStartup(input)
	if err != nil {
		return metaStartup{}, err
	}
	opts := startup.options
	if opts.Join && !startup.pinFound && opts.Identity.MemberID != 0 {
		return metaStartup{}, fault.New(fault.IdentityMismatch,
			"a retained joining identity requires a pinned store; fresh joiners must request an identity", nil)
	}
	if !opts.Join && !startup.pinFound && len(opts.Founders) == 0 {
		return metaStartup{}, fault.New(fault.Invalid, "fresh meta storage requires founding members", nil)
	}
	if !startup.pinFound && !opts.Join {
		if err := opts.freshVoterConfig().Validate(1, opts.Limits.MaxNodes); err != nil {
			return metaStartup{}, err
		}
	}
	var foundingRoles map[string]topology.ReplicaRole
	if !startup.pinFound && !opts.Join {
		roles, err := opts.selectFoundingRoles()
		if err != nil {
			return metaStartup{}, err
		}
		foundingRoles = roles
	}
	return metaStartup{options: opts, pinFound: startup.pinFound, foundingRoles: foundingRoles}, nil
}

func inspectMetaStartup(input Options) (metaStartup, error) {
	if err := input.Validate(); err != nil {
		return metaStartup{}, err
	}
	opts := input.defaults()
	pin, err := metastore.LoadIdentity(opts.DBPath)
	if err != nil {
		return metaStartup{}, err
	}
	if pin.Found {
		if pin.Identity.NodeName != opts.Identity.NodeName || pin.Identity.ClusterID != opts.Identity.ClusterID ||
			input.Identity.MemberID != 0 && input.Identity.MemberID != pin.Identity.MemberID {
			return metaStartup{}, fault.ErrIdentityMismatch
		}
		opts.Identity.MemberID = pin.Identity.MemberID
	} else {
		fresh, err := metastore.StoreIsFresh(opts.DBPath)
		if err != nil {
			return metaStartup{}, err
		}
		if !fresh {
			return metaStartup{}, fault.New(fault.Unreadable, "retained meta storage lacks an identity pin", nil)
		}
	}
	return metaStartup{options: opts, pinFound: pin.Found}, nil
}

func (startup metaStartup) constructGroup() (*Group, error) {
	opts := startup.options
	g := &Group{
		opts:                opts,
		rules:               topology.Rules{SchemaVersion: 1, Limits: opts.Limits},
		identity:            opts.Identity,
		lifecycle:           "constructed",
		coordinator:         membership.NewCoordinator(),
		leases:              newSessionManager(opts.SessionTTL),
		addresses:           make(map[string]string),
		joinMembers:         make(map[uint64]string),
		admissionToken:      make(chan struct{}, 1),
		ready:               make(chan struct{}),
		stopped:             make(chan struct{}),
		quiesced:            make(chan struct{}),
		participationFenced: make(chan struct{}),
		removalSignal:       make(chan struct{}, 1),
		sessionToken:        make(chan struct{}, 1),
	}
	g.firstBootstrap = !startup.pinFound && !opts.Join
	g.foundingRoles = startup.foundingRoles
	g.lastLoadStatus = make(map[string]topology.GroupStatus)
	for _, founder := range opts.Founders {
		g.addresses[founder.Node.Name] = founder.Address
	}
	g.addresses[opts.Identity.NodeName] = opts.Network.AdvertiseAddress
	var err error
	g.machine, err = metaengine.NewMachine(g.rules, nil)
	if err != nil {
		return nil, err
	}
	g.watches = newWatchRegistry(opts.MaxWatchers)
	return g, nil
}

func (g *Group) configureServices() error {
	opts := g.opts
	var err error
	g.scheduler, err = internalscheduling.New(internalscheduling.Config{
		Host:            schedulingHost{g},
		Settings:        opts.Scheduling,
		Policy:          opts.RepairPolicy,
		CallbackTimeout: opts.CallbackTimeout,
		Held:            opts.HeldLoads,
		Logger:          opts.Logger,
		OnTransition:    g.recordScheduling,
	})
	if err != nil {
		return err
	}
	g.observer, err = observe.New(observe.Config{
		Source:            observationSource{g},
		Settings:          opts.Observation,
		Logger:            opts.Logger,
		OnGroupTransition: g.recordGroupHealth,
		OnNodeTransition:  g.recordNodeHealth,
	})
	if err != nil {
		return err
	}
	g.collector, err = observe.NewMetricsCollector(observe.MetricsConfig{
		Source:        &metricsSource{g: g, pins: map[topology.ReplicaKey]topology.Identity{}},
		Settings:      opts.Metrics,
		Local:         g.localMetrics,
		Identity:      g.currentIdentity,
		Switches:      func() []topology.VoterSwitchState { return g.state().VoterSwitches() },
		Enabled:       g.scheduler.Enabled,
		WakeScheduler: g.scheduler.Wake,
	})
	if err != nil {
		return err
	}
	g.controller, err = operator.New(g.OperationControllerConfig())
	if err != nil {
		return err
	}
	return nil
}

func (g *Group) configureTransport() error {
	opts := g.opts
	// The transport refers back to this group through its meta-only handler
	// seam.
	network, err := metatransport.New(metatransport.Options{
		Header: &metapb.Header{
			ClusterId:        opts.Identity.ClusterID,
			NodeName:         opts.Identity.NodeName,
			InstanceId:       opts.Identity.InstanceID,
			MemberId:         opts.Identity.MemberID,
			AdvertiseAddress: opts.Network.AdvertiseAddress,
		},
		Listener:         opts.Network.Listener,
		TLS:              opts.Network.TLS,
		ClientTLS:        opts.Network.ClientTLS,
		AllowPlaintext:   opts.Network.AllowPlaintext,
		Credential:       opts.Network.ClusterCredential,
		Handler:          g,
		Resolve:          g.resolveMember,
		SendTimeout:      opts.Timing.SendTimeout,
		SnapshotTimeout:  10 * time.Second,
		RetryInitial:     opts.Timing.PeerRetryInitial,
		RetryMax:         opts.Timing.PeerRetryMax,
		MaxSnapshotBytes: opts.Network.MaxSnapshotBytes,
		OnUnreachable: func(id uint64) {
			if engine := g.engine.Load(); engine != nil {
				engine.ReportUnreachable(id)
			}
		},
		OnSnapshot: func(id uint64, status raft.SnapshotStatus) {
			if engine := g.engine.Load(); engine != nil {
				engine.ReportSnapshot(id, status)
			}
		},
	})
	if err != nil {
		return err
	}
	g.transport = network
	g.refreshAddressHints()
	return nil
}

func (g *Group) openInitialEngine(startup metaStartup) error {
	opts := startup.options
	if opts.Identity.MemberID == 0 {
		return nil
	}
	mode := raftengine.Joining
	var peers []raft.Peer
	if !startup.pinFound && !opts.Join {
		mode = raftengine.Founding
		roles := g.foundingRoles
		for _, founder := range opts.Founders {
			if roles[founder.Node.Name] == topology.ReplicaVoter {
				peers = append(peers, raft.Peer{ID: founder.MemberID})
			}
		}
	}
	err := g.authorizeEngine(opts.Identity)
	if startup.pinFound && opts.Join && errors.Is(err, fault.ErrAuthorityFenced) {
		err = g.openControlStore(opts.Identity)
	} else if err == nil {
		err = g.openAuthorizedEngine(opts.Identity, mode, peers)
	}
	if err != nil {
		_ = g.transport.Stop(context.Background())
		return err
	}

	return nil
}

func (g *Group) openEngine(identity topology.Identity, mode raftengine.Mode, peers []raft.Peer) error {
	if err := g.authorizeEngine(identity); err != nil {
		return err
	}
	return g.openAuthorizedEngine(identity, mode, peers)
}

func (g *Group) authorizeEngine(identity topology.Identity) error {
	if g.opts.BeforeOpenEngine != nil {
		return g.opts.BeforeOpenEngine(identity)
	}
	return nil
}

func (g *Group) openAuthorizedEngine(identity topology.Identity, mode raftengine.Mode, peers []raft.Peer) error {
	store, err := metastore.Open(g.opts.DBPath)
	if err != nil {
		return err
	}
	retained, err := g.inspectRetainedState(store)
	if err != nil {
		store.Close()
		return err
	}
	machine, err := g.engineMachine(store, identity)
	if err != nil {
		store.Close()
		return err
	}
	var learners []raft.Peer
	if mode == raftengine.Founding {
		roles := g.foundingRoles
		for _, founder := range g.opts.Founders {
			if roles[founder.Node.Name] == topology.ReplicaLearner {
				learners = append(learners, raft.Peer{ID: founder.MemberID})
			}
		}
	}
	engine, err := raftengine.NewMeta(raftengine.Options{
		GroupID:          topology.MetaGroupID,
		NodeName:         identity.NodeName,
		ClusterID:        identity.ClusterID,
		InstanceID:       identity.InstanceID,
		ReplicaID:        identity.MemberID,
		MemberID:         identity.MemberID,
		ManagedAuthority: true,
		Mode:             mode,
		Peers:            peers,
		FoundingLearners: learners,
		Reads:            g.opts.Reads,
		Metrics:          g.opts.Metrics.Local(),
		Timing: raftengine.Timing{
			TickInterval:   g.opts.Timing.TickInterval,
			ElectionTicks:  g.opts.Timing.ElectionTicks,
			HeartbeatTicks: g.opts.Timing.HeartbeatTicks,
		},
		Store:             &metastore.Adapter{Store: store},
		Machine:           machine,
		Transport:         g.transport,
		Events:            g.opts.Events,
		Logger:            g.opts.Logger,
		SnapshotThreshold: g.opts.SnapshotThreshold,
	})
	if err != nil {
		store.Close()
		return err
	}
	if g.opts.AfterOpenEngine != nil {
		if err := g.opts.AfterOpenEngine(identity); err != nil {
			// NewMeta owns no workers; nothing can use this borrowed store yet.
			store.Close()
			return err
		}
	}
	g.mu.Lock()
	g.store = store
	g.serveOverride = retained.override
	g.recoveryGroups, g.recoveryVersion = retained.groups, retained.version
	g.machine = machine
	g.identity = identity
	g.controlOnly = false
	g.memberSeen = retained.seen || containsMember(engine.Status().ConfState, identity.MemberID)
	g.selfRemoval = nil
	g.mu.Unlock()
	g.engine.Store(engine)
	g.transport.SetMemberID(identity.MemberID)
	return nil
}

type retainedMetaState struct {
	groups   []topology.Group
	version  uint64
	override int
	seen     bool
}

func (g *Group) inspectRetainedState(store *metastore.Store) (retainedMetaState, error) {
	// Validate the persisted rule pin even when no Raft snapshot was produced.
	// This materialized state is never used to skip committed-log replay.
	var recoveryGroups []topology.Group
	var recoveryVersion uint64
	materialized, err := store.LoadState()
	if err != nil {
		return retainedMetaState{}, err
	}
	if len(materialized) > 0 {
		check, err := metaengine.NewMachine(g.rules, nil)
		if err != nil {
			return retainedMetaState{}, err
		}
		if err = check.Restore(context.Background(), materialized); err != nil {
			return retainedMetaState{}, err
		}
		recoveryVersion = check.Latest().Version
		recoveryGroups, err = check.RecoveryCatalog(context.Background())
		if err != nil {
			return retainedMetaState{}, err
		}
	}
	override, err := store.LoadOverride()
	if err != nil || override < 0 || override > g.rules.Limits.MaxNodes {
		if err != nil {
			return retainedMetaState{}, err
		}
		return retainedMetaState{}, fault.New(fault.Unreadable, "invalid persisted local serving override", nil)
	}
	seen, err := store.MemberSeen()
	if err != nil {
		return retainedMetaState{}, err
	}
	return retainedMetaState{groups: recoveryGroups, version: recoveryVersion, override: override, seen: seen}, nil
}

func (g *Group) engineMachine(store *metastore.Store, identity topology.Identity) (*metaengine.Machine, error) {
	machine, err := metaengine.NewMachine(g.rules, store)
	if err != nil {
		return nil, err
	}
	machine.OnApplied = func(index uint64, cs raftpb.ConfState) {
		views := machine.Views()
		target := views.Target
		g.watches.publish(views)
		g.recordScopedApplied(machine)
		g.observer.Wake()
		g.observeRemoval(identity, target, cs, index)
		if g.opts.OnApplied != nil {
			g.opts.OnApplied()
		}
	}
	return machine, nil
}

// Start acquires and starts meta participation under the supplied lifetime
// context.
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
	g.controller.Start(g.workCtx)
	if err := g.scheduler.Start(g.workCtx); err != nil {
		g.wg.Done()
		return g.startFailed(err)
	}
	if err := g.observer.Start(g.workCtx); err != nil {
		g.wg.Done()
		return g.startFailed(err)
	}
	if err := g.collector.Start(g.workCtx); err != nil {
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

// WaitReady waits for the meta startup gate without canceling accepted
// participation.
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
	stopped := g.selfRemoval != nil || g.quiescing || g.lifecycle == "stopped" || g.lifecycle == "stopping" ||
		g.ctx != nil && g.ctx.Err() != nil
	g.mu.RUnlock()
	if err == nil && stopped {
		return fault.ErrStopped
	}
	return err
}

// Rules returns the service's replicated cluster schema and capacity limits.
func (g *Group) Rules() topology.Rules { return g.rules }

// IsLeader reports a running, unfenced local meta leader from engine
// observations.
func (g *Group) IsLeader() bool {
	g.mu.RLock()
	running := g.started && !g.controlOnly && !g.quiescing && g.selfRemoval == nil && g.ctx != nil && g.ctx.Err() == nil
	g.mu.RUnlock()
	if !running {
		return false
	}
	engine := g.engine.Load()
	return engine != nil && engine.Running() && engine.Error() == nil && engine.IsLeader()
}

func (g *Group) state() *metaengine.Machine { g.mu.RLock(); defer g.mu.RUnlock(); return g.machine }
func (g *Group) currentIdentity() topology.Identity {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.identity
}
func (g *Group) resolveNode(ctx context.Context, name string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if g.opts.Network.Resolver != nil {
		address, err := g.opts.Network.Resolver.Resolve(ctx, name)
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
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
	// Ready messages can still carry a retired peer's final commit when progress
	// advances before the asynchronous sender drains. Retained documents may
	// resolve that destination, but never authorize incoming retired
	// identities.
	if name == "" {
		name = g.historicalMemberNode(id)
	}
	if name == "" {
		return "", fault.ErrIdentityMismatch
	}
	return g.resolveNode(ctx, name)
}

func (g *Group) historicalMemberNode(id uint64) string {
	for _, doc := range g.state().History(0, 0) {
		group := doc.MetaGroup()
		if group == nil {
			continue
		}
		for _, replica := range group.Replicas {
			if replica.ReplicaID == id {
				if replica.NodeName != "" {
					return replica.NodeName
				}
				break
			}
		}
	}
	return ""
}

func (g *Group) ensureRunning(ctx context.Context) error {
	return g.ensureRuntime(ctx, ordinaryRuntime)
}
func (g *Group) ensureRuntime(ctx context.Context, access runtimeAccess) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.ensureRuntimeLocked(access)
}

func (g *Group) ensureRuntimeLocked(access runtimeAccess) error {
	if g.terminal != nil {
		return g.terminal
	}
	running := g.started && g.ctx != nil && g.ctx.Err() == nil
	if !running || g.controlOnly || (g.quiescing || g.selfRemoval != nil) && access != shutdownRuntime {
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

func (g *Group) eligibleMemberID(name string) uint64 {
	return g.state().EligibleMemberID(name)
}

func (g *Group) sessionLive() bool {
	if g.ensureRunning(context.Background()) != nil {
		return false
	}
	identity := g.currentIdentity()
	if g.eligibleMemberID(identity.NodeName) != identity.MemberID {
		return false
	}
	g.sessionMu.Lock()
	defer g.sessionMu.Unlock()
	return g.ownSession != "" && time.Now().Before(g.ownDeadline)
}

// OwnSessionLive reports whether the local participant's observed session is
// live.
func (g *Group) OwnSessionLive() bool { return g.sessionLive() }
