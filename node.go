package gquorum

import (
	"context"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/managed"
	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/walstore"
	"github.com/asalimonov/gquorum/topology"
)

// Node owns one cluster participant, its _meta service and local load replicas.
// Start's context owns its lifetime. WaitReady and individual operation contexts
// bound waits; they never become the lifetime of an accepted load replica.
type Node interface {
	Start(context.Context) error
	WaitReady(context.Context) error
	Stop(context.Context) error
	RequestStop()
	Identity() topology.Identity
	Status() NodeStatus
	Cluster() Cluster
	StartLoadGroup(context.Context, load.ReplicaRef) (*load.Group, error)
	LoadGroup(string) (*load.Group, bool)
	LoadGroupIDs() []string
	StopLoadGroup(context.Context, load.ReplicaRef) error
	ParkedLoads() map[string]ParkedLoad
	ReceiveLoadRaft(context.Context, load.RaftBatch) error
	ReadLoadReplica(context.Context, load.ReplicaQuery) (load.ReplicaStatus, error)
	ApplyLoadAction(context.Context, load.ActionRequest) (load.ActionResult, error)
	ReportLoadDelivery(context.Context, load.DeliveryReport) error
	ActivateSession(context.Context) (topology.Session, error)
	KeepAlive(context.Context, string) error
	DeactivateSession(context.Context, string) error
	OwnSessionLive() bool
	ServeOverride() int
	SetServeOverride(context.Context, int) error
	SchedulingStatus() topology.SchedulingStatus
	SetSchedulingEnabled(context.Context, bool) error
}

type node struct {
	opts                    NodeOptions
	metaOpts                metaservice.Options
	cluster                 *clusterView
	startGate               sync.Mutex
	mu                      sync.RWMutex
	state                   NodeState
	terminal                error
	lastError               error
	ctx                     context.Context
	cancel                  context.CancelFunc
	startedAt               time.Time
	meta                    *metaservice.Group
	host                    *managed.Host
	store                   storage.Store
	retirement              storage.RetirementStore
	ownedWAL                *walstore.Store
	policies                *lifecycleRunner
	pending                 *topology.Identity
	fenced, ready, stopping bool
	readySignal             chan struct{}
	readyOnce               sync.Once
	wake                    chan struct{}
	removal                 chan struct{}
	wg                      sync.WaitGroup
	stopAttempt             *nodeStopAttempt
	retryStop               chan *nodeStopAttempt
	stopped                 chan struct{}
}

// NewNode validates and copies settings without acquiring storage, taking the
// supplied listener or starting goroutines. Stop never closes borrowed stores,
// load transports, application listeners or the event bus.
func NewNode(input NodeOptions) (Node, error) {
	opts, meta, err := resolveNodeOptions(input)
	if err != nil {
		return nil, err
	}
	n := &node{opts: opts, metaOpts: meta, state: NodeNew, readySignal: make(chan struct{}), wake: make(chan struct{}, 1), removal: make(chan struct{}, 1), retryStop: make(chan *nodeStopAttempt, 1), stopped: make(chan struct{})}
	n.cluster = &clusterView{node: n}
	return n, nil
}

func FoundingIDs(clusterID string, names []string) (map[string]uint64, error) {
	return metaservice.FoundingIDs(clusterID, names)
}
func LoadStoredIdentity(path string) (topology.Identity, bool, error) {
	return metaservice.LoadStoredIdentity(path)
}
func (n *node) Cluster() Cluster { return n.cluster }
func (n *node) Identity() topology.Identity {
	n.mu.RLock()
	m := n.meta
	id := n.opts.Identity
	n.mu.RUnlock()
	if m != nil {
		return m.Identity()
	}
	return id
}
func (n *node) signal() {
	select {
	case n.wake <- struct{}{}:
	default:
	}
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h != nil {
		h.Wake()
	}
}

// service keeps reads available after fencing. Work admission is separate from
// optional readiness: approved load actions must work during cluster admission.
func (n *node) service(ctx context.Context, work bool) (*metaservice.Group, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	n.mu.RLock()
	m, refused := n.meta, n.stopping || n.fenced || n.terminal != nil
	n.mu.RUnlock()
	if work && refused {
		return nil, fault.ErrStopped
	}
	if m == nil {
		return nil, fault.ErrReplicaNotReady
	}
	if work {
		_, err, removal := m.Lifecycle()
		if err != nil {
			return nil, err
		}
		if removal != nil {
			return nil, fault.ErrStopped
		}
	}
	return m, nil
}

func (n *node) loadHost(ctx context.Context) (*managed.Host, error) {
	if _, err := n.service(ctx, true); err != nil {
		return nil, err
	}
	n.mu.RLock()
	h, pending := n.host, n.pending != nil
	n.mu.RUnlock()
	if pending {
		return nil, fault.ErrReplicaNotReady
	}
	if h == nil {
		return nil, fault.New(fault.NoController, "node has no load hosting configured", nil)
	}
	return h, nil
}
