package gquorum

import (
	"context"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/loadsnapshot"
	"github.com/asalimonov/gquorum/internal/managed"
	"github.com/asalimonov/gquorum/internal/metaservice"
	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	recoveryruntime "github.com/asalimonov/gquorum/internal/recovery/runtime"
	"github.com/asalimonov/gquorum/internal/watchstate"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/memstore"
	"github.com/asalimonov/gquorum/storage/walstore"
	"github.com/asalimonov/gquorum/topology"
)

// Node owns one cluster participant, its _meta service and local load replicas.
// Start's context owns its lifetime. WaitReady and individual operation
// contexts
// bound waits; they never become the lifetime of an accepted load replica.
type Node interface {
	// Lifecycle. Start accepts the participant lifetime; Stop joins teardown.
	// WaitReady only bounds the caller's wait, and RequestStop starts teardown.

	// Start acquires resources and begins participation without waiting for
	// quorum readiness.
	Start(context.Context) error
	// WaitReady waits for this participant's admission and local readiness.
	WaitReady(context.Context) error
	// Stop requests teardown and waits for owned resources to be released.
	Stop(context.Context) error
	// RequestStop begins teardown without waiting for it to finish.
	RequestStop()

	// Identity and local observations.

	// Identity returns this participant's durable and process identity.
	Identity() topology.Identity
	// Status returns a local lifecycle snapshot.
	Status() NodeStatus
	// Watch seeds local state and coalesces updates until cancellation or final
	// stop.
	Watch(context.Context) (<-chan NodeObservation, error)
	// Cluster returns the administration view owned by this Node.
	Cluster() Cluster
	// Recovery configures local maintenance without starting or stopping Node.
	// Join shutdown before inspecting or archiving; Status/List can run online.
	Recovery(...recovery.Options) (Recovery, error)

	// Managed load replicas. StartLoadGroup and StopLoadGroup address an exact
	// replica; LoadGroup and LoadGroupIDs inspect currently published handles.

	// StartLoadGroup starts an approved local replica and returns its handle.
	StartLoadGroup(context.Context, load.ReplicaRef) (*load.Group, error)
	// LoadGroup looks up a published local replica by group ID.
	LoadGroup(string) (*load.Group, bool)
	// LoadGroupIDs lists published local group IDs.
	LoadGroupIDs() []string
	// LoadSnapshots returns the borrowed load-transfer service, or nil for a
	// metadata-only Node. Options is available before Start; ingress is not.
	LoadSnapshots() snapshot.Service
	// StopLoadGroup holds an exact local replica without deleting its durable
	// state.
	StopLoadGroup(context.Context, load.ReplicaRef) error
	// ParkedLoads reports replicas held out of service with their evidence.
	ParkedLoads() map[string]ParkedLoad

	// Load ingress and approved effects. The transport adapter authenticates
	// senders before calling Node, which routes requests to exact replicas.

	// ReceiveLoadRaft accepts a batch from an authenticated load peer.
	ReceiveLoadRaft(context.Context, load.RaftBatch) error
	// ReadLoadReplica queries an exact local replica's status.
	ReadLoadReplica(context.Context, load.ReplicaQuery) (load.ReplicaStatus, error)
	// ApplyLoadAction applies an approved action to an exact local replica.
	ApplyLoadAction(context.Context, load.ActionRequest) (load.ActionResult, error)
	// InstallLoadFence applies an approved local participation fence.
	InstallLoadFence(context.Context, load.FenceRequest) (load.FenceResult, error)
	// ReportLoadDelivery records a transport delivery result.
	ReportLoadDelivery(context.Context, load.DeliveryReport) error

	// Local session participation and serve override.

	// ActivateSession starts this participant's cluster session.
	ActivateSession(context.Context) (topology.Session, error)
	// KeepAlive renews the identified session.
	KeepAlive(context.Context, string) error
	// DeactivateSession ends the identified session.
	DeactivateSession(context.Context, string) error
	// OwnSessionLive reports the locally known state of this participant's
	// session.
	OwnSessionLive() bool
	// ServeOverride returns this participant's current serve override.
	ServeOverride() int
	// SetServeOverride changes this participant's serve override.
	SetServeOverride(context.Context, int) error

	// Local scheduler observation and control.

	// SchedulingStatus reports this participant's scheduler state.
	SchedulingStatus() topology.SchedulingStatus
	// SetSchedulingEnabled changes whether this participant runs scheduling.
	SetSchedulingEnabled(context.Context, bool) error
}

type node struct {
	observationMu           sync.Mutex
	observations            *watchstate.Latest[NodeObservation]
	observation             NodeObservation
	observationsFinished    bool
	opts                    NodeOptions
	metaOpts                metaservice.Options
	cluster                 *clusterView
	snapshots               snapshot.Service
	startGate               sync.Mutex
	authorityGate           sync.Mutex
	authority               *authoritystore.Store
	authorityFreshness      authoritystore.Freshness
	metaAuthorityWork       *metaAuthorityWork
	readmissionWork         *readmissionWork
	readmissionFenceWork    *readmissionFenceWork
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
	storageOwner            *recoverydisk.Owner
	recoveryJournal         *recoverydisk.Journal
	recoveryCoordinator     *recoveryruntime.Coordinator
	recoveryManifest        *model.LocalManifest
	limitsExplicit          bool
	ownedMemory             *memstore.Store
	lifecycle               *lifecycleRunner
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
	resolved, err := resolveNodeOptions(input)
	if err != nil {
		return nil, err
	}
	opts, meta := resolved.node, resolved.meta
	n := &node{
		opts:           opts,
		limitsExplicit: input.Limits != (topology.Limits{}),
		metaOpts:       meta,
		state:          NodeNew,
		readySignal:    make(chan struct{}),
		wake:           make(chan struct{}, 1),
		removal:        make(chan struct{}, 1),
		retryStop:      make(chan *nodeStopAttempt, 1),
		stopped:        make(chan struct{}),
	}
	n.cluster = &clusterView{node: n}
	if opts.Loads.Factory != nil {
		n.snapshots = loadsnapshot.NewService(opts.Loads.SnapshotTransfer, n.snapshotCoordinator)
	}
	n.observation = NodeObservation{Revision: 1, Status: n.Status()}
	n.observations = watchstate.New(opts.MaxWatchers, n.observation, cloneNodeObservation)
	return n, nil
}

// FoundingIDs derives deterministic initial meta member IDs from the cluster
// and founder names.
func FoundingIDs(clusterID string, names []string) (map[string]uint64, error) {
	return metaservice.FoundingIDs(clusterID, names)
}

// LoadStoredIdentity reads the _meta.bolt pin in storageDir without creating
// storage. The returned durable identity excludes the process InstanceID.
func LoadStoredIdentity(storageDir string) (topology.IdentityLookup, error) {
	o := NodeOptions{StorageDir: storageDir}
	if err := validateStorageDir(o); err != nil {
		return topology.IdentityLookup{}, err
	}
	return metaservice.LoadStoredIdentity(o.metaDBPath())
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

type serviceAccess uint8

const (
	readService serviceAccess = iota
	workService
)

// service keeps reads available after fencing. Work admission is separate from
// optional readiness: approved load actions must work during cluster admission.
func (n *node) service(ctx context.Context, access serviceAccess) (*metaservice.Group, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	n.mu.RLock()
	m, refused := n.meta, n.stopping || n.fenced || n.terminal != nil
	n.mu.RUnlock()
	if access == workService && refused {
		return nil, fault.ErrStopped
	}
	if m == nil {
		return nil, fault.ErrReplicaNotReady
	}
	if access == workService {
		observed, err := m.Lifecycle()
		removal := observed.Removal
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
	if _, err := n.service(ctx, workService); err != nil {
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

// Only granted action/fence ingress uses this retained owner. The Host keeps
// creation and native operations fenced while validating exact cleanup grants.
func (n *node) authorityHost(ctx context.Context) (*managed.Host, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.stopping || n.terminal != nil || n.ctx == nil || n.ctx.Err() != nil {
		return nil, fault.ErrStopped
	}
	if n.pending != nil || n.meta == nil {
		return nil, fault.ErrReplicaNotReady
	}
	if n.host == nil {
		return nil, fault.New(fault.NoController, "node has no load hosting configured", nil)
	}
	return n.host, nil
}
