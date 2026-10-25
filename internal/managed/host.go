// Package managed owns application replica lifetimes inside one admitted node.
// Its configuration is private library composition, never a caller-facing API.
package managed

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/loadhandle"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/internal/observe"
	"github.com/asalimonov/gquorum/internal/raftengine"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

const (
	defaultEffectTimeout    = 10 * time.Second
	defaultCallbackTimeout  = 10 * time.Second
	defaultRecoveryInterval = 250 * time.Millisecond
)

// Config supplies private authority, application, storage and transport
// dependencies for a node's replicas.
type Config struct {
	// OnChange only wakes the owning Node; it runs outside host locks.
	OnChange           func()
	RecordRecoverySeed func(context.Context, model.SeedProgress) error
	// Authority becomes available once a fresh join receives its member
	// identity.
	// The Node owns this store beyond all Host work and callbacks.
	Authority            func() *authoritystore.Store
	ValidateGrant        func(context.Context, topology.AuthorityGrant) error
	ValidateCourier      func(context.Context, topology.AuthorityGrant, topology.Identity) error
	ResolveAuthorityPeer func(context.Context, topology.AuthorityGrant, topology.AuthorityTarget) (load.Peer, error)
	RecordFence          func(context.Context, topology.FenceEvidence) error
	AuthorityFloors      func(context.Context) ([]topology.AuthorityGrant, error)
	ForgetGrantedReplica func(context.Context, topology.AuthorityGrant, uint64) error
	Identity             func() topology.Identity
	MetaDBPath           string
	AuthorizeReadmission func(context.Context, Readmission) error
	Scope                func(context.Context, storage.Identity) (metaengine.LoadScope, error)
	LookupRecord         func(context.Context, string) (metastore.ReplicaLookup, error)
	RecordReplica        func(context.Context, string, metastore.ReplicaRecord, uint64) error
	Catalog              func(context.Context, string) ([]storage.Identity, error)
	Progress             func(context.Context) (topology.ProgressPage, error)
	RetireApplication    func(context.Context, load.RetirementRequest) (load.RetirementResult, error)
	Collection           CollectionCallbacks
	Retirement           storage.RetirementStore
	Retention            func(context.Context, lifecycle.RetentionInput) (lifecycle.RetentionDecision, error)
	RetirementInterval   time.Duration
	Assignments          func(context.Context) ([]load.ReplicaRef, error)
	Factory              func(context.Context, load.ReplicaSpec) (load.Application, error)
	Store                storage.Store
	MemoryStore          storage.Store           // optional: groups of topology.StorageMemory
	MemoryRetirement     storage.RetirementStore // required with MemoryStore
	ReseatOwn            func(ctx context.Context, identity storage.Identity) error
	Transport            load.Transport
	ResolvePeer          func(context.Context, load.ReplicaRef) (load.Peer, error)
	ValidatePeer         func(context.Context, load.Peer) error
	ValidateNode         func(context.Context, topology.Identity) error
	Engine               load.EngineOptions
	Metrics              internalmetrics.Settings
	EffectTimeout        time.Duration
	CallbackTimeout      time.Duration
	RecoveryInterval     time.Duration
	Events               *events.Bus
	Logger               *slog.Logger
}

// ReplicaStatus reports the identity, incarnation, run and local state of a
// managed replica.
type ReplicaStatus struct {
	InstanceID, RunID string
	Identity          storage.Identity
	State             string
	ErrorCode         fault.Code
	Error             string
}

// Host owns exact replica reservations, application lifetimes, recovery and
// retirement work.
type Host struct {
	recoveryManifest            *model.LocalManifest
	recoveryImport              *model.ImportState
	recoveryReleased            bool
	recoveryGate                chan struct{}
	recoveryResumes             map[string]bool
	runFailures                 map[storage.Identity]ReplicaStatus
	actionWorks                 map[string]*actionWork
	fenceWorks                  map[string]*fenceWork
	cfg                         Config
	mu                          sync.Mutex
	ctx                         context.Context
	workCancel                  context.CancelFunc
	workDone                    chan struct{}
	wake                        chan struct{}
	started, quiescing, stopped bool
	slots                       map[string]*replica
	holds                       map[storage.Identity]bool
	metricsPeers                map[storage.Identity]load.Peer
	observationPool             *observe.ReadPool
	observationCursor           int
	recoveryCursor              string
	observationGroups           map[string]*groupOperation
	parked                      map[string]ParkedReplica
	stopGate                    chan struct{}
	deliverySeq                 uint64
	operations                  map[string]*groupOperation
	operationCount              int
	operationsDone              chan struct{}
	operationContext            context.Context
	authorityContext            context.Context
	authorityCancel             context.CancelFunc
	authorityCount              int
	authorityDone               chan struct{}
	authorityClosed             bool
	workWG                      sync.WaitGroup
	retentionGate               chan struct{}
	readmissionGate             chan struct{}
	readmissionCancel           context.CancelFunc
	readmissionOwned            map[storage.Identity]bool
	shutdownRequested           bool
	ownReseats                  map[storage.Identity]bool // reseat loop running or refusal logged
	reseatWG                    sync.WaitGroup
}

// New validates host dependencies and initializes bounded private ownership
// state.
func New(cfg Config) (*Host, error) {
	if err := validateHostConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.EffectTimeout == 0 {
		cfg.EffectTimeout = defaultEffectTimeout
	}
	if cfg.CallbackTimeout == 0 {
		cfg.CallbackTimeout = defaultCallbackTimeout
	}
	if cfg.RecoveryInterval == 0 {
		cfg.RecoveryInterval = defaultRecoveryInterval
	}
	if cfg.RetirementInterval == 0 {
		cfg.RetirementInterval = time.Minute
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	var err error
	if cfg.Metrics, err = cfg.Metrics.Resolve(); err != nil {
		return nil, err
	}
	h := &Host{
		cfg:         cfg,
		runFailures: make(map[storage.Identity]ReplicaStatus),
		slots:       make(map[string]*replica),
		holds:       make(map[storage.Identity]bool),
		parked:      make(map[string]ParkedReplica),
		stopGate:    make(chan struct{}, 1),
		wake:        make(chan struct{}, 1),
	}
	h.operations, h.operationsDone = make(map[string]*groupOperation), make(chan struct{})
	h.observationGroups = make(map[string]*groupOperation)
	close(h.operationsDone)
	h.authorityDone = make(chan struct{})
	close(h.authorityDone)
	h.retentionGate = make(chan struct{}, 1)
	h.ownReseats = make(map[storage.Identity]bool)
	h.readmissionGate = make(chan struct{}, 1)
	// Validate engine settings without opening storage or invoking application
	// code.
	check := h.engineOptions(&replica{
		identity: storage.Identity{NodeName: "validation", ClusterID: "validation", GroupID: "validation",
			ReplicaID: 1},
		node:  topology.Identity{MemberID: 1, InstanceID: "validation"},
		store: cfg.Store,
	}, load.Application{}, raftengine.Joining, nil)
	if err := check.Validate(); err != nil {
		return nil, err
	}
	h.observationPool, err = observe.NewReadPool(cfg.Metrics.MaxParallelReads)
	if err != nil {
		return nil, err
	}
	return h, nil
}

// Start accepts the host lifetime and starts recovery and retired-state
// maintenance.
func (h *Host) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.quiescing || h.stopped {
		return fault.ErrStopped
	}
	if h.started {
		if h.ctx.Err() != nil {
			return fault.ErrStopped
		}
		return nil
	}
	h.started, h.ctx = true, ctx
	work, cancel := context.WithCancel(ctx)
	h.workCancel, h.workDone = cancel, make(chan struct{})
	h.operationContext = work
	h.authorityContext, h.authorityCancel = context.WithCancel(ctx)
	h.workWG.Add(1)
	go func() { defer h.workWG.Done(); h.maintain(work) }()
	h.startCollectionLoopLocked()
	go func() { h.workWG.Wait(); close(h.workDone) }()
	return nil
}

// Quiesce prevents new starts and joins participation maintenance. Existing
// replicas are joined by Stop, after upstream effect producers have quiesced.
func (h *Host) Quiesce(ctx context.Context) error {
	h.mu.Lock()
	h.quiescing = true
	done := h.workDone
	operations := h.operationsDone
	if h.workCancel != nil {
		h.workCancel()
	}
	h.mu.Unlock()
	// Native status calls have their own bounded pool so a stuck status adapter
	// cannot consume the independent temporal collector's permits. Join even
	// callbacks whose observation caller already returned with partial results.
	if err := h.observationPool.Stop(ctx); err != nil {
		return err
	}
	// No reseat loop starts once quiescing is set; running loops use the
	// canceled work context.
	reseats := make(chan struct{})
	go func() { h.reseatWG.Wait(); close(reseats) }()
	for _, wait := range []<-chan struct{}{done, operations, reseats} {
		if wait == nil {
			continue
		}
		select {
		case <-wait:
		case <-ctx.Done():
			return fault.New(fault.ShutdownTimeout, "managed work has not joined", ctx.Err())
		}
	}
	return nil
}

// Fence immediately cancels replica participation and refuses ordinary
// operations. Granted host cleanup and fencing retain their separate
// administrative lifetime.
// Joining and application Close remain owned by FenceParticipation or Stop.
// Removal cannot wait for a noncooperative lifecycle hook before stopping Raft
// work.
func (h *Host) Fence() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.quiescing = true
	if h.workCancel != nil {
		h.workCancel()
	}
	for _, slot := range h.slots {
		slot.cancel()
	}
}

// Stop quiesces work and joins replica engines and application releases.
func (h *Host) Stop(ctx context.Context) error {
	h.mu.Lock()
	h.shutdownRequested = true
	if h.readmissionCancel != nil {
		h.readmissionCancel()
	}
	h.mu.Unlock()
	h.Fence()
	if err := h.closeAuthority(ctx); err != nil {
		return err
	}
	if err := h.Quiesce(ctx); err != nil {
		return err
	}
	select {
	case h.readmissionGate <- struct{}{}:
		defer func() { <-h.readmissionGate }()
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "readmission work has not joined", ctx.Err())
	}
	return h.stopReplicas(ctx)
}

// FenceParticipation joins replicas while retaining exact granted host cleanup
// and fence requests. Full Stop or Readmit closes that remaining lifetime.
func (h *Host) FenceParticipation(ctx context.Context) error {
	h.Fence()
	return h.stopReplicas(ctx)
}

// FenceReadmission joins the retired owner's remaining callbacks without
// closing
// its ability to reconcile a later approved readmission. Node owns this call
// outside normal administration, whose admission Readmit has already closed.
func (h *Host) FenceReadmission(ctx context.Context) error {
	h.mu.Lock()
	if h.readmissionCancel != nil {
		h.readmissionCancel()
	}
	h.mu.Unlock()
	h.Fence()
	if err := h.closeAuthority(ctx); err != nil {
		return err
	}
	select {
	case h.readmissionGate <- struct{}{}:
		defer func() { <-h.readmissionGate }()
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "readmission callbacks have not joined", ctx.Err())
	}
	return h.stopReplicas(ctx)
}

func (h *Host) closeAuthority(ctx context.Context) error {
	h.mu.Lock()
	h.authorityClosed = true
	if h.authorityCancel != nil {
		h.authorityCancel()
	}
	done := h.authorityDone
	h.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "managed authority work has not joined", ctx.Err())
	}
}

func (h *Host) stopReplicas(ctx context.Context) error {
	select {
	case h.stopGate <- struct{}{}:
		defer func() { <-h.stopGate }()
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "managed shutdown already in progress", ctx.Err())
	}
	if err := h.Quiesce(ctx); err != nil {
		return err
	}
	h.mu.Lock()
	waits := make([]*releaseAttempt, 0, len(h.slots))
	for _, slot := range h.slots {
		waits = append(waits, h.beginStopLocked(slot))
	}
	h.mu.Unlock()
	var failures []error
	for _, attempt := range waits {
		if err := waitRelease(ctx, attempt); err != nil {
			failures = append(failures, err)
		}
	}
	if err := errors.Join(failures...); err != nil {
		return err
	}
	h.mu.Lock()
	h.stopped = len(h.slots) == 0
	h.mu.Unlock()
	return nil
}

// Lookup returns a published ready handle for the named group, if present.
func (h *Host) Lookup(id string) (*loadhandle.Handle, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.slots[id]
	if s == nil || s.state != "ready" || s.handle == nil {
		return nil, false
	}
	return s.handle, true
}

// IDs returns sorted group names with published ready handles.
func (h *Host) IDs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	ids := make([]string, 0, len(h.slots))
	for id, slot := range h.slots {
		if slot.state == "ready" && slot.handle != nil {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// Parked returns an owned map of refused local replica boots.
func (h *Host) Parked() map[string]ParkedReplica {
	h.mu.Lock()
	defer h.mu.Unlock()
	return maps.Clone(h.parked)
}

// Held returns an ordered list of exact local stop holds.
func (h *Host) Held() []storage.Identity {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([]storage.Identity, 0, len(h.holds))
	for id := range h.holds {
		result = append(result, id)
	}
	slices.SortFunc(result, func(a, b storage.Identity) int {
		if a.GroupID < b.GroupID {
			return -1
		}
		if a.GroupID > b.GroupID {
			return 1
		}
		if a.ReplicaID < b.ReplicaID {
			return -1
		}
		if a.ReplicaID > b.ReplicaID {
			return 1
		}
		return 0
	})
	return result
}

// Snapshot returns owned local status for current replica slots and retained
// run failures.
func (h *Host) Snapshot() []ReplicaStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	statuses := make(map[storage.Identity]ReplicaStatus)
	for _, slot := range h.slots {
		s := statusForReplica(slot)
		statuses[s.Identity] = s
	}
	h.addHeldStatuses(statuses)
	for _, p := range h.parked {
		if _, found := statuses[p.Identity]; !found {
			statuses[p.Identity] = ReplicaStatus{
				Identity:  p.Identity,
				State:     "parked",
				ErrorCode: fault.ReplicaNotReady,
				Error:     string(p.Reason),
			}
		}
	}
	result := make([]ReplicaStatus, 0, len(statuses))
	for _, s := range statuses {
		result = append(result, s)
	}
	slices.SortFunc(result, func(a, b ReplicaStatus) int {
		if a.Identity.GroupID < b.Identity.GroupID {
			return -1
		}
		if a.Identity.GroupID > b.Identity.GroupID {
			return 1
		}
		if a.Identity.ReplicaID < b.Identity.ReplicaID {
			return -1
		}
		if a.Identity.ReplicaID > b.Identity.ReplicaID {
			return 1
		}
		return 0
	})
	return result
}

// Wake coalesces a request for the maintenance loop to recheck assignments.
func (h *Host) Wake() {
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

func (h *Host) maintain(ctx context.Context) {
	tick := time.NewTicker(min(h.cfg.RecoveryInterval, h.cfg.EffectTimeout))
	defer tick.Stop()
	var nextRecovery time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-h.wake:
			nextRecovery = time.Time{}
		}
		h.expireDeliveries(time.Now())
		if time.Now().Before(nextRecovery) {
			continue
		}
		nextRecovery = time.Now().Add(h.cfg.RecoveryInterval)
		bounded, cancel := context.WithTimeout(ctx, h.cfg.EffectTimeout)
		_ = h.RecoverAssignments(bounded)
		cancel()
	}
}

// RecoverAssignments reserves independent starts without waiting for elections
// or slow factories in an earlier group. Rotate after each attempted reservation
// so a blocked authority reconciliation cannot consume every subsequent round.
// Holds belong to this Host lifetime.
func (h *Host) RecoverAssignments(ctx context.Context) error {
	if h.cfg.Assignments == nil {
		return nil
	}
	refs, err := h.cfg.Assignments(ctx)
	if err != nil {
		return err
	}
	refs = slices.Clone(refs)
	slices.SortFunc(refs, func(a, b load.ReplicaRef) int { return cmp.Compare(a.GroupID, b.GroupID) })
	h.mu.Lock()
	start := max(0, slices.IndexFunc(refs,
		func(ref load.ReplicaRef) bool { return ref.GroupID > h.recoveryCursor }))
	h.mu.Unlock()
	var failures []error
	for i := range refs {
		position := (start + i) % len(refs)
		ref := refs[position]
		h.mu.Lock()
		slot := h.slots[ref.GroupID]
		needsStop := slot != nil && slot.identity.ReplicaID == ref.ReplicaID && slot.state == "ready" &&
			slot.core != nil && !slot.core.Running()
		// An engine stopped by lifetime cancellation belongs to the shutdown
		// coordinator. Check after observing Running: a late recovery read must
		// not consume a Close failure before the owner's first Stop attempt.
		if err := ctx.Err(); err != nil {
			h.mu.Unlock()
			return err
		}
		if !h.started || h.quiescing || h.stopped || h.ctx.Err() != nil {
			h.mu.Unlock()
			return fault.ErrStopped
		}
		if needsStop {
			h.beginStopLocked(slot)
		}
		h.recoveryCursor = ref.GroupID
		h.mu.Unlock()
		if _, err := h.reserveStart(ctx, ref, StartRecovery); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func validateHostConfig(cfg Config) error {
	if cfg.Authority == nil || cfg.ValidateGrant == nil || cfg.ValidateCourier == nil ||
		cfg.AuthorityFloors == nil {
		return fault.New(fault.Invalid, "managed authority dependencies are required", nil)
	}
	if cfg.Identity == nil || cfg.Scope == nil || cfg.LookupRecord == nil || cfg.RecordReplica == nil ||
		cfg.Factory == nil || cfg.Store == nil || cfg.Transport == nil || cfg.ResolvePeer == nil ||
		cfg.ValidatePeer == nil || cfg.ValidateNode == nil {
		return fault.New(fault.Invalid, "managed host dependencies are required", nil)
	}
	return validateHostRetirementConfig(cfg)
}

func validateHostRetirementConfig(cfg Config) error {
	if cfg.Catalog == nil || cfg.Progress == nil || cfg.ForgetGrantedReplica == nil || cfg.Retirement == nil ||
		cfg.Retention == nil {
		return fault.New(fault.Invalid, "managed approval and retirement dependencies are required", nil)
	}
	if (cfg.MemoryStore == nil) != (cfg.MemoryRetirement == nil) {
		return fault.New(fault.Invalid, "memory storage requires both Store and Retirement", nil)
	}
	if cfg.EffectTimeout < 0 || cfg.CallbackTimeout < 0 || cfg.RecoveryInterval < 0 || cfg.RetirementInterval < 0 {
		return fault.New(fault.Invalid, "negative managed host duration", nil)
	}
	return nil
}

func statusForReplica(slot *replica) ReplicaStatus {
	s := ReplicaStatus{Identity: slot.identity, InstanceID: slot.node.InstanceID, State: slot.state}
	if slot.core != nil {
		s.RunID = slot.core.RunID()
	}
	err := slot.err
	if err == nil && slot.core != nil {
		err = slot.core.Error()
		if err != nil {
			s.State = "failed"
		}
	}
	if err != nil {
		s.Error = err.Error()
		var f *fault.Error
		if errors.As(err, &f) {
			s.ErrorCode = f.Code
		}
	}
	return s
}

func (h *Host) addHeldStatuses(statuses map[storage.Identity]ReplicaStatus) {
	for id := range h.holds {
		if _, found := statuses[id]; !found {
			if failed, ok := h.runFailures[id]; ok {
				statuses[id] = failed
			} else {
				statuses[id] = ReplicaStatus{Identity: id, State: "held"}
			}
		}
	}
}
