// Package managed owns application replica lifetimes inside one admitted node.
// Its configuration is private library composition, never a caller-facing API.
package managed

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/loadhandle"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/policy"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type Config struct {
	Identity                                         func() topology.Identity
	MetaDBPath                                       string
	AuthorizeReadmission                             func(context.Context, Readmission) error
	Scope                                            func(context.Context, storage.Identity) (metaengine.LoadScope, error)
	LookupRecord                                     func(context.Context, string) (metastore.ReplicaRecord, bool, error)
	RecordReplica                                    func(context.Context, string, metastore.ReplicaRecord, uint64) error
	Authorize                                        func(context.Context, topology.AuthorizedStep) error
	ActionTargets                                    func(context.Context, topology.AuthorizedStep) ([]storage.Identity, error)
	Catalog                                          func(context.Context, string) ([]storage.Identity, error)
	Active                                           func(context.Context) (topology.Document, error)
	ForgetReplica                                    func(context.Context, topology.AuthorizedStep, uint64) error
	RetireApplication                                func(context.Context, load.RetirementRequest) (load.RetirementResult, error)
	Retirement                                       storage.RetirementStore
	Retention                                        func(context.Context, policy.RetentionInput) (policy.RetentionDecision, error)
	RetirementInterval                               time.Duration
	Assignments                                      func(context.Context) ([]load.ReplicaRef, error)
	Factory                                          func(context.Context, load.ReplicaSpec) (load.Application, error)
	Store                                            storage.Store
	Transport                                        load.Transport
	ResolvePeer                                      func(context.Context, load.ReplicaRef) (load.Peer, error)
	ValidatePeer                                     func(context.Context, load.Peer) error
	ValidateNode                                     func(context.Context, topology.Identity) error
	Engine                                           load.EngineOptions
	EffectTimeout, CallbackTimeout, RecoveryInterval time.Duration
	Events                                           *events.Bus
	Logger                                           *slog.Logger
}

type ReplicaStatus struct {
	Identity  storage.Identity
	State     string
	ErrorCode fault.Code
	Error     string
}

type Host struct {
	cfg                         Config
	mu                          sync.Mutex
	ctx                         context.Context
	workCancel                  context.CancelFunc
	workDone                    chan struct{}
	wake                        chan struct{}
	started, quiescing, stopped bool
	slots                       map[string]*replica
	holds                       map[storage.Identity]bool
	parked                      map[string]ParkedReplica
	stopGate                    chan struct{}
	deliverySeq                 uint64
	operations                  map[string]*groupOperation
	operationCount              int
	operationsDone              chan struct{}
	operationContext            context.Context
	workWG                      sync.WaitGroup
	membership                  *membership.Coordinator
	retentionGate               chan struct{}
	readmissionGate             chan struct{}
	readmissionCancel           context.CancelFunc
	readmissionOwned            map[storage.Identity]bool
	shutdownRequested           bool
}

func New(cfg Config) (*Host, error) {
	if cfg.Identity == nil || cfg.Scope == nil || cfg.LookupRecord == nil || cfg.RecordReplica == nil || cfg.Factory == nil || cfg.Store == nil || cfg.Transport == nil || cfg.ResolvePeer == nil || cfg.ValidatePeer == nil || cfg.ValidateNode == nil {
		return nil, fault.New(fault.Invalid, "managed host dependencies are required", nil)
	}
	if cfg.Authorize == nil || cfg.ActionTargets == nil || cfg.Catalog == nil || cfg.Active == nil || cfg.ForgetReplica == nil || cfg.Retirement == nil || cfg.Retention == nil {
		return nil, fault.New(fault.Invalid, "managed approval and retirement dependencies are required", nil)
	}
	if cfg.EffectTimeout < 0 || cfg.CallbackTimeout < 0 || cfg.RecoveryInterval < 0 || cfg.RetirementInterval < 0 {
		return nil, fault.New(fault.Invalid, "negative managed host duration", nil)
	}
	if cfg.EffectTimeout == 0 {
		cfg.EffectTimeout = 10 * time.Second
	}
	if cfg.CallbackTimeout == 0 {
		cfg.CallbackTimeout = 10 * time.Second
	}
	if cfg.RecoveryInterval == 0 {
		cfg.RecoveryInterval = 250 * time.Millisecond
	}
	if cfg.RetirementInterval == 0 {
		cfg.RetirementInterval = time.Minute
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	h := &Host{cfg: cfg, slots: make(map[string]*replica), holds: make(map[storage.Identity]bool), parked: make(map[string]ParkedReplica), stopGate: make(chan struct{}, 1), wake: make(chan struct{}, 1)}
	h.operations, h.operationsDone = make(map[string]*groupOperation), make(chan struct{})
	close(h.operationsDone)
	h.membership, h.retentionGate = membership.NewCoordinator(), make(chan struct{}, 1)
	h.readmissionGate = make(chan struct{}, 1)
	// Validate engine settings without opening storage or invoking application code.
	check := h.engineOptions(&replica{identity: storage.Identity{NodeName: "validation", ClusterID: "validation", GroupID: "validation", ReplicaID: 1}}, load.Application{}, raftengine.Joining, nil)
	if err := check.Validate(); err != nil {
		return nil, err
	}
	return h, nil
}

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
	h.workWG.Add(2)
	go func() { defer h.workWG.Done(); h.maintain(work) }()
	go func() { defer h.workWG.Done(); h.collectLoop(work) }()
	go func() { h.workWG.Wait(); close(h.workDone) }()
	return nil
}

// Quiesce prevents new starts and joins recovery/report maintenance. Existing
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
	for _, wait := range []<-chan struct{}{done, operations} {
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

func (h *Host) Stop(ctx context.Context) error {
	h.mu.Lock()
	h.shutdownRequested = true
	if h.readmissionCancel != nil {
		h.readmissionCancel()
	}
	h.mu.Unlock()
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

func (h *Host) Lookup(id string) (*loadhandle.Handle, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.slots[id]
	if s == nil || s.state != "ready" || s.handle == nil {
		return nil, false
	}
	return s.handle, true
}
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
func (h *Host) Parked() map[string]ParkedReplica {
	h.mu.Lock()
	defer h.mu.Unlock()
	return maps.Clone(h.parked)
}
func (h *Host) Snapshot() []ReplicaStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	statuses := make(map[storage.Identity]ReplicaStatus)
	for _, slot := range h.slots {
		s := ReplicaStatus{Identity: slot.identity, State: slot.state}
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
		statuses[s.Identity] = s
	}
	for id := range h.holds {
		if _, found := statuses[id]; !found {
			statuses[id] = ReplicaStatus{Identity: id, State: "held"}
		}
	}
	for _, p := range h.parked {
		if _, found := statuses[p.Identity]; !found {
			statuses[p.Identity] = ReplicaStatus{Identity: p.Identity, State: "parked", ErrorCode: fault.ReplicaNotReady, Error: string(p.Reason)}
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
// or slow factories in an earlier group. Holds belong to this Host lifetime.
func (h *Host) RecoverAssignments(ctx context.Context) error {
	if h.cfg.Assignments == nil {
		return nil
	}
	refs, err := h.cfg.Assignments(ctx)
	if err != nil {
		return err
	}
	var failures []error
	for _, ref := range refs {
		h.mu.Lock()
		slot := h.slots[ref.GroupID]
		if slot != nil && slot.identity.ReplicaID == ref.ReplicaID && slot.state == "ready" && slot.core != nil && !slot.core.Running() {
			h.beginStopLocked(slot)
		}
		h.mu.Unlock()
		if _, err := h.reserveStart(ctx, ref, StartRecovery); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
