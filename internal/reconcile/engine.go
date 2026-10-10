package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

const (
	defaultInterval        = 30 * time.Second
	defaultTickTimeout     = 20 * time.Second
	defaultStabilityWindow = 60 * time.Second
	defaultCooldown        = 5 * time.Minute
)

// Settings controls local observation and repair timing, independently of the
// replicated cluster rules. Zero durations select conservative defaults.
type Settings struct {
	Disabled                                         bool
	Interval, TickTimeout, StabilityWindow, Cooldown time.Duration
}

func (s Settings) WithDefaults() Settings {
	if s.Interval == 0 {
		s.Interval = defaultInterval
	}
	if s.TickTimeout == 0 {
		s.TickTimeout = min(defaultTickTimeout, s.Interval-s.Interval/3)
	}
	if s.StabilityWindow == 0 {
		s.StabilityWindow = defaultStabilityWindow
	}
	if s.Cooldown == 0 {
		s.Cooldown = defaultCooldown
	}
	return s
}
func (s Settings) Validate() error {
	s = s.WithDefaults()
	if s.Interval <= 0 || s.TickTimeout <= 0 || s.TickTimeout >= s.Interval || s.StabilityWindow <= 0 || s.Cooldown <= 0 {
		return fault.New(fault.Invalid, "repair durations must be positive and tick timeout must be below interval", nil)
	}
	return nil
}

type Config struct {
	Host     Host
	Settings Settings
	Logger   *slog.Logger
	// OnTransition receives owned diagnostic snapshots, once per state change.
	// It must return promptly; event bus publication is suitable here.
	OnTransition func(topology.ReconciliationStatus)
}

// Reconciler deliberately has no caller-provided policy extension point.
// The narrow policy and replicated repair command enforce the same identity.
type Reconciler struct {
	host             Host
	settings         Settings
	policy           ReconcilePolicy
	logger           *slog.Logger
	onTransition     func(topology.ReconciliationStatus)
	now              func() time.Time
	gate             chan struct{}
	aliveSince       map[string]time.Time
	lastRun          time.Time
	mu               sync.Mutex
	started, stopped bool
	cancel           context.CancelFunc
	done             chan struct{}
	statusMu         sync.RWMutex
	status           topology.ReconciliationStatus
	enabled          atomic.Bool
}

func New(cfg Config) (*Reconciler, error) {
	if cfg.Host == nil {
		return nil, fault.New(fault.Invalid, "repair host is required", nil)
	}
	if err := cfg.Settings.Validate(); err != nil {
		return nil, err
	}
	r := &Reconciler{host: cfg.Host, settings: cfg.Settings.WithDefaults(), logger: cfg.Logger, onTransition: cfg.OnTransition, now: time.Now, gate: make(chan struct{}, 1), done: make(chan struct{}), aliveSince: make(map[string]time.Time)}
	r.policy = NewRestoreDegradedPolicy(r.settings.StabilityWindow)
	r.enabled.Store(!r.settings.Disabled)
	r.status = r.newStatus(cfg.Host.IsLeader(), topology.ReconciliationWaiting, ReconcileReasonStarting)
	if r.settings.Disabled {
		r.status = r.newStatus(cfg.Host.IsLeader(), topology.ReconciliationDisabled, ReconcileReasonConfigDisabled)
	}
	r.status.Since = r.now().UTC()
	return r, nil
}

func (r *Reconciler) Start(parent context.Context) error {
	if err := parent.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.stopped {
		return fault.ErrStopped
	}
	r.started = true
	ctx, cancel := context.WithCancel(parent)
	r.cancel = cancel
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(r.settings.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = r.Tick(ctx)
			}
		}
	}()
	return nil
}

// Stop cancels the current bounded tick and joins the owned worker. Calling it
// before Start permanently closes the lifecycle, without starting a goroutine.
func (r *Reconciler) Stop(ctx context.Context) error {
	r.mu.Lock()
	if !r.stopped {
		r.stopped = true
		if r.cancel != nil {
			r.cancel()
		}
		if !r.started {
			close(r.done)
		}
	}
	r.mu.Unlock()
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "repair worker has not stopped", ctx.Err())
	}
}

func (r *Reconciler) Status() topology.ReconciliationStatus {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()
	return r.status.Clone()
}

// SetEnabled serializes the local kill switch with the current tick. Once
// disabling returns successfully, no originating repair remains in flight.
// Previously approved convergence remains independent of this switch.
func (r *Reconciler) SetEnabled(ctx context.Context, enabled bool) error {
	select {
	case r.gate <- struct{}{}:
		defer func() { <-r.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	stopped := r.stopped
	r.mu.Unlock()
	if stopped {
		return fault.ErrStopped
	}
	if r.enabled.Swap(enabled) == enabled {
		return nil
	}
	r.resetAlive()
	s := r.newStatus(r.host.IsLeader(), topology.ReconciliationDisabled, ReconcileReasonConfigDisabled)
	if enabled {
		s = r.newStatus(r.host.IsLeader(), topology.ReconciliationWaiting, ReconcileReasonStarting)
	}
	r.transition(s, "")
	return nil
}

// Tick serializes observations and bounds reads and proposals. It is an
// internal testing/maintenance seam, not an alternative authorization path.
func (r *Reconciler) Tick(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, r.settings.TickTimeout)
	defer cancel()
	select {
	case r.gate <- struct{}{}:
		defer func() { <-r.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	stopped := r.stopped
	r.mu.Unlock()
	if stopped {
		return fault.ErrStopped
	}
	r.reconcileOnce(ctx)
	return ctx.Err()
}

func (r *Reconciler) reconcileOnce(ctx context.Context) {
	leader := r.host.IsLeader()
	if !r.enabled.Load() {
		r.resetAlive()
		r.transition(r.newStatus(leader, topology.ReconciliationDisabled, ReconcileReasonConfigDisabled), "")
		return
	}
	latest, target, active, err := r.host.Views(ctx)
	if err != nil {
		r.resetAlive()
		r.transition(r.newStatus(leader, topology.ReconciliationError, ReconcileReasonStatusUnavailable), err.Error())
		return
	}
	leader = r.host.IsLeader()
	if !leader {
		r.resetAlive()
	}
	execution, exists, err := r.host.CurrentExecution(ctx)
	if err != nil {
		r.transition(r.newStatus(leader, topology.ReconciliationError, ReconcileReasonStatusUnavailable), err.Error())
		return
	}
	if resumable(execution, exists, active, target) {
		if leader {
			r.host.WakeConvergence()
		}
		r.transition(r.runningExecutionStatus(leader, execution.Status.ID), "")
		return
	}
	if exists && execution.Status.State == topology.ExecutionFailed && execution.Status.Version == latest.Version {
		s := r.newStatus(leader, topology.ReconciliationBlocked, ReconcileReasonFailedExecution)
		s.BlockingVersion, s.CurrentExecutionID = &execution.Status.Version, &execution.Status.ID
		r.transition(s, "")
		return
	}
	if !reflect.DeepEqual(latest, target) || !reflect.DeepEqual(latest, active) {
		if leader && r.healStagedInternal(ctx, latest, active) {
			return
		}
		r.transition(r.blockedStagedStatus(leader, latest.Version), "")
		return
	}
	if !leader {
		r.transition(r.newStatus(false, topology.ReconciliationStandby, ReconcileReasonNotLeader), "")
		return
	}
	health, err := r.host.Health(ctx)
	if err != nil || !knownHealth(active, health) {
		r.resetAlive()
		r.transition(r.newStatus(true, topology.ReconciliationWaiting, ReconcileReasonUnknownHealth), "")
		return
	}
	now := r.now()
	r.sampleAlive(active, health, now)
	if !r.lastRun.IsZero() && now.Sub(r.lastRun) < r.settings.Cooldown {
		r.transition(r.newStatus(true, topology.ReconciliationWaiting, ReconcileReasonCooldown), "")
		return
	}
	status, err := r.host.GlobalStatus(ctx, active)
	if err != nil {
		r.transition(r.newStatus(true, topology.ReconciliationError, ReconcileReasonStatusUnavailable), err.Error())
		return
	}
	evaluations := map[string]topology.GroupHealthEvaluation{}
	for _, item := range topology.EvaluateDataGroupHealth(active, health) {
		evaluations[item.GroupID] = item
	}
	decision := r.policy.Evaluate(ReconcileInput{Latest: latest, Target: target, Active: active, Status: status, Health: evaluations, Alive: r.aliveDurations(now)})
	if decision.Action == nil {
		s := r.newStatus(true, topology.ReconciliationIdle, ReconcileReasonConverged)
		switch decision.Reason {
		case ReconcileReasonStabilityWindow:
			s = r.newStatus(true, topology.ReconciliationWaiting, ReconcileReasonStabilityWindow)
		case ReconcileReasonQuorumLost:
			s = r.newStatus(true, topology.ReconciliationBlocked, ReconcileReasonQuorumLost)
		}
		r.transition(s, "")
		return
	}
	action := *decision.Action
	if action.Kind != ActionRepairReplica {
		r.transition(r.newStatus(true, topology.ReconciliationError, ReconcileReasonPolicyRejected), fmt.Sprintf("forbidden action %q", action.Kind))
		return
	}
	s := r.newStatus(true, topology.ReconciliationRunning, ReconcileReasonMissingReplica)
	s.CurrentAction = &topology.ReconciliationActionStatus{Kind: action.Kind, GroupID: action.GroupID, NodeName: action.NodeName, ReplicaID: action.ReplicaID}
	r.transition(s, "")
	id, err := r.commitReplicaRepair(ctx, latest, active, status, action, now)
	r.lastRun = now
	if err != nil {
		reason := ReconcileReasonPolicyRejected
		if errors.Is(err, fault.ErrVersionConflict) {
			reason = ReconcileReasonCASConflict
		}
		r.transition(r.newStatus(true, topology.ReconciliationError, reason), err.Error())
		return
	}
	r.transition(r.runningExecutionStatus(true, id), "")
}

func resumable(ex topology.ExecutionRecord, exists bool, active, target topology.Document) bool {
	return exists && (!ex.Status.State.Terminal() || ex.Status.State == topology.ExecutionSucceeded && !reflect.DeepEqual(active, target))
}

func (r *Reconciler) healable(ctx context.Context, latest, active topology.Document) (bool, error) {
	if latest.Origin != topology.OriginInternal || latest.ParentVersion != active.Version {
		return false, nil
	}
	health, err := r.host.Health(ctx)
	if err != nil || !knownHealth(active, health) {
		return false, nil
	}
	plan, err := r.host.PlanFor(ctx, latest)
	return err == nil && plan.IsEmpty(), err
}
func (r *Reconciler) healStagedInternal(ctx context.Context, latest, active topology.Document) bool {
	ok, err := r.healable(ctx, latest, active)
	if err != nil {
		s := r.newStatus(true, topology.ReconciliationError, ReconcileReasonStatusUnavailable)
		s.BlockingVersion = &latest.Version
		r.transition(s, err.Error())
		return true
	}
	if !ok {
		return false
	}
	id, err := r.host.Approve(ctx, latest.Version)
	if err != nil {
		r.transition(r.blockedStagedStatus(true, latest.Version), err.Error())
		return true
	}
	r.transition(r.runningExecutionStatus(true, id), "")
	return true
}

func (r *Reconciler) commitReplicaRepair(ctx context.Context, latest, active topology.Document, status map[string]topology.GroupStatus, action ReconcileAction, at time.Time) (topology.ExecutionID, error) {
	group, node := active.Group(action.GroupID), active.Node(action.NodeName)
	observed, ok := status[action.GroupID]
	if action.Kind != ActionRepairReplica || group == nil || action.GroupID == topology.MetaGroupID || node == nil || node.State != topology.NodeActive || !ok || !observed.Found || observed.GroupID != action.GroupID {
		return "", fault.ErrExecutionAuthorization
	}
	for _, id := range append(append([]uint64(nil), observed.ConfState.Voters...), observed.ConfState.Learners...) {
		if id == action.ReplicaID {
			return "", fault.New(fault.ExecutionAuthorization, "repair identity is already a member", nil)
		}
	}
	repair, _, err := metaengine.ReplicaRepairDocument(active, latest, action.GroupID, action.NodeName, action.ReplicaID, at, topology.OriginReconciler, "auto-repair")
	if err != nil {
		return "", err
	}
	plan, err := r.host.PlanFor(ctx, repair)
	if err != nil {
		return "", err
	}
	if plan.IsEmpty() {
		return "", fault.New(fault.ExecutionAuthorization, "fresh identity repair cannot have an empty plan", nil)
	}
	if !r.host.IsLeader() {
		return "", fault.ErrNotLeader
	}
	id, err := r.host.CommitRepair(ctx, repair, latest.Version, plan)
	if err == nil {
		r.host.WakeConvergence()
	}
	return id, err
}

func (r *Reconciler) sampleAlive(doc topology.Document, health map[string]topology.Health, now time.Time) {
	declared := make(map[string]bool, len(doc.Nodes))
	for _, node := range doc.Nodes {
		declared[node.Name] = true
		if health[node.Name] != topology.HealthAlive {
			delete(r.aliveSince, node.Name)
		} else if _, exists := r.aliveSince[node.Name]; !exists {
			r.aliveSince[node.Name] = now
		}
	}
	for name := range r.aliveSince {
		if !declared[name] {
			delete(r.aliveSince, name)
		}
	}
}

func knownHealth(doc topology.Document, health map[string]topology.Health) bool {
	if health == nil {
		return false
	}
	for _, n := range doc.Nodes {
		if n.State != topology.NodeExcluded && health[n.Name] != topology.HealthAlive && health[n.Name] != topology.HealthDead {
			return false
		}
	}
	return true
}
func (r *Reconciler) resetAlive() { clear(r.aliveSince) }
func (r *Reconciler) aliveDurations(now time.Time) map[string]time.Duration {
	out := make(map[string]time.Duration, len(r.aliveSince))
	for name, since := range r.aliveSince {
		out[name] = max(0, now.Sub(since))
	}
	return out
}
func (r *Reconciler) newStatus(leader bool, state topology.ReconciliationState, reason string) topology.ReconciliationStatus {
	return topology.ReconciliationStatus{Enabled: r.enabled.Load(), IsLeader: leader, State: state, Reason: reason, Policy: r.policy.Name()}
}
func (r *Reconciler) runningExecutionStatus(leader bool, id topology.ExecutionID) topology.ReconciliationStatus {
	s := r.newStatus(leader, topology.ReconciliationRunning, ReconcileReasonApprovedExecution)
	s.CurrentExecutionID = &id
	return s
}
func (r *Reconciler) blockedStagedStatus(leader bool, version uint64) topology.ReconciliationStatus {
	s := r.newStatus(leader, topology.ReconciliationBlocked, ReconcileReasonStagedLatest)
	s.BlockingVersion = &version
	return s
}
func (r *Reconciler) transition(next topology.ReconciliationStatus, detail string) {
	r.statusMu.Lock()
	previous := r.status
	a, b := previous.Clone(), next.Clone()
	a.Since, b.Since = time.Time{}, time.Time{}
	if reflect.DeepEqual(a, b) {
		r.statusMu.Unlock()
		return
	}
	next.Since = r.now().UTC()
	r.status = next.Clone()
	r.statusMu.Unlock()
	if r.onTransition != nil {
		r.onTransition(next.Clone())
	}
	if r.logger == nil {
		return
	}
	level := slog.LevelDebug
	if next.IsLeader {
		if next.State == topology.ReconciliationBlocked || next.State == topology.ReconciliationError {
			level = slog.LevelWarn
		} else if previous.State == topology.ReconciliationBlocked || previous.State == topology.ReconciliationError {
			level = slog.LevelInfo
		}
	}
	r.logger.Log(context.Background(), level, "topology reconciliation changed", "state", next.State, "reason", next.Reason, "detail", detail)
}
