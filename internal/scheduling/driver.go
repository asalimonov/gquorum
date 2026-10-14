package scheduling

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	schedulingapi "github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

const (
	defaultCallbackTimeout = 10 * time.Second
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

// WithDefaults fills absent repair intervals, callback budgets and stability
// windows.
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

// Validate requires positive repair durations and a tick timeout below the
// interval.
func (s Settings) Validate() error {
	resolved := s.WithDefaults()
	if resolved.Interval <= 0 || resolved.TickTimeout <= 0 || resolved.TickTimeout >= resolved.Interval ||
		resolved.StabilityWindow <= 0 || resolved.Cooldown <= 0 {
		return fault.New(fault.Invalid, "repair durations must be positive and tick timeout must be below interval",
			nil)
	}
	return nil
}

// Config supplies the repair host, policy, timing, exact holds and transition
// callback.
type Config struct {
	Host            Host
	Settings        Settings
	Logger          *slog.Logger
	Policy          schedulingapi.RepairPolicy
	CallbackTimeout time.Duration
	Held            func() []storage.Identity
	// OnTransition receives owned diagnostic snapshots, once per state change.
	// It must return promptly; event bus publication is suitable here.
	OnTransition func(topology.SchedulingStatus)
}

// Scheduler invokes the selected scheduler only after leader/staging/health
// gates. It owns validation and concrete repair proposals; callbacks cannot
// bypass them.
type Scheduler struct {
	host             Host
	settings         Settings
	policy           schedulingapi.RepairPolicy
	callbackTimeout  time.Duration
	held             func() []storage.Identity
	logger           *slog.Logger
	onTransition     func(topology.SchedulingStatus)
	now              func() time.Time
	gate             chan struct{}
	wake             chan struct{}
	aliveSince       map[string]time.Time
	lastRun          time.Time
	mu               sync.Mutex
	started, stopped bool
	cancel           context.CancelFunc
	done             chan struct{}
	statusMu         sync.RWMutex
	status           topology.SchedulingStatus
	callback         context.Context
	callbackStarted  time.Time
	enabled          atomic.Bool
}

// New validates repair dependencies and constructs an inactive serialized
// scheduler.
func New(cfg Config) (*Scheduler, error) {
	if cfg.Host == nil {
		return nil, fault.New(fault.Invalid, "repair host is required", nil)
	}
	if err := cfg.Settings.Validate(); err != nil {
		return nil, err
	}
	if cfg.CallbackTimeout < 0 {
		return nil, fault.ErrInvalid
	}
	if cfg.CallbackTimeout == 0 {
		cfg.CallbackTimeout = defaultCallbackTimeout
	}
	r := &Scheduler{
		host:         cfg.Host,
		settings:     cfg.Settings.WithDefaults(),
		logger:       cfg.Logger,
		onTransition: cfg.OnTransition,
		now:          time.Now,
		gate:         make(chan struct{}, 1),
		wake:         make(chan struct{}, 1),
		done:         make(chan struct{}),
		aliveSince:   make(map[string]time.Time),
	}
	r.policy, r.callbackTimeout, r.held = cfg.Policy, cfg.CallbackTimeout, cfg.Held
	if r.policy == nil {
		var err error
		r.policy, err = schedulingapi.NewDefaultRepairPolicy(schedulingapi.DefaultRepairOptions{
			StableFor: r.settings.StabilityWindow})
		if err != nil {
			return nil, err
		}
	}
	r.enabled.Store(!r.settings.Disabled)
	r.status = r.newStatus(cfg.Host.IsLeader(), topology.SchedulingWaiting, ReconcileReasonStarting)
	if r.settings.Disabled {
		r.status = r.newStatus(cfg.Host.IsLeader(), topology.SchedulingDisabled, ReconcileReasonConfigDisabled)
	}
	r.status.Since = r.now().UTC()
	return r, nil
}

// Start begins periodic repair evaluation under the supplied lifetime context.
func (r *Scheduler) Start(parent context.Context) error {
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
			case <-r.wake:
			}
			_ = r.Tick(ctx)
		}
	}()
	return nil
}

// Stop cancels the current bounded tick and joins the owned worker. Calling it
// before Start permanently closes the lifecycle, without starting a goroutine.
func (r *Scheduler) Stop(ctx context.Context) error {
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

// Status returns an owned diagnostic snapshot of current scheduling activity.
func (r *Scheduler) Status() topology.SchedulingStatus {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()
	status := r.status.Clone()
	if r.callback != nil && r.callback.Err() != nil {
		status.State, status.Reason, status.Since = topology.SchedulingError, "policy_stalled", r.callbackStarted
	}
	return status
}

// SetEnabled serializes the local kill switch with the current tick. Once
// disabling returns successfully, no originating repair remains in flight.
// Previously approved convergence remains independent of this switch.
func (r *Scheduler) SetEnabled(ctx context.Context, setting Enablement) error {
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
	if r.enabled.Swap(setting.Enabled) == setting.Enabled {
		return nil
	}
	r.host.ResetObservations()
	r.resetAlive()
	s := r.newStatus(r.host.IsLeader(), topology.SchedulingDisabled, ReconcileReasonConfigDisabled)
	if setting.Enabled {
		s = r.newStatus(r.host.IsLeader(), topology.SchedulingWaiting, ReconcileReasonStarting)
	}
	r.transition(s, "")
	return nil
}

// Tick serializes observations and bounds reads and proposals. It is an
// internal testing/maintenance seam, not an alternative authorization path.
func (r *Scheduler) Tick(parent context.Context) error {
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

func (r *Scheduler) reconcileOnce(ctx context.Context) {
	leader := r.host.IsLeader()
	views, err := r.host.Views(ctx)
	latest, target, active := views.Latest, views.Target, views.Active
	if err != nil {
		r.resetAlive()
		r.transition(r.newStatus(leader, topology.SchedulingError, ReconcileReasonStatusUnavailable), err.Error())
		return
	}
	leader = r.host.IsLeader()
	if !leader {
		r.resetAlive()
	}
	operator, err := r.host.CurrentOperator(ctx)
	if err != nil {
		r.transition(r.newStatus(leader, topology.SchedulingError, ReconcileReasonStatusUnavailable), err.Error())
		return
	}
	if r.reconcileApproval(ctx, schedulingApproval{views: views, operator: operator, leader: leader}) {
		return
	}
	health, err := r.host.Health(ctx)
	if err != nil || !knownHealth(active, health) {
		r.resetAlive()
		r.transition(r.newStatus(true, topology.SchedulingWaiting, ReconcileReasonUnknownHealth), "")
		return
	}
	now := r.now()
	r.sampleAlive(active, health, now)
	if !hasVoterConfigs(active) && !r.lastRun.IsZero() && now.Sub(r.lastRun) < r.settings.Cooldown {
		r.transition(r.newStatus(true, topology.SchedulingWaiting, ReconcileReasonCooldown), "")
		return
	}
	status, err := r.host.GlobalStatus(ctx, active)
	if err != nil {
		r.transition(r.newStatus(true, topology.SchedulingError, ReconcileReasonStatusUnavailable), err.Error())
		return
	}
	evaluations := map[string]topology.GroupHealthEvaluation{}
	for _, item := range topology.EvaluateDataGroupHealth(active, health) {
		evaluations[item.GroupID] = item
	}
	in := schedulingapi.ScheduleInput{
		Now:      now,
		Latest:   latest,
		Target:   target,
		Active:   active,
		Status:   status,
		Health:   evaluations,
		Alive:    r.aliveDurations(now),
		Held:     r.heldIdentities(),
		Metrics:  r.host.Observations(),
		Switches: r.host.VoterSwitches(),
	}
	r.evaluatePolicy(ctx, in)
}

func resumable(ex topology.Operator, exists bool, active, target topology.Document) bool {
	return exists && (!ex.Status.State.Terminal() || ex.Status.State == topology.OperatorSucceeded &&
		!reflect.DeepEqual(active, target))
}

func (r *Scheduler) healable(ctx context.Context, latest, active topology.Document) (bool, error) {
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

func (r *Scheduler) healStagedInternal(ctx context.Context, latest, active topology.Document) bool {
	ok, err := r.healable(ctx, latest, active)
	if err != nil {
		s := r.newStatus(true, topology.SchedulingError, ReconcileReasonStatusUnavailable)
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
	r.transition(r.runningOperatorStatus(true, id), "")
	return true
}

type replicaRepair struct {
	Latest, Active topology.Document
	Action         schedulingapi.ScheduleAction
	At             time.Time
}

func (r *Scheduler) commitReplicaRepair(ctx context.Context, repairRequest replicaRepair) (topology.OperatorID, error) {
	latest, active, action, at := repairRequest.Latest, repairRequest.Active, repairRequest.Action, repairRequest.At
	// Caller code may have waited while topology or native membership changed.
	// Recheck the decision boundary using fresh owned observations. The
	// replicated
	// CAS still protects changes occurring after this local validation.
	views, err := r.host.Views(ctx)
	currentLatest, currentTarget, currentActive := views.Latest, views.Target, views.Active
	if err != nil {
		return "", err
	}
	if !reflect.DeepEqual(currentLatest, latest) || !reflect.DeepEqual(currentTarget, latest) ||
		!reflect.DeepEqual(currentActive, active) {
		return "", fault.ErrVersionConflict
	}
	if !r.host.IsLeader() {
		return "", fault.ErrNotLeader
	}
	health, err := r.host.Health(ctx)
	if err != nil {
		return "", err
	}
	if !knownHealth(active, health) {
		return "", fault.New(fault.Unavailable, "repair health observation is unknown", nil)
	}
	if topology.EvaluateGroupHealth(active, action.Identity.GroupID, health).ObservedHealth.State.QuorumLost() {
		return "", fault.New(fault.UnsafeMembership, "repair group has lost its observed quorum", nil)
	}
	status, err := r.host.GlobalStatus(ctx, active)
	if err != nil {
		return "", err
	}
	if err := r.validateReplicaRepair(active, action, status); err != nil {
		return "", err
	}
	identity := action.Identity
	options := metaengine.ReplicaRepairOptions{GroupID: identity.GroupID, NodeName: identity.NodeName,
		ReplicaID: identity.ReplicaID, At: at, Origin: topology.OriginReconciler, Actor: "auto-repair"}
	repaired, err := metaengine.ReplicaRepairDocument(active, latest, options)
	if err != nil {
		return "", err
	}
	repair := repaired.Doc
	plan, err := r.host.PlanFor(ctx, repair)
	if err != nil {
		return "", err
	}
	if plan.IsEmpty() {
		return "", fault.New(fault.OperatorAuthorization, "fresh identity repair cannot have an empty plan", nil)
	}
	if !r.host.IsLeader() {
		return "", fault.ErrNotLeader
	}
	id, err := r.host.CommitRepair(ctx, RepairCommit{Document: repair, ExpectedVersion: latest.Version, Plan: plan})
	if err == nil {
		r.host.WakeOperator()
	}
	return id, err
}

func (r *Scheduler) sampleAlive(doc topology.Document, health map[string]topology.Health, now time.Time) {
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
		if n.State != topology.NodeExcluded && health[n.Name] != topology.HealthAlive &&
			health[n.Name] != topology.HealthDead {
			return false
		}
	}
	return true
}
func (r *Scheduler) resetAlive() { clear(r.aliveSince) }
func (r *Scheduler) aliveDurations(now time.Time) map[string]time.Duration {
	out := make(map[string]time.Duration, len(r.aliveSince))
	for name, since := range r.aliveSince {
		out[name] = max(0, now.Sub(since))
	}
	return out
}

func (r *Scheduler) newStatus(leader bool, state topology.SchedulingState, reason string) topology.SchedulingStatus {
	name := fmt.Sprintf("%T", r.policy)
	if _, builtIn := r.policy.(*schedulingapi.DefaultRepairPolicy); builtIn {
		name = "default-repair"
	}
	return topology.SchedulingStatus{
		Enabled:      r.enabled.Load(),
		IsLeader:     leader,
		State:        state,
		Reason:       reason,
		RepairPolicy: name,
	}
}

func (r *Scheduler) heldIdentities() []storage.Identity {
	if r.held == nil {
		return nil
	}
	return slices.Clone(r.held())
}

func (r *Scheduler) runningOperatorStatus(leader bool, id topology.OperatorID) topology.SchedulingStatus {
	s := r.newStatus(leader, topology.SchedulingRunning, ReconcileReasonApprovedOperator)
	s.CurrentOperatorID = &id
	return s
}

func (r *Scheduler) blockedStagedStatus(leader bool, version uint64) topology.SchedulingStatus {
	s := r.newStatus(leader, topology.SchedulingBlocked, ReconcileReasonStagedLatest)
	s.BlockingVersion = &version
	return s
}
func (r *Scheduler) transition(next topology.SchedulingStatus, detail string) {
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
		if next.State == topology.SchedulingBlocked || next.State == topology.SchedulingError {
			level = slog.LevelWarn
		} else if previous.State == topology.SchedulingBlocked || previous.State == topology.SchedulingError {
			level = slog.LevelInfo
		}
	}
	r.logger.Log(context.Background(), level, "topology reconciliation changed", "state", next.State, "reason",
		next.Reason, "detail", detail)
}

// Enabled reports the local decision switch without waiting for a callback.
func (r *Scheduler) Enabled() bool { return r.enabled.Load() }

// Wake coalesces a request for another repair evaluation.
func (r *Scheduler) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}
func hasVoterConfigs(doc topology.Document) bool {
	for _, g := range doc.Groups {
		if g.VoterConfig != nil {
			return true
		}
	}
	return false
}
