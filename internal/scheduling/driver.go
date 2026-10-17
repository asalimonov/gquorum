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

// Scheduler invokes the selected scheduler only after leadership and per-group safety
// gates. It owns validation and concrete repair proposals; callbacks cannot
// bypass them.
type Scheduler struct {
	host             Host
	repairAttempts   map[repairAttempt]time.Time
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
	aliveMembers     map[string]uint64
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
		host:           cfg.Host,
		repairAttempts: make(map[repairAttempt]time.Time),
		settings:       cfg.Settings.WithDefaults(),
		logger:         cfg.Logger,
		onTransition:   cfg.OnTransition,
		now:            time.Now,
		gate:           make(chan struct{}, 1),
		wake:           make(chan struct{}, 1),
		done:           make(chan struct{}),
		aliveSince:     make(map[string]time.Time),
		aliveMembers:   make(map[string]uint64),
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
	r.status = r.newStatus(r.host.IsLeader(), topology.SchedulingWaiting, ReconcileReasonStarting)
	if r.settings.Disabled {
		r.status = r.newStatus(r.host.IsLeader(), topology.SchedulingDisabled, ReconcileReasonConfigDisabled)
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

func (r *Scheduler) sampleAlive(doc topology.Document, health map[string]topology.Health, now time.Time) {
	declared := make(map[string]bool, len(doc.Nodes))
	for _, node := range doc.Nodes {
		declared[node.Name] = true
		member := doc.MemberID(node.Name)
		if previous := r.aliveMembers[node.Name]; previous != 0 && previous != member {
			delete(r.aliveSince, node.Name)
		}
		r.aliveMembers[node.Name] = member
		if health[node.Name] != topology.HealthAlive {
			delete(r.aliveSince, node.Name)
		} else if _, exists := r.aliveSince[node.Name]; !exists {
			r.aliveSince[node.Name] = now
		}
	}
	for name := range r.aliveMembers {
		if !declared[name] {
			delete(r.aliveMembers, name)
			delete(r.aliveSince, name)
		}
	}
}

func (r *Scheduler) resetAlive() { clear(r.aliveSince); clear(r.aliveMembers) }
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

func (r *Scheduler) runningOperationStatus(leader bool, id topology.OperationID) topology.SchedulingStatus {
	s := r.newStatus(leader, topology.SchedulingRunning, ReconcileReasonApprovedOperation)
	s.CurrentOperationID = &id
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
