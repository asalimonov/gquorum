package observe

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type Source interface {
	Active(context.Context) (topology.Document, error)
	Health(context.Context, topology.Document) (map[string]topology.Health, error)
}
type Settings struct{ Interval, Timeout time.Duration }

func (s Settings) WithDefaults() Settings {
	if s.Interval == 0 {
		s.Interval = 5 * time.Second
	}
	if s.Timeout == 0 {
		s.Timeout = min(2*time.Second, s.Interval-s.Interval/3)
	}
	return s
}
func (s Settings) Validate() error {
	s = s.WithDefaults()
	if s.Interval <= 0 || s.Timeout <= 0 || s.Timeout >= s.Interval {
		return fault.New(fault.Invalid, "observation durations must be positive and timeout below interval", nil)
	}
	return nil
}

type Config struct {
	Source   Source
	Settings Settings
	Logger   *slog.Logger
	// Transition hooks run synchronously on the sampler; they must be bounded
	// and must not call Refresh or Stop on this watcher.
	OnGroupTransition func(topology.GroupHealth, topology.GroupHealthEvaluation)
	OnNodeTransition  func(string, topology.Health, topology.Health)
}

// Watch samples Active-declared groups uniformly, without a process-global
// metrics registry. Applications may publish metrics from its owned Snapshot.
type Watch struct {
	cfg              Config
	now              func() time.Time
	wake             chan struct{}
	gate             chan struct{}
	previous         map[string]topology.GroupHealth
	everKnown        map[string]bool
	nodes            map[string]topology.Health
	mu               sync.Mutex
	started, stopped bool
	cancel           context.CancelFunc
	done             chan struct{}
	snapshotMu       sync.RWMutex
	snapshot         topology.HealthObservation
}

func New(cfg Config) (*Watch, error) {
	if cfg.Source == nil {
		return nil, fault.New(fault.Invalid, "health observation source is required", nil)
	}
	if err := cfg.Settings.Validate(); err != nil {
		return nil, err
	}
	cfg.Settings = cfg.Settings.WithDefaults()
	return &Watch{cfg: cfg, now: time.Now, wake: make(chan struct{}, 1), gate: make(chan struct{}, 1), done: make(chan struct{}), previous: map[string]topology.GroupHealth{}, everKnown: map[string]bool{}, nodes: map[string]topology.Health{}}, nil
}
func (w *Watch) Start(parent context.Context) error {
	if err := parent.Err(); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started || w.stopped {
		return fault.ErrStopped
	}
	ctx, cancel := context.WithCancel(parent)
	w.cancel = cancel
	w.started = true
	go func() {
		defer close(w.done)
		ticker := time.NewTicker(w.cfg.Settings.Interval)
		defer ticker.Stop()
		_ = w.Refresh(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			case <-w.wake:
			}
			_ = w.Refresh(ctx)
		}
	}()
	return nil
}
func (w *Watch) Wake() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}
func (w *Watch) Stop(ctx context.Context) error {
	w.mu.Lock()
	if !w.stopped {
		w.stopped = true
		if w.cancel != nil {
			w.cancel()
		}
		if !w.started {
			close(w.done)
		}
	}
	w.mu.Unlock()
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "health observation worker has not stopped", ctx.Err())
	}
}
func (w *Watch) Snapshot() topology.HealthObservation {
	w.snapshotMu.RLock()
	defer w.snapshotMu.RUnlock()
	return w.snapshot.Clone()
}
func (w *Watch) Refresh(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, w.cfg.Settings.Timeout)
	defer cancel()
	select {
	case w.gate <- struct{}{}:
		defer func() { <-w.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	w.mu.Lock()
	stopped := w.stopped
	w.mu.Unlock()
	if stopped {
		return fault.ErrStopped
	}
	doc, err := w.cfg.Source.Active(ctx)
	if err != nil {
		return err
	}
	health, healthErr := w.cfg.Source.Health(ctx, doc)
	if err := parent.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		healthErr = err
	}
	if healthErr != nil {
		health = nil
	}
	// Failed node observation is itself visible as unknown, preserving durable
	// declared shape and removing stale numeric measurements.
	w.recompute(doc, health)
	return healthErr
}
func (w *Watch) recompute(doc topology.Document, health map[string]topology.Health) {
	s := topology.HealthObservation{Version: doc.Version, At: w.now().UTC(), Nodes: map[string]topology.Health{}, Groups: topology.EvaluateAllGroupHealth(doc, health)}
	for _, node := range doc.Nodes {
		observed := health[node.Name]
		if node.State == topology.NodeExcluded || observed != topology.HealthAlive && observed != topology.HealthDead {
			observed = topology.HealthUnknown
		}
		s.Nodes[node.Name] = observed
	}
	w.snapshotMu.Lock()
	w.snapshot = s.Clone()
	w.snapshotMu.Unlock()
	seen := map[string]bool{}
	for _, e := range s.Groups {
		seen[e.GroupID] = true
		old, exists := w.previous[e.GroupID]
		if !exists {
			old = topology.GroupUnknown
		}
		next := e.ObservedHealth.State
		w.previous[e.GroupID] = next
		if exists && old == next {
			continue
		}
		if w.cfg.OnGroupTransition != nil {
			w.cfg.OnGroupTransition(old, e.Clone())
		}
		w.logTransition(old, e)
	}
	for id := range w.previous {
		if !seen[id] {
			delete(w.previous, id)
			delete(w.everKnown, id)
		}
	}
	for name, observed := range s.Nodes {
		old, exists := w.nodes[name]
		if !exists {
			old = topology.HealthUnknown
		}
		if (!exists || old != observed) && w.cfg.OnNodeTransition != nil {
			w.cfg.OnNodeTransition(name, old, observed)
		}
	}
	w.nodes = s.Nodes
}
func (w *Watch) logTransition(old topology.GroupHealth, e topology.GroupHealthEvaluation) {
	next := e.ObservedHealth.State
	known := w.everKnown[e.GroupID]
	if next != topology.GroupUnknown {
		w.everKnown[e.GroupID] = true
	}
	if w.cfg.Logger == nil {
		return
	}
	level := slog.LevelDebug
	switch next {
	case topology.GroupQuorumLost:
		level = slog.LevelError
	case topology.GroupDegraded:
		level = slog.LevelWarn
	case topology.GroupHealthy:
		if known {
			level = slog.LevelInfo
		}
	case topology.GroupUnknown:
		if !known {
			return
		}
		level = slog.LevelWarn
	}
	w.cfg.Logger.Log(context.Background(), level, "group health changed", "group", e.GroupID, "old", old, "state", next, "voters", e.Voters, "quorum", e.Quorum)
}
