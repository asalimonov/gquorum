package observe

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

type seriesKey struct {
	cluster, group, node string
	replica              uint64
}

func keyOf(id metrics.ReplicaIdentity) seriesKey {
	return seriesKey{id.ClusterID, id.GroupID, id.NodeName, id.ReplicaID}
}

type observedSeries struct {
	epoch    string
	id       string
	identity metrics.ReplicaIdentity
	ring     *internalmetrics.Ring[metrics.ReplicaObservation]
	latest   metrics.ReplicaObservation
	at       time.Time
	sequence uint64
}
type seriesPair struct{ current, retired *observedSeries }

// MetricsCollector owns independent rounds, temporal evidence and a bounded
// registry of histories. Scheduler callbacks never run on its collection loop.
type MetricsCollector struct {
	cfg                 MetricsConfig
	pool                *ReadPool
	now                 func() time.Time
	runID               string
	gate                chan struct{}
	mu                  sync.Mutex
	ctx                 context.Context
	cancel              context.CancelFunc
	started, stopped    bool
	done                chan struct{}
	stateMu             sync.Mutex
	tracker             *evidenceTracker
	snapshot            metrics.ClusterSnapshot
	series              map[seriesKey]*seriesPair
	nextSeries          uint64
	epoch               LeaderEpoch
	leader, enabled     bool
	epochSequence       uint64
	generationExhausted bool
}

func NewMetricsCollector(cfg MetricsConfig) (*MetricsCollector, error) {
	if cfg.Source == nil {
		return nil, fault.New(fault.Invalid, "metrics source is required", nil)
	}
	settings, err := cfg.Settings.Resolve()
	if err != nil {
		return nil, err
	}
	cfg.Settings = settings
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	pool, err := NewReadPool(settings.MaxParallelReads)
	if err != nil {
		return nil, err
	}
	// Generation randomness is local construction data, with no I/O or worker.
	var nonce [16]byte
	rand.Read(nonce[:])
	ctx, cancel := context.WithCancel(context.Background())
	c := &MetricsCollector{cfg: cfg, pool: pool, now: cfg.Now, runID: hex.EncodeToString(nonce[:]), gate: make(chan struct{}, 1), ctx: ctx, cancel: cancel, done: make(chan struct{}), tracker: newEvidenceTracker(settings), series: map[seriesKey]*seriesPair{}}
	c.snapshot = metrics.ClusterSnapshot{CollectorRunID: c.runID, Local: []metrics.ReplicaObservation{}, Groups: []metrics.GroupObservation{}}
	return c, nil
}
func (c *MetricsCollector) Start(parent context.Context) error {
	if err := parent.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.stopped {
		return fault.ErrStopped
	}
	c.started = true
	go func() {
		defer close(c.done)
		cancelParent := context.AfterFunc(parent, c.cancel)
		defer cancelParent()
		timer := time.NewTicker(c.cfg.Settings.CollectInterval)
		defer timer.Stop()
		_ = c.Refresh(c.ctx)
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-timer.C:
				_ = c.Refresh(c.ctx)
			}
		}
	}()
	return nil
}
func (c *MetricsCollector) Stop(ctx context.Context) error {
	c.mu.Lock()
	if !c.stopped {
		c.stopped = true
		c.cancel()
		if !c.started {
			close(c.done)
		}
	}
	c.mu.Unlock()
	select {
	case <-c.done:
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "metrics collector has not stopped", ctx.Err())
	}
	// Explicit Refresh calls share the gate and remain owned after Stop starts.
	select {
	case c.gate <- struct{}{}:
		defer func() { <-c.gate }()
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "metrics round has not stopped", ctx.Err())
	}
	if err := c.pool.Stop(ctx); err != nil {
		return err
	}
	c.stateMu.Lock()
	c.leader = false
	c.tracker.reset("")
	c.snapshot.IsCollectorLeader = false
	c.snapshot.Groups = []metrics.GroupObservation{}
	c.stateMu.Unlock()
	return nil
}
func (c *MetricsCollector) Refresh(parent context.Context) error { return c.refresh(parent, true) }

// Revalidate collects fresh source identities for a pending voter approval
// without waking the same driver recursively on a refused candidate.
func (c *MetricsCollector) Revalidate(parent context.Context) error { return c.refresh(parent, false) }
func (c *MetricsCollector) refresh(parent context.Context, wake bool) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stopCancel := context.AfterFunc(c.ctx, cancel)
	defer stopCancel()
	select {
	case c.gate <- struct{}{}:
		defer func() { <-c.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if c.ctx.Err() != nil {
		return fault.ErrStopped
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	doc, err := c.cfg.Source.Active(ctx)
	if err != nil {
		c.invalidate()
		return err
	}
	epoch, leader := c.cfg.Source.Epoch()
	enabled := c.cfg.Enabled == nil || c.cfg.Enabled()
	var round Round
	if leader {
		bounded, finish := context.WithTimeout(ctx, c.cfg.Settings.CollectTimeout)
		round, err = c.cfg.Source.Collect(bounded, doc, c.pool)
		finish()
		// Per-round deadline can publish dispatched partial outcomes. Ownership or
		// leadership loss invalidates the whole round, including apparent failures.
		if ctx.Err() != nil {
			c.invalidate()
			return ctx.Err()
		}
		after, isLeader := c.cfg.Source.Epoch()
		if !isLeader || after != epoch {
			c.invalidate()
			return fault.ErrNotLeader
		}
		current, readErr := c.cfg.Source.Active(ctx)
		if readErr != nil {
			c.invalidate()
			return readErr
		}
		if !reflect.DeepEqual(current, doc) || round.ActiveVersion != doc.Version || round.Epoch != epoch {
			c.invalidate()
			return fault.ErrVersionConflict
		}
	}
	local := c.local()
	var switches []topology.VoterSwitchState
	if c.cfg.Switches != nil {
		switches = c.cfg.Switches()
	}
	now := c.now()
	c.stateMu.Lock()
	if c.epoch != epoch || c.leader != leader || c.enabled != enabled {
		c.advanceEpoch()
		c.tracker.reset(fmt.Sprintf("%s/%d", c.runID, c.epochSequence))
	}
	c.epoch, c.leader, c.enabled = epoch, leader, enabled
	c.snapshot = metrics.ClusterSnapshot{CollectorRunID: c.epochID(), At: now.UTC(), ActiveVersion: doc.Version, IsCollectorLeader: leader && !c.generationExhausted, Local: local, Groups: []metrics.GroupObservation{}}
	if c.cfg.Identity != nil {
		id := c.cfg.Identity()
		c.snapshot.ObserverNodeName = id.NodeName
		c.snapshot.ObserverInstanceID = id.InstanceID
	}
	// Collection remains active while disabled, but disabled time cannot earn a
	// future eligibility window. A fresh enabling epoch rearms durable cooldowns.
	if leader && !c.generationExhausted {
		if !enabled {
			c.tracker.reset(fmt.Sprintf("%s/disabled/%d", c.runID, c.epochSequence))
		}
		c.tracker.apply(now, doc, round, local, switches)
		c.snapshot.Groups = c.tracker.snapshot(now)
	}
	retained := map[seriesKey]bool{}
	for _, group := range doc.Groups {
		for _, r := range group.Replicas {
			retained[seriesKey{doc.ClusterID, group.ID, r.NodeName, r.ReplicaID}] = true
		}
	}
	for i := range c.snapshot.Groups {
		for j := range c.snapshot.Groups[i].Replicas {
			o := &c.snapshot.Groups[i].Replicas[j]
			c.remember(now, o)
		}
	}
	for i := range c.snapshot.Local {
		o := &c.snapshot.Local[i]
		retained[keyOf(o.Sample.Identity)] = true
		c.remember(now, o)
	}
	for key := range c.series {
		if !retained[key] {
			delete(c.series, key)
		}
	}
	if c.generationExhausted {
		c.tracker.reset("")
		c.snapshot.IsCollectorLeader = false
		c.snapshot.Groups = []metrics.GroupObservation{}
	}
	actionable := leader && enabled && !c.generationExhausted
	c.stateMu.Unlock()
	if wake && c.cfg.WakeScheduler != nil && actionable {
		c.cfg.WakeScheduler()
	}
	return err
}
func (c *MetricsCollector) local() []metrics.ReplicaObservation {
	if c.cfg.Local == nil {
		return []metrics.ReplicaObservation{}
	}
	local := c.cfg.Local()
	out := make([]metrics.ReplicaObservation, len(local))
	for i := range local {
		out[i] = local[i].Clone()
	}
	return out
}
func (c *MetricsCollector) invalidate() {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	c.leader = false
	c.tracker.reset("")
	c.snapshot.IsCollectorLeader = false
	c.snapshot.Groups = []metrics.GroupObservation{}
}
func (c *MetricsCollector) remember(now time.Time, o *metrics.ReplicaObservation) {
	identity := o.Sample.Identity
	if identity.RunID == "" || identity.InstanceID == "" || identity.ClusterID == "" || identity.GroupID == "" || identity.NodeName == "" || identity.ReplicaID == 0 {
		return
	}
	key := keyOf(identity)
	pair := c.series[key]
	if pair == nil {
		pair = &seriesPair{}
		c.series[key] = pair
	}
	if pair.current == nil || pair.current.identity != identity || pair.current.epoch != c.epochID() {
		// A late old-run reply must never displace the new current series.
		if pair.current != nil && pair.current.identity != identity && pair.retired != nil && pair.retired.identity == identity {
			o.State = metrics.Unknown
			return
		}
		if c.nextSeries == math.MaxUint64 {
			c.generationExhausted = true
			o.State = metrics.Unknown
			return
		}
		ring, _ := internalmetrics.NewRing(c.cfg.Settings.MaxHistoryPoints, c.cfg.Settings.HistoryRetention, func(v metrics.ReplicaObservation) metrics.ReplicaObservation { return v.Clone() })
		c.nextSeries++
		pair.retired = pair.current
		pair.current = &observedSeries{id: fmt.Sprintf("%s/%d", c.runID, c.nextSeries), identity: identity, ring: ring, epoch: c.epochID()}
	}
	s := pair.current
	o.SeriesID = s.id
	sample, old := o.Sample, s.latest.Sample
	if sample.Meter.Sequence < old.Meter.Sequence || sample.Raft.Sequence < old.Raft.Sequence || sample.Writer.Sequence < old.Writer.Sequence {
		o.State = metrics.Unknown
		o.Sequence = s.sequence
		return
	}
	if s.sequence > 0 && sample.Meter.Sequence == old.Meter.Sequence && sample.Raft.Sequence == old.Raft.Sequence && sample.Writer.Sequence == old.Writer.Sequence && o.Reachability == s.latest.Reachability && (o.Reachability != "unreachable" || o.Evidence.UnavailableFor == s.latest.Evidence.UnavailableFor) && (o.ReplicationLag.State != metrics.Current || o.ReplicationLag == s.latest.ReplicationLag) {
		o.Sequence = s.sequence
		return
	}
	if s.sequence == math.MaxUint64 {
		c.generationExhausted = true
		o.State, o.Sequence = metrics.Unknown, s.sequence
		return
	}
	s.sequence++
	o.Sequence = s.sequence
	s.latest = o.Clone()
	s.at = now
	s.ring.Roll(now, *o)
}
func (c *MetricsCollector) Snapshot() metrics.ClusterSnapshot {
	// Exact local reads stay available on follower nodes and while an independent
	// remote round is waiting. These callbacks only copy published meter state.
	local := c.local()
	epoch, leader := c.cfg.Source.Epoch()
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	out := c.snapshot.Clone()
	now := c.now()
	if !c.leader || !leader || epoch != c.epoch || c.ctx.Err() != nil || c.generationExhausted {
		out.IsCollectorLeader = false
		out.Groups = []metrics.GroupObservation{}
	} else {
		out.Groups = c.tracker.snapshot(now)
		for i := range out.Groups {
			for j := range out.Groups[i].Replicas {
				c.assignCursor(&out.Groups[i].Replicas[j])
			}
		}
	}
	out.Local = local
	for i := range out.Local {
		c.assignCursor(&out.Local[i])
	}
	return out
}
func (c *MetricsCollector) assignCursor(o *metrics.ReplicaObservation) {
	o.SeriesID = ""
	o.Sequence = 0
	if pair := c.series[keyOf(o.Sample.Identity)]; pair != nil && pair.current != nil && pair.current.identity == o.Sample.Identity {
		o.SeriesID = pair.current.id
		o.Sequence = pair.current.sequence
	}
}
func (c *MetricsCollector) History(q metrics.HistoryQuery) (metrics.HistoryPage, error) {
	if q.Limit < 0 || q.Limit > c.cfg.Settings.MaxHistoryPoints {
		return metrics.HistoryPage{}, fault.ErrInvalid
	}
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	for _, pair := range c.series {
		for _, series := range []*observedSeries{pair.current, pair.retired} {
			if series == nil || series.id != q.SeriesID {
				continue
			}
			points, next, truncated := series.ring.Snapshot(c.now(), q.After, q.Limit)
			if series == pair.retired || c.ctx.Err() != nil {
				for i := range points {
					points[i].State = metrics.Stale
				}
			}
			return metrics.HistoryPage{SeriesID: q.SeriesID, Points: points, Next: next, Truncated: truncated}, nil
		}
	}
	return metrics.HistoryPage{}, fault.ErrHistoryUnavailable
}

// ResetDecisions fences evidence across a local scheduler enablement change.
// Sampling and retained diagnostic history continue independently.
func (c *MetricsCollector) ResetDecisions() {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	c.advanceEpoch()
	c.tracker.reset(fmt.Sprintf("%s/%d", c.runID, c.epochSequence))
	c.snapshot.Groups = []metrics.GroupObservation{}
}

// stateMu owns these counters. A saturated namespace cannot fence another
// epoch safely; retain diagnostic history but require a new collector lifetime.
func (c *MetricsCollector) advanceEpoch() {
	if c.epochSequence == math.MaxUint64 {
		c.generationExhausted = true
		return
	}
	c.epochSequence++
}

func (c *MetricsCollector) epochID() string { return fmt.Sprintf("%s/%d", c.runID, c.epochSequence) }
