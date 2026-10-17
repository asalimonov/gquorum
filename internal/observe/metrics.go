package observe

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

const (
	runSequenceFormat = "%s/%d"
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

// NewMetricsCollector validates dependencies and creates bounded observation
// history and read ownership.
func NewMetricsCollector(cfg MetricsConfig) (*MetricsCollector, error) {
	if cfg.Source == nil || cfg.Identity == nil {
		return nil, fault.New(fault.Invalid, "metrics source and identity are required", nil)
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
	_, _ = rand.Read(nonce[:])
	ctx, cancel := context.WithCancel(context.Background())
	c := &MetricsCollector{
		cfg:     cfg,
		pool:    pool,
		now:     cfg.Now,
		runID:   hex.EncodeToString(nonce[:]),
		gate:    make(chan struct{}, 1),
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
		tracker: newEvidenceTracker(settings),
		series:  map[seriesKey]*seriesPair{},
	}
	c.snapshot = metrics.ClusterSnapshot{
		CollectorRunID: c.runID,
		Local:          []metrics.ReplicaObservation{},
		Groups:         []metrics.GroupObservation{},
	}
	return c, nil
}

// Start begins periodic metrics collection under the supplied lifetime context.
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

// Stop cancels collection and joins the worker and outstanding read callbacks.
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

// Refresh collects current metrics and wakes scheduling when the round is
// published.
func (c *MetricsCollector) Refresh(parent context.Context) error {
	return c.collectMetrics(parent, wakeScheduler)
}

// Revalidate collects fresh source identities for a pending voter approval
// without waking the same driver recursively on a refused candidate.
func (c *MetricsCollector) Revalidate(parent context.Context) error {
	return c.collectMetrics(parent, noSchedulerWake)
}

type metricsWakeMode uint8

const (
	noSchedulerWake metricsWakeMode = iota
	wakeScheduler
)

func (c *MetricsCollector) collectMetrics(parent context.Context, wakeMode metricsWakeMode) error {
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
	doc, err := c.cfg.Source.Topology(ctx)
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
		current, validationErr := c.validateCollectedRound(ctx, epoch, round)
		if validationErr != nil {
			return validationErr
		}
		round = filterMetricsRound(doc, current, round)
		doc = current
	}
	local := c.local()
	var switches []topology.VoterSwitchState
	if c.cfg.Switches != nil {
		switches = c.cfg.Switches()
	}
	now := c.now()
	actionable := c.publishCollected(collectedMetrics{doc: doc, epoch: epoch, leader: leader, enabled: enabled,
		round: round, local: local, switches: switches, at: now})
	if wakeMode == wakeScheduler && c.cfg.WakeScheduler != nil && actionable {
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
	if identity.RunID == "" || identity.InstanceID == "" || identity.ClusterID == "" || identity.GroupID == "" ||
		identity.NodeName == "" || identity.ReplicaID == 0 {
		return
	}
	key := keyOf(identity)
	pair := c.series[key]
	if pair == nil {
		pair = &seriesPair{}
		c.series[key] = pair
	}
	s := c.prepareSeries(pair, o)
	if s == nil {
		return
	}
	o.SeriesID = s.id
	sample, old := o.Sample, s.latest.Sample
	if sample.Meter.Sequence < old.Meter.Sequence || sample.Raft.Sequence < old.Raft.Sequence ||
		sample.Writer.Sequence < old.Writer.Sequence {
		o.State = metrics.Unknown
		o.Sequence = s.sequence
		return
	}
	if sameSeriesObservation(s, o) {
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

// Snapshot returns owned local and collected observations with their current
// ages.
func (c *MetricsCollector) Snapshot() metrics.ClusterSnapshot {
	// Exact local reads stay available on follower nodes and while an
	// independent
	// remote round is waiting. These callbacks only copy published meter state.
	local := c.local()
	epoch, leader := c.cfg.Source.Epoch()
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	out := c.snapshot.Clone()
	now := c.now()
	if !c.leader || !leader || epoch != c.epoch || c.generationExhausted || c.ctx.Err() != nil {
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
	if pair := c.series[keyOf(o.Sample.Identity)]; pair != nil && pair.current != nil &&
		pair.current.identity == o.Sample.Identity {
		o.SeriesID = pair.current.id
		o.Sequence = pair.current.sequence
	}
}

// History returns a bounded page for a retained exact replica observation
// series.
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
			return c.historyPage(q, series, pair), nil
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
	c.tracker.reset(fmt.Sprintf(runSequenceFormat, c.runID, c.epochSequence))
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

func (c *MetricsCollector) epochID() string {
	return fmt.Sprintf(runSequenceFormat, c.runID, c.epochSequence)
}

func (c *MetricsCollector) validateCollectedRound(ctx context.Context,
	epoch LeaderEpoch, round Round) (topology.ProgressPage, error) {
	// Per-round deadlines can yield partial outcomes; owner cancellation or
	// collector leadership loss invalidates even apparent failure evidence.
	if err := ctx.Err(); err != nil {
		c.invalidate()
		return topology.ProgressPage{}, err
	}
	after, leader := c.cfg.Source.Epoch()
	if !leader || after != epoch || round.Epoch != epoch {
		c.invalidate()
		return topology.ProgressPage{}, fault.ErrNotLeader
	}
	current, err := c.cfg.Source.Topology(ctx)
	if err != nil {
		c.invalidate()
		return topology.ProgressPage{}, err
	}
	after, leader = c.cfg.Source.Epoch()
	if !leader || after != epoch {
		c.invalidate()
		return topology.ProgressPage{}, fault.ErrNotLeader
	}
	return current, nil
}

type collectedMetrics struct {
	doc             topology.ProgressPage
	epoch           LeaderEpoch
	leader, enabled bool
	round           Round
	local           []metrics.ReplicaObservation
	switches        []topology.VoterSwitchState
	at              time.Time
}

func (c *MetricsCollector) publishCollected(input collectedMetrics) bool {
	doc, epoch, leader, enabled, round := input.doc, input.epoch, input.leader, input.enabled, input.round
	local, switches, now := input.local, input.switches, input.at
	c.stateMu.Lock()
	if c.epoch != epoch || c.leader != leader || c.enabled != enabled {
		c.advanceEpoch()
		c.tracker.reset(fmt.Sprintf(runSequenceFormat, c.runID, c.epochSequence))
	}
	c.epoch, c.leader, c.enabled = epoch, leader, enabled
	c.snapshot = metrics.ClusterSnapshot{
		CollectorRunID:    c.epochID(),
		At:                now.UTC(),
		TargetVersion:     doc.TargetVersion,
		IsCollectorLeader: leader && !c.generationExhausted,
		Local:             local,
		Groups:            []metrics.GroupObservation{},
	}
	id := c.cfg.Identity()
	c.snapshot.ObserverNodeName = id.NodeName
	c.snapshot.ObserverInstanceID = id.InstanceID
	// Collection remains active while disabled, but disabled time cannot earn a
	// future eligibility window. A fresh enabling epoch rearms durable
	// cooldowns.
	if leader && !c.generationExhausted {
		if !enabled {
			c.tracker.reset(fmt.Sprintf("%s/disabled/%d", c.runID, c.epochSequence))
		}
		c.tracker.apply(now, doc, round, evidenceInput{local: local, switches: switches, clusterID: id.ClusterID})
		c.snapshot.Groups = c.tracker.snapshot(now)
	}
	c.rememberCollected(now, doc)
	if c.generationExhausted {
		c.tracker.reset("")
		c.snapshot.IsCollectorLeader = false
		c.snapshot.Groups = []metrics.GroupObservation{}
	}
	actionable := leader && enabled && !c.generationExhausted
	c.stateMu.Unlock()
	return actionable
}

func (c *MetricsCollector) rememberCollected(now time.Time, doc topology.ProgressPage) {
	retained := map[seriesKey]bool{}
	cluster := c.cfg.Identity().ClusterID
	for _, row := range doc.Groups {
		for _, group := range []*topology.Group{row.Converged, row.Approved} {
			if group == nil {
				continue
			}
			for _, r := range group.Replicas {
				retained[seriesKey{cluster, group.ID, r.NodeName, r.ReplicaID}] = true
			}
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
}

func (c *MetricsCollector) prepareSeries(pair *seriesPair, o *metrics.ReplicaObservation) *observedSeries {
	identity := o.Sample.Identity
	if pair.current == nil || pair.current.identity != identity || pair.current.epoch != c.epochID() {
		// A late old-run reply must never displace the new current series.
		if pair.current != nil && pair.current.identity != identity && pair.retired != nil &&
			pair.retired.identity == identity {
			o.State = metrics.Unknown
			return nil
		}
		if c.nextSeries == math.MaxUint64 {
			c.generationExhausted = true
			o.State = metrics.Unknown
			return nil
		}
		ring, _ := internalmetrics.NewRing(c.cfg.Settings.MaxHistoryPoints, c.cfg.Settings.HistoryRetention,
			func(v metrics.ReplicaObservation) metrics.ReplicaObservation { return v.Clone() })
		c.nextSeries++
		pair.retired = pair.current
		pair.current = &observedSeries{
			id:       fmt.Sprintf(runSequenceFormat, c.runID, c.nextSeries),
			identity: identity,
			ring:     ring,
			epoch:    c.epochID(),
		}
	}
	return pair.current
}

func sameSeriesObservation(s *observedSeries, o *metrics.ReplicaObservation) bool {
	sample, old := o.Sample, s.latest.Sample
	return s.sequence > 0 && sample.Meter.Sequence == old.Meter.Sequence && sample.Raft.Sequence == old.Raft.Sequence &&
		sample.Writer.Sequence == old.Writer.Sequence && o.Reachability == s.latest.Reachability &&
		(o.Reachability != "unreachable" || o.Evidence.UnavailableFor == s.latest.Evidence.UnavailableFor) &&
		(o.ReplicationLag.State != metrics.Current || o.ReplicationLag == s.latest.ReplicationLag)
}

func (c *MetricsCollector) historyPage(q metrics.HistoryQuery,
	series *observedSeries, pair *seriesPair) metrics.HistoryPage {
	batch := series.ring.Snapshot(c.now(), q.After, q.Limit)
	points := batch.Points
	if series == pair.retired || c.ctx.Err() != nil {
		for i := range points {
			points[i].State = metrics.Stale
		}
	}
	return metrics.HistoryPage{SeriesID: q.SeriesID, Points: points, Next: batch.Next,
		Truncated: batch.Truncated}
}
