package metrics

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum/fault"
	api "github.com/asalimonov/gquorum/metrics"
)

const (
	latencySmoothing = 0.3
	latencyWindow    = 100 * time.Millisecond
)

// Outcome classifies an origin proposal's completion for measurement.
type Outcome uint8

// Proposal outcomes distinguish application results, cancellation, timeout and
// admission failures.
const (
	Applied Outcome = iota
	BusinessRejected
	Canceled
	TimedOut
	Unavailable
	Overloaded
)

// Meter counts origin proposal outcomes; replicated application work is counted
// separately by ApplyLatency. A forwarded command is not counted as a second
// client proposal at its leader or on each replica that applies it.
type Meter struct {
	id                                              api.ReplicaIdentity
	options                                         Options
	proposals, results, timeouts, rejected          atomic.Uint64
	walRetries, sendErrors, leaderChanges           atomic.Uint64
	resultLatency, persistenceLatency, applyLatency *EWMA
	mu                                              sync.Mutex
	previous                                        api.Counters
	last                                            time.Time
	sequence                                        uint64
	latest                                          api.ReplicaObservation
	history                                         *Ring[api.ReplicaObservation]
	stopped                                         bool
}

// NewMeter validates exact replica-run identity and creates bounded latency and
// history meters.
func NewMeter(id api.ReplicaIdentity, options Options) (*Meter, error) {
	o, err := options.Resolve()
	if err != nil {
		return nil, err
	}
	if id.ClusterID == "" || id.GroupID == "" || id.NodeName == "" || id.ReplicaID == 0 || id.RunID == "" {
		return nil, fault.ErrInvalid
	}
	r, err := NewRing(o.MaxHistoryPoints, o.HistoryRetention, api.ReplicaObservation.Clone)
	if err != nil {
		return nil, err
	}
	result, _ := NewEWMA(latencySmoothing, latencyWindow)
	persist, _ := NewEWMA(latencySmoothing, latencyWindow)
	apply, _ := NewEWMA(latencySmoothing, latencyWindow)
	return &Meter{
		id:                 id,
		options:            o,
		history:            r,
		resultLatency:      result,
		persistenceLatency: persist,
		applyLatency:       apply,
	}, nil
}
func increment(c *atomic.Uint64) {
	for {
		v := c.Load()
		if v == math.MaxUint64 || c.CompareAndSwap(v, v+1) {
			return
		}
	}
}

// ObserveProposal increments the origin proposal count with saturation.
func (m *Meter) ObserveProposal() { increment(&m.proposals) }

// ObserveResult records result latency and outcome-specific result, timeout and
// rejection counts.
func (m *Meter) ObserveResult(latency time.Duration, outcome Outcome) {
	switch outcome {
	case Applied, BusinessRejected:
		increment(&m.results)
		m.resultLatency.Observe(latency)
	case TimedOut:
		increment(&m.timeouts)
	default: // Other outcomes do not update result or timeout counters.
	}
	if outcome == BusinessRejected || outcome == Overloaded || outcome == Unavailable {
		increment(&m.rejected)
	}
}

// PersistenceObservation describes one storage write and its retry outcome.
type PersistenceObservation struct {
	Latency time.Duration
	Retry   bool
}

// ObservePersistence records write latency and increments retries when
// requested.
func (m *Meter) ObservePersistence(observation PersistenceObservation) {
	m.persistenceLatency.Observe(observation.Latency)
	if observation.Retry {
		increment(&m.walRetries)
	}
}

// ObserveApply records replicated application latency.
func (m *Meter) ObserveApply(latency time.Duration) {
	m.applyLatency.Observe(latency)
}

// ObserveSendError increments the transport error count.
func (m *Meter) ObserveSendError() { increment(&m.sendErrors) }

// ObserveLeaderChange increments the observed leadership-change count.
func (m *Meter) ObserveLeaderChange() { increment(&m.leaderChanges) }

func (m *Meter) counts() api.Counters {
	return api.Counters{
		Proposals:     m.proposals.Load(),
		Results:       m.results.Load(),
		Timeouts:      m.timeouts.Load(),
		Rejected:      m.rejected.Load(),
		WALRetries:    m.walRetries.Load(),
		SendErrors:    m.sendErrors.Load(),
		LeaderChanges: m.leaderChanges.Load(),
	}
}
func rate(cur, prev uint64, elapsed time.Duration) (float64, bool) {
	if elapsed <= 0 || cur < prev || cur == math.MaxUint64 {
		return 0, false
	}
	return float64(cur-prev) / elapsed.Seconds(), true
}
func staleLatency(l api.Latency, maxAge time.Duration) api.Latency {
	if l.State == api.Current && l.LastAge > maxAge {
		l.State = api.Stale
	}
	return l
}

// Tick combines an owned engine sample with counters, rates and latency before
// recording history.
func (m *Meter) Tick(now time.Time, sample api.ReplicaSample) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || m.sequence == math.MaxUint64 || !m.last.IsZero() && !now.After(m.last) {
		return
	}
	observed := sample.Clone()
	observed.Identity = m.id
	observed.At = now.UTC()
	observed.Counters = m.counts()
	observed.Rates = api.Rates{State: api.Unknown}
	if !m.last.IsZero() {
		d := now.Sub(m.last)
		p, okP := rate(observed.Counters.Proposals, m.previous.Proposals, d)
		r, okR := rate(observed.Counters.Results, m.previous.Results, d)
		t, okT := rate(observed.Counters.Timeouts, m.previous.Timeouts, d)
		x, okX := rate(observed.Counters.Rejected, m.previous.Rejected, d)
		if okP && okR && okT && okX {
			observed.Rates = api.Rates{State: api.Current, Window: d, Proposals: p, Results: r,
				Timeouts: t, Rejected: x}
		}
	}
	m.previous = observed.Counters
	m.resultLatency.Update(now)
	m.persistenceLatency.Update(now)
	m.applyLatency.Update(now)
	observed.ResultLatency = staleLatency(m.resultLatency.Snapshot(now), m.options.MaxSampleAge)
	observed.PersistenceLatency = staleLatency(m.persistenceLatency.Snapshot(now), m.options.MaxSampleAge)
	observed.ApplyLatency = staleLatency(m.applyLatency.Snapshot(now), m.options.MaxSampleAge)
	m.sequence++
	observed.Meter = api.Stamp{Sequence: m.sequence, State: api.Current}
	state := api.Unknown
	if observed.Raft.State == api.Current && observed.Writer.State == api.Current && observed.Ready &&
		observed.Running &&
		!observed.Restoring {
		state = api.Current
	}
	m.latest = api.ReplicaObservation{
		SeriesID:       m.id.RunID,
		Sequence:       m.sequence,
		ReceivedAt:     now.UTC(),
		State:          state,
		Sample:         observed,
		Reachability:   "alive",
		ReplicationLag: api.UnsignedValue{State: api.Unknown},
		ApplyBacklog:   api.UnsignedValue{State: api.Unknown},
		Evidence:       UnknownEvidence(),
	}
	if observed.Raft.State == api.Current {
		m.latest.ApplyBacklog = api.UnsignedValue{State: api.Current, Value: observed.Commit - min(observed.Commit,
			observed.Applied)}
	}
	m.last = now
	m.history.Roll(now, m.latest)
}

// UnknownEvidence constructs unmeasured health windows with an empty reason
// list.
func UnknownEvidence() api.Evidence {
	u := api.DurationValue{State: api.Unknown}
	return api.Evidence{
		UnavailableFor:    u,
		ReplicationLagFor: u,
		NoProgressFor:     u,
		StandbyReadyFor:   u,
		Reasons:           []string{},
	}
}
func ageStamp(s api.Stamp, elapsed, bound time.Duration) api.Stamp {
	s.Age = AddDuration(s.Age, elapsed)
	if s.State == api.Current && s.Age > bound {
		s.State = api.Stale
	}
	if s.Sequence == 0 {
		s.State = api.Unknown
	}
	return s
}

// Snapshot returns an owned observation with source ages and validity advanced
// to the supplied time.
func (m *Meter) Snapshot(now time.Time) api.ReplicaObservation {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.last.IsZero() {
		return (api.ReplicaObservation{State: api.Unknown, Sample: api.ReplicaSample{Identity: m.id, Meter: api.Stamp{
			State: api.Unknown}, Raft: api.Stamp{State: api.Unknown}, Writer: api.Stamp{State: api.Unknown}},
			Reachability: "unknown", ReplicationLag: api.UnsignedValue{
				State: api.Unknown}, ApplyBacklog: api.UnsignedValue{
				State: api.Unknown}, Evidence: UnknownEvidence()}).Clone()
	}
	s := m.latest.Clone()
	elapsed := Age(now, m.last)
	s.Sample.Meter = ageStamp(s.Sample.Meter, elapsed, m.options.MaxSampleAge)
	s.Sample.Raft = ageStamp(s.Sample.Raft, elapsed, m.options.MaxSampleAge)
	s.Sample.Writer = ageStamp(s.Sample.Writer, elapsed, m.options.MaxSampleAge)
	if s.Sample.WALStall > 0 {
		s.Sample.WALStall = AddDuration(s.Sample.WALStall, elapsed)
	}
	for _, latency := range []*api.Latency{&s.Sample.ResultLatency, &s.Sample.PersistenceLatency,
		&s.Sample.ApplyLatency} {
		latency.LastAge = AddDuration(latency.LastAge, elapsed)
		*latency = staleLatency(*latency, m.options.MaxSampleAge)
	}
	for i := range s.Sample.Progress {
		s.Sample.Progress[i].AckAge = AddDuration(s.Sample.Progress[i].AckAge, elapsed)
		AgeSnapshot(s.Sample.Progress[i].Snapshot, elapsed)
	}
	if s.Sample.Meter.State != api.Current {
		s.Sample.Rates.State = api.Stale
	}
	if s.Sample.Raft.State != api.Current {
		s.ApplyBacklog.State = s.Sample.Raft.State
	}
	if m.stopped || s.Sample.Meter.State == api.Stale || s.Sample.Raft.State == api.Stale ||
		s.Sample.Writer.State == api.Stale {
		s.State = api.Stale
	}
	if m.stopped {
		s.Sample.Running = false
		s.Sample.Ready = false
		s.Reachability = "unknown"
	}
	return s
}

// History returns a bounded page for this exact run and marks stopped
// observations stale.
func (m *Meter) History(now time.Time, q api.HistoryQuery) (api.HistoryPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if q.Limit < 0 || q.Limit > m.options.MaxHistoryPoints {
		return api.HistoryPage{}, fault.ErrInvalid
	}
	if q.SeriesID != m.id.RunID || m.last.IsZero() {
		return api.HistoryPage{}, fault.ErrHistoryUnavailable
	}
	batch := m.history.Snapshot(now, q.After, q.Limit)
	points := batch.Points
	if m.stopped {
		for i := range points {
			points[i].State = api.Stale
		}
	}
	return api.HistoryPage{SeriesID: q.SeriesID, Points: points, Next: batch.Next, Truncated: batch.Truncated}, nil
}

// Stop prevents new samples and marks subsequent observations as stopped and
// stale.
func (m *Meter) Stop() { m.mu.Lock(); m.stopped = true; m.mu.Unlock() }
