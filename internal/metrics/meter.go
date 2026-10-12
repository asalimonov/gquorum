package metrics

import (
	"github.com/asalimonov/gquorum/fault"
	api "github.com/asalimonov/gquorum/metrics"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type Outcome uint8

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
	result, _ := NewEWMA(.3, 100*time.Millisecond)
	persist, _ := NewEWMA(.3, 100*time.Millisecond)
	apply, _ := NewEWMA(.3, 100*time.Millisecond)
	return &Meter{id: id, options: o, history: r, resultLatency: result, persistenceLatency: persist, applyLatency: apply}, nil
}
func increment(c *atomic.Uint64) {
	for {
		v := c.Load()
		if v == math.MaxUint64 || c.CompareAndSwap(v, v+1) {
			return
		}
	}
}
func (m *Meter) ObserveProposal() { increment(&m.proposals) }
func (m *Meter) ObserveResult(latency time.Duration, outcome Outcome) {
	switch outcome {
	case Applied, BusinessRejected:
		increment(&m.results)
		m.resultLatency.Observe(latency)
	case TimedOut:
		increment(&m.timeouts)
	}
	if outcome == BusinessRejected || outcome == Overloaded || outcome == Unavailable {
		increment(&m.rejected)
	}
}
func (m *Meter) ObservePersistence(latency time.Duration, retry bool) {
	m.persistenceLatency.Observe(latency)
	if retry {
		increment(&m.walRetries)
	}
}
func (m *Meter) ObserveApply(latency time.Duration) { m.applyLatency.Observe(latency) }
func (m *Meter) ObserveSendError()                  { increment(&m.sendErrors) }
func (m *Meter) ObserveLeaderChange()               { increment(&m.leaderChanges) }

func (m *Meter) counts() api.Counters {
	return api.Counters{Proposals: m.proposals.Load(), Results: m.results.Load(), Timeouts: m.timeouts.Load(), Rejected: m.rejected.Load(), WALRetries: m.walRetries.Load(), SendErrors: m.sendErrors.Load(), LeaderChanges: m.leaderChanges.Load()}
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
func (m *Meter) Tick(now time.Time, sample api.ReplicaSample) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || m.sequence == math.MaxUint64 || !m.last.IsZero() && !now.After(m.last) {
		return
	}
	sample = sample.Clone()
	sample.Identity = m.id
	sample.At = now.UTC()
	sample.Counters = m.counts()
	sample.Rates = api.Rates{State: api.Unknown}
	if !m.last.IsZero() {
		d := now.Sub(m.last)
		p, okP := rate(sample.Counters.Proposals, m.previous.Proposals, d)
		r, okR := rate(sample.Counters.Results, m.previous.Results, d)
		t, okT := rate(sample.Counters.Timeouts, m.previous.Timeouts, d)
		x, okX := rate(sample.Counters.Rejected, m.previous.Rejected, d)
		if okP && okR && okT && okX {
			sample.Rates = api.Rates{State: api.Current, Window: d, Proposals: p, Results: r, Timeouts: t, Rejected: x}
		}
	}
	m.previous = sample.Counters
	m.resultLatency.Update(now)
	m.persistenceLatency.Update(now)
	m.applyLatency.Update(now)
	sample.ResultLatency = staleLatency(m.resultLatency.Snapshot(now), m.options.MaxSampleAge)
	sample.PersistenceLatency = staleLatency(m.persistenceLatency.Snapshot(now), m.options.MaxSampleAge)
	sample.ApplyLatency = staleLatency(m.applyLatency.Snapshot(now), m.options.MaxSampleAge)
	m.sequence++
	sample.Meter = api.Stamp{Sequence: m.sequence, State: api.Current}
	state := api.Unknown
	if sample.Raft.State == api.Current && sample.Writer.State == api.Current && sample.Ready && sample.Running && !sample.Restoring {
		state = api.Current
	}
	m.latest = api.ReplicaObservation{SeriesID: m.id.RunID, Sequence: m.sequence, ReceivedAt: now.UTC(), State: state, Sample: sample, Reachability: "alive", ReplicationLag: api.UnsignedValue{State: api.Unknown}, ApplyBacklog: api.UnsignedValue{State: api.Unknown}, Evidence: UnknownEvidence()}
	if sample.Raft.State == api.Current {
		m.latest.ApplyBacklog = api.UnsignedValue{State: api.Current, Value: sample.Commit - min(sample.Commit, sample.Applied)}
	}
	m.last = now
	m.history.Roll(now, m.latest)
}

func UnknownEvidence() api.Evidence {
	u := api.DurationValue{State: api.Unknown}
	return api.Evidence{UnavailableFor: u, ReplicationLagFor: u, NoProgressFor: u, StandbyReadyFor: u, Reasons: []string{}}
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
func (m *Meter) Snapshot(now time.Time) api.ReplicaObservation {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.last.IsZero() {
		return (api.ReplicaObservation{State: api.Unknown, Sample: api.ReplicaSample{Identity: m.id, Meter: api.Stamp{State: api.Unknown}, Raft: api.Stamp{State: api.Unknown}, Writer: api.Stamp{State: api.Unknown}}, Reachability: "unknown", ReplicationLag: api.UnsignedValue{State: api.Unknown}, ApplyBacklog: api.UnsignedValue{State: api.Unknown}, Evidence: UnknownEvidence()}).Clone()
	}
	s := m.latest.Clone()
	elapsed := Age(now, m.last)
	s.Sample.Meter = ageStamp(s.Sample.Meter, elapsed, m.options.MaxSampleAge)
	s.Sample.Raft = ageStamp(s.Sample.Raft, elapsed, m.options.MaxSampleAge)
	s.Sample.Writer = ageStamp(s.Sample.Writer, elapsed, m.options.MaxSampleAge)
	if s.Sample.WALStall > 0 {
		s.Sample.WALStall = AddDuration(s.Sample.WALStall, elapsed)
	}
	for _, latency := range []*api.Latency{&s.Sample.ResultLatency, &s.Sample.PersistenceLatency, &s.Sample.ApplyLatency} {
		latency.LastAge = AddDuration(latency.LastAge, elapsed)
		*latency = staleLatency(*latency, m.options.MaxSampleAge)
	}
	for i := range s.Sample.Progress {
		s.Sample.Progress[i].AckAge = AddDuration(s.Sample.Progress[i].AckAge, elapsed)
	}
	if s.Sample.Meter.State != api.Current {
		s.Sample.Rates.State = api.Stale
	}
	if s.Sample.Raft.State != api.Current {
		s.ApplyBacklog.State = s.Sample.Raft.State
	}
	if m.stopped || s.Sample.Meter.State == api.Stale || s.Sample.Raft.State == api.Stale || s.Sample.Writer.State == api.Stale {
		s.State = api.Stale
	}
	if m.stopped {
		s.Sample.Running = false
		s.Sample.Ready = false
		s.Reachability = "unknown"
	}
	return s
}
func (m *Meter) History(now time.Time, q api.HistoryQuery) (api.HistoryPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if q.Limit < 0 || q.Limit > m.options.MaxHistoryPoints {
		return api.HistoryPage{}, fault.ErrInvalid
	}
	if m.last.IsZero() || q.SeriesID != m.id.RunID {
		return api.HistoryPage{}, fault.ErrHistoryUnavailable
	}
	points, next, truncated := m.history.Snapshot(now, q.After, q.Limit)
	if m.stopped {
		for i := range points {
			points[i].State = api.Stale
		}
	}
	return api.HistoryPage{SeriesID: q.SeriesID, Points: points, Next: next, Truncated: truncated}, nil
}
func (m *Meter) Stop() { m.mu.Lock(); m.stopped = true; m.mu.Unlock() }
