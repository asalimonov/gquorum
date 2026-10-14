//go:build !functional

package metrics

import (
	"errors"
	"github.com/asalimonov/gquorum/fault"
	api "github.com/asalimonov/gquorum/metrics"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestRingBoundedOwnedCursorAndExpiry(t *testing.T) {
	t.Parallel()
	now := time.Now()
	r, err := NewRing(3, 10*time.Second, func(x []int) []int { return append([]int{}, x...) })
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		v := []int{i}
		r.Roll(now.Add(time.Duration(i)*time.Second), v)
		v[0] = -1
	}
	batch := r.Snapshot(now.Add(5*time.Second), 0, 2)
	points, next, truncated := batch.Points, batch.Next, batch.Truncated
	if !reflect.DeepEqual(points, [][]int{{2}, {3}}) || next != 4 || !truncated {
		t.Fatal(points, next, truncated)
	}
	points[0][0] = 99
	batch = r.Snapshot(now.Add(5*time.Second), next, 2)
	points, next, truncated = batch.Points, batch.Next, batch.Truncated
	if !reflect.DeepEqual(points, [][]int{{4}}) || next != 5 || truncated {
		t.Fatal(points, next, truncated)
	}
	batch = r.Snapshot(now.Add(14*time.Second), 0, 3)
	points, next, truncated = batch.Points, batch.Next, batch.Truncated
	if len(points) != 0 || next != 5 || !truncated || r.size != 0 {
		t.Fatal(points, next, truncated)
	}
	for _, b := range r.slots {
		if b.value != nil {
			t.Fatal("expired nested data retained")
		}
	}
	r.Roll(now.Add(15*time.Second), []int{5})
	batch = r.Snapshot(now.Add(15*time.Second), 5, 1)
	points, next, truncated = batch.Points, batch.Next, batch.Truncated
	if !reflect.DeepEqual(points, [][]int{{5}}) || next != 6 || truncated {
		t.Fatal(points, next, truncated)
	}
	batch = r.Snapshot(now.Add(15*time.Second), 99, 1)
	points, next = batch.Points, batch.Next
	if len(points) != 0 || next != 99 {
		t.Fatal("future cursor moved backwards")
	}
}

func TestEWMAPortedDurationGateAndUnknown(t *testing.T) {
	t.Parallel()
	now := time.Now()
	e, err := NewEWMA(.3, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	e.now = func() time.Time { return now }
	if e.Snapshot(now).State != api.Unknown {
		t.Fatal("missing latency measured as zero")
	}
	e.Observe(20 * time.Millisecond)
	if e.Update(now.Add(time.Hour)) {
		t.Fatal("wall time replaced sample-duration gate")
	}
	e.Observe(80 * time.Millisecond)
	if !e.Update(now) || e.Snapshot(now).EWMA != 50*time.Millisecond {
		t.Fatal(e.Snapshot(now))
	}
	e.Observe(200 * time.Millisecond)
	e.Update(now)
	got := e.Snapshot(now.Add(time.Second))
	if got.EWMA != 95*time.Millisecond || got.Count != 3 || got.Sum != 300*time.Millisecond || got.LastAge != time.Second {
		t.Fatal(got)
	}
	e.Observe(time.Duration(math.MaxInt64))
	if e.Update(now) || e.Snapshot(now).State != api.Unknown {
		t.Fatal("duration overflow became evidence")
	}
}

func TestMeterFrozenSourcesRatesHistoryAndStop(t *testing.T) {
	t.Parallel()
	now := time.Now()
	m, err := NewMeter(api.ReplicaIdentity{ClusterID: "c", GroupID: "g", NodeName: "n", ReplicaID: 1, RunID: "run"}, Options{MaxHistoryPoints: 3})
	if err != nil {
		t.Fatal(err)
	}
	if m.Snapshot(now).State != api.Unknown {
		t.Fatal("unsampled meter current")
	}
	sample := api.ReplicaSample{Ready: true, Running: true, Raft: api.Stamp{Sequence: 1, State: api.Current}, Writer: api.Stamp{Sequence: 1, State: api.Current}, Voters: []uint64{1, 2, 3}, Commit: 10, Applied: 8}
	m.Tick(now, sample)
	for i := 0; i < 6; i++ {
		m.ObserveProposal()
	}
	m.Tick(now.Add(3*time.Second), sample)
	got := m.Snapshot(now.Add(3 * time.Second))
	if got.Sample.Rates.Proposals != 2 || got.Sample.Rates.Window != 3*time.Second || got.ApplyBacklog.Value != 2 {
		t.Fatal(got)
	}
	got.Sample.Voters[0] = 999
	if m.Snapshot(now.Add(3 * time.Second)).Sample.Voters[0] != 1 {
		t.Fatal("snapshot leaked source slices")
	}
	// Independent meter sampling must retain the producer's growing age.
	sample.Raft.Age = 16 * time.Second
	sample.Raft.State = api.Stale
	m.Tick(now.Add(16*time.Second), sample)
	got = m.Snapshot(now.Add(16 * time.Second))
	if got.State != api.Stale || got.Sample.Meter.State != api.Current || got.Sample.Raft.Sequence != 1 {
		t.Fatal(got)
	}
	if _, err := m.History(now, api.HistoryQuery{SeriesID: "other"}); !errors.Is(err, fault.ErrHistoryUnavailable) {
		t.Fatal(err)
	}
	if _, err := m.History(now, api.HistoryQuery{SeriesID: "run", Limit: 4}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	h, err := m.History(now.Add(16*time.Second), api.HistoryQuery{SeriesID: "run", Limit: 1})
	if err != nil || len(h.Points) != 1 || h.Next != 1 {
		t.Fatal(h, err)
	}
	m.Stop()
	m.Tick(now.Add(17*time.Second), sample)
	got = m.Snapshot(now.Add(17 * time.Second))
	if got.State != api.Stale || got.Sample.Running || got.Sequence != 3 {
		t.Fatal(got)
	}
	h, err = m.History(now.Add(11*time.Minute), api.HistoryQuery{SeriesID: "run"})
	if err != nil || len(h.Points) != 0 || !h.Truncated {
		t.Fatal(h, err)
	}
}

func TestMeterConcurrentCountersSnapshotsAndHistory(t *testing.T) {
	t.Parallel()
	m, err := NewMeter(api.ReplicaIdentity{ClusterID: "c", GroupID: "g", NodeName: "n", ReplicaID: 1, RunID: "run"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	now := time.Now()
	for range 4 {
		wg.Go(func() {
			for range 100 {
				m.ObserveProposal()
				m.ObserveResult(time.Millisecond, Applied)
				m.Snapshot(now)
				_, _ = m.History(now, api.HistoryQuery{SeriesID: "run"})
			}
		})
	}
	for i := 0; i < 100; i++ {
		m.Tick(now.Add(time.Duration(i)*time.Second), api.ReplicaSample{})
	}
	wg.Wait()
	m.Tick(now.Add(101*time.Second), api.ReplicaSample{})
	got := m.Snapshot(now.Add(101 * time.Second))
	if got.Sample.Counters.Proposals != 400 || got.Sample.Counters.Results != 400 {
		t.Fatal(got.Sample.Counters)
	}
}

func TestSettingsDefaultsAndBounds(t *testing.T) {
	t.Parallel()
	s, err := (Settings{}).Resolve()
	if err != nil || s.SampleInterval != time.Second || s.CollectInterval != 5*time.Second || s.MaxParallelReads != 8 || s.MaxHistoryPoints != 600 {
		t.Fatal(s, err)
	}
	for _, bad := range []Settings{{SampleInterval: -1}, {CollectInterval: time.Second}, {CollectTimeout: 5 * time.Second}, {MaxSampleAge: time.Second}, {MaxHistoryPoints: -1}, {MaxParallelReads: -1}, {SampleInterval: time.Duration(math.MaxInt64)}, {MaxHistoryPoints: math.MaxInt}} {
		if _, err := bad.Resolve(); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(bad, err)
		}
	}
	s, err = (Settings{CollectTimeout: time.Millisecond, MaxSampleAge: time.Second + time.Millisecond}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Local().Resolve(); err != nil {
		t.Fatal("local settings reintroduced default collector timeout", err)
	}
}
