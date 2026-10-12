//go:build functional

package observe

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

type collectorSource struct {
	mu      sync.Mutex
	doc     topology.Document
	epoch   LeaderEpoch
	leader  bool
	round   Round
	local   []metrics.ReplicaObservation
	collect func(context.Context, *ReadPool) (Round, error)
}

func (s *collectorSource) Active(context.Context) (topology.Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.doc.Clone(), nil
}
func (s *collectorSource) Epoch() (LeaderEpoch, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch, s.leader
}
func (s *collectorSource) Collect(ctx context.Context, _ topology.Document, p *ReadPool) (Round, error) {
	s.mu.Lock()
	f, r := s.collect, s.round
	s.mu.Unlock()
	if f != nil {
		return f(ctx, p)
	}
	return r, nil
}
func (s *collectorSource) locals() []metrics.ReplicaObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]metrics.ReplicaObservation, len(s.local))
	for i := range s.local {
		out[i] = s.local[i].Clone()
	}
	return out
}
func collectorFixture(t *testing.T) (*MetricsCollector, *collectorSource, *time.Time) {
	t.Helper()
	doc, settings := evidenceFixture()
	round, local := evidenceRound(doc, 1)
	now := time.Unix(100, 0)
	s := &collectorSource{doc: doc, epoch: round.Epoch, leader: true, round: round, local: local}
	settings.MaxHistoryPoints = 3
	settings.HistoryRetention = 10 * time.Second
	c, err := NewMetricsCollector(MetricsConfig{Source: s, Settings: settings, Local: s.locals, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return c, s, &now
}
func refreshMetrics(t *testing.T, c *MetricsCollector) {
	t.Helper()
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestMetricsCollectorHistoryBoundedAcrossRuns(t *testing.T) {
	t.Parallel()
	c, s, now := collectorFixture(t)
	refreshMetrics(t, c)
	first := c.Snapshot().Local[0].SeriesID
	if first == "" {
		t.Fatal("no owned series")
	}
	for i := 2; i <= 6; i++ {
		*now = now.Add(time.Second)
		s.round, s.local = evidenceRound(s.doc, uint64(i))
		refreshMetrics(t, c)
	}
	page, err := c.History(metrics.HistoryQuery{SeriesID: first, Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if !page.Truncated || len(page.Points) != 3 || page.Next != 6 {
		t.Fatalf("page %+v", page)
	}
	page.Points[0].Sample.Voters[0] = 99
	again, _ := c.History(metrics.HistoryQuery{SeriesID: first})
	if again.Points[0].Sample.Voters[0] == 99 {
		t.Fatal("aliased history")
	}
	// Each engine restart retains only one retired series for the exact replica.
	for _, run := range []string{"next-run", "third-run"} {
		*now = now.Add(time.Second)
		s.round, s.local = evidenceRound(s.doc, 1)
		g := s.round.Groups[topology.MetaGroupID]
		g.Probes[0].Identity.RunID = run
		g.Probes[0].Observation.Sample.Identity.RunID = run
		s.round.Groups[topology.MetaGroupID] = g
		s.local[0].Sample.Identity.RunID = run
		refreshMetrics(t, c)
	}
	if _, err := c.History(metrics.HistoryQuery{SeriesID: first}); !errors.Is(err, fault.ErrHistoryUnavailable) {
		t.Fatal("old series survived", err)
	}
	current := c.Snapshot().Local[0].SeriesID
	*now = now.Add(11 * time.Second)
	page, err = c.History(metrics.HistoryQuery{SeriesID: current})
	if err != nil || len(page.Points) != 0 {
		t.Fatalf("read did not expire stopped history: %+v %v", page, err)
	}
	if _, err = c.History(metrics.HistoryQuery{SeriesID: current, Limit: 4}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestMetricsCollectorCounterExhaustionPreservesHistoryAndInvalidatesDecisions(t *testing.T) {
	t.Parallel()
	for _, counter := range []string{"epoch", "series", "cursor"} {
		t.Run(counter, func(t *testing.T) {
			t.Parallel()
			c, source, now := collectorFixture(t)
			refreshMetrics(t, c)
			series := c.Snapshot().Local[0].SeriesID
			c.stateMu.Lock()
			switch counter {
			case "epoch":
				c.epochSequence = math.MaxUint64
			case "series":
				c.nextSeries = math.MaxUint64
			case "cursor":
				for _, pair := range c.series {
					pair.current.sequence = math.MaxUint64
				}
			}
			c.stateMu.Unlock()
			if counter != "cursor" {
				c.ResetDecisions()
			}
			for i := range 2 {
				*now = now.Add(time.Second)
				source.mu.Lock()
				source.round, source.local = evidenceRound(source.doc, uint64(i+2))
				source.mu.Unlock()
				refreshMetrics(t, c)
				view := c.Snapshot()
				if view.IsCollectorLeader || len(view.Groups) != 0 || !c.generationExhausted {
					t.Fatal("exhausted counter retained actionable evidence", view)
				}
			}
			page, err := c.History(metrics.HistoryQuery{SeriesID: series})
			if err != nil || len(page.Points) == 0 {
				t.Fatal("exhaustion destroyed diagnostic history", page, err)
			}
			if counter == "epoch" && c.epochSequence != math.MaxUint64 || counter == "series" && c.nextSeries != math.MaxUint64 {
				t.Fatal("generation counter wrapped")
			}
			if counter == "cursor" {
				for _, pair := range c.series {
					if pair.current.sequence != math.MaxUint64 {
						t.Fatal("history cursor wrapped")
					}
				}
			}
		})
	}
}
func TestMetricsCollectorDuplicateReadsDoNotCreateHistory(t *testing.T) {
	t.Parallel()
	c, _, now := collectorFixture(t)
	refreshMetrics(t, c)
	id := c.Snapshot().Local[0].SeriesID
	for range 4 {
		*now = now.Add(time.Second)
		refreshMetrics(t, c)
		_ = c.Snapshot()
	}
	page, err := c.History(metrics.HistoryQuery{SeriesID: id})
	if err != nil || len(page.Points) != 1 || page.Next != 1 {
		t.Fatalf("duplicate generated history %+v %v", page, err)
	}
}
func TestMetricsCollectorDiscardsRoundAcrossLeadershipOrTopology(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"leadership", "topology"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			c, s, _ := collectorFixture(t)
			refreshMetrics(t, c)
			s.collect = func(context.Context, *ReadPool) (Round, error) {
				s.mu.Lock()
				defer s.mu.Unlock()
				if change == "topology" {
					s.doc.Version++
				} else {
					s.leader = false
				}
				return s.round, nil
			}
			err := c.Refresh(context.Background())
			if err == nil {
				t.Fatal("accepted stale round")
			}
			if c.Snapshot().IsCollectorLeader || len(c.Snapshot().Groups) != 0 {
				t.Fatal("stale remote evidence remained actionable")
			}
			if len(c.Snapshot().Local) != 1 {
				t.Fatal("follower lost local observation")
			}
		})
	}
}
func TestMetricsCollectorCancellationRetainsReadOwnership(t *testing.T) {
	t.Parallel()
	c, s, _ := collectorFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	s.collect = func(ctx context.Context, p *ReadPool) (Round, error) {
		err := p.DoKey(ctx, "exact-source", func(context.Context) error { close(entered); <-release; return nil })
		return s.round, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Refresh(ctx) }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := c.Stop(short)
	stop()
	if !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatalf("released live callback: %v", err)
	}
	close(release)
	if err := c.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}
