//go:build functional

package observe

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type sourceFixture struct {
	mu      sync.RWMutex
	doc     topology.Document
	health  map[string]topology.Health
	fail    error
	blocked bool
	entered chan struct{}
	once    sync.Once
}

func (s *sourceFixture) Active(ctx context.Context) (topology.Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.doc.Clone(), ctx.Err()
}
func (s *sourceFixture) Health(ctx context.Context, _ topology.Document) (map[string]topology.Health, error) {
	s.mu.RLock()
	blocked, entered := s.blocked, s.entered
	health, err := maps.Clone(s.health), s.fail
	s.mu.RUnlock()
	if blocked {
		if entered != nil {
			s.once.Do(func() { close(entered) })
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return health, err
}
func observationDoc() topology.Document {
	doc := topology.Document{Version: 1, ClusterID: "observation", Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3}}
	for i, n := range []string{"n1", "n2", "n3"} {
		doc.Nodes = append(doc.Nodes, topology.Node{Name: n, Type: "worker", State: topology.NodeActive})
		if i == 0 {
			doc.Groups = []topology.Group{{ID: topology.MetaGroupID, NextReplicaID: 4}, {ID: "work", NextReplicaID: 4}}
		}
		for j := range doc.Groups {
			doc.Groups[j].Replicas = append(doc.Groups[j].Replicas, topology.Replica{NodeName: n, ReplicaID: uint64(i + 1), Role: topology.ReplicaVoter})
		}
	}
	return doc
}
func newSource() *sourceFixture {
	return &sourceFixture{doc: observationDoc(), health: map[string]topology.Health{"n1": topology.HealthAlive, "n2": topology.HealthAlive, "n3": topology.HealthAlive}}
}
func newWatch(t *testing.T, s *sourceFixture, cfg Config) *Watch {
	t.Helper()
	cfg.Source = s
	w, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func refresh(t *testing.T, w *Watch) {
	t.Helper()
	if err := w.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWatchStartStopLifecycle(t *testing.T) {
	t.Parallel()
	s := newSource()
	w := newWatch(t, s, Config{})
	if err := w.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	w = newWatch(t, s, Config{})
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	for range 2 {
		if err := w.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Refresh(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
}
func TestWatchReflectsDegradationIntoOwnedSnapshot(t *testing.T) {
	t.Parallel()
	s := newSource()
	w := newWatch(t, s, Config{})
	refresh(t, w)
	for i, expected := range []topology.GroupHealth{topology.GroupHealthy, topology.GroupDegraded, topology.GroupQuorumLost} {
		if i > 0 {
			s.health[[]string{"n1", "n2"}[i-1]] = topology.HealthDead
			refresh(t, w)
		}
		for _, g := range w.Snapshot().Groups {
			if g.ObservedHealth.State != expected || *g.ObservedHealth.HealthyVoters != 3-i {
				t.Fatal(g)
			}
		}
	}
}
func TestWatchPublishesMetaAndDataUniformly(t *testing.T) {
	t.Parallel()
	s := newSource()
	w := newWatch(t, s, Config{})
	refresh(t, w)
	snapshot := w.Snapshot()
	if snapshot.Version != 1 || snapshot.At.IsZero() || len(snapshot.Groups) != 2 || snapshot.Groups[0].GroupID != topology.MetaGroupID || snapshot.Groups[1].GroupID != "work" {
		t.Fatal(snapshot)
	}
	for _, g := range snapshot.Groups {
		if g.Voters != 3 || g.Quorum != 2 || g.ObservedHealth.State != topology.GroupHealthy {
			t.Fatal(g)
		}
	}
}
func TestWatchBelowMinReplicasSeparate(t *testing.T) {
	t.Parallel()
	s := newSource()
	s.doc.Groups[1].Placement = &topology.PlacementRule{MinReplicas: 3, MaxReplicas: 3}
	s.health["n1"] = topology.HealthDead
	w := newWatch(t, s, Config{})
	refresh(t, w)
	groups := w.Snapshot().Groups
	if groups[0].ObservedHealth.BelowMinReplicas != nil || groups[1].ObservedHealth.BelowMinReplicas == nil || !*groups[1].ObservedHealth.BelowMinReplicas || groups[1].ObservedHealth.State != topology.GroupDegraded {
		t.Fatal(groups)
	}
}
func TestWatchUnknownRemovesNumericObservations(t *testing.T) {
	t.Parallel()
	s := newSource()
	w := newWatch(t, s, Config{})
	refresh(t, w)
	s.health = nil
	refresh(t, w)
	for _, g := range w.Snapshot().Groups {
		if g.Voters != 3 || g.Quorum != 2 || g.ObservedHealth.State != topology.GroupUnknown || g.ObservedHealth.HealthyVoters != nil || g.ObservedHealth.FailureTolerance != nil || g.ObservedHealth.BelowMinReplicas != nil {
			t.Fatal(g)
		}
	}
	if w.Snapshot().Nodes["n1"] != topology.HealthUnknown {
		t.Fatal("stale node observation")
	}
}
func TestWatchRemovedGroupCleared(t *testing.T) {
	t.Parallel()
	s := newSource()
	w := newWatch(t, s, Config{})
	refresh(t, w)
	s.doc.Version++
	s.doc.Groups = s.doc.Groups[:1]
	refresh(t, w)
	if len(w.Snapshot().Groups) != 1 || len(w.previous) != 1 || len(w.everKnown) != 1 {
		t.Fatal("removed group left stale observation", w.Snapshot())
	}
}
func TestWatchTransitionLogsOncePerChange(t *testing.T) {
	t.Parallel()
	s := newSource()
	s.doc.Groups = s.doc.Groups[1:]
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	w := newWatch(t, s, Config{Logger: logger})
	refresh(t, w)
	refresh(t, w)
	s.health["n1"] = topology.HealthDead
	refresh(t, w)
	refresh(t, w)
	s.health["n2"] = topology.HealthDead
	refresh(t, w)
	s.health["n1"] = topology.HealthAlive
	s.health["n2"] = topology.HealthAlive
	refresh(t, w)
	for _, level := range []string{"DEBUG", "WARN", "ERROR", "INFO"} {
		if strings.Count(buf.String(), "level="+level) != 1 {
			t.Fatal(buf.String())
		}
	}
}
func TestWatchOnTransitionFiresOncePerEdgeEvenWithoutLogger(t *testing.T) {
	t.Parallel()
	s := newSource()
	s.doc.Groups = s.doc.Groups[1:]
	var edges []topology.GroupHealth
	var nodes []topology.Health
	w := newWatch(t, s, Config{OnGroupTransition: func(old topology.GroupHealth, next topology.GroupHealthEvaluation) {
		edges = append(edges, old, next.ObservedHealth.State)
		if next.ObservedHealth.HealthyVoters != nil {
			*next.ObservedHealth.HealthyVoters = 99
		}
	}, OnNodeTransition: func(_ string, _ topology.Health, next topology.Health) { nodes = append(nodes, next) }})
	refresh(t, w)
	refresh(t, w)
	s.health["n1"] = topology.HealthDead
	refresh(t, w)
	refresh(t, w)
	s.health["n2"] = topology.HealthDead
	refresh(t, w)
	s.health["n1"] = topology.HealthAlive
	s.health["n2"] = topology.HealthAlive
	refresh(t, w)
	want := []topology.GroupHealth{topology.GroupUnknown, topology.GroupHealthy, topology.GroupHealthy, topology.GroupDegraded, topology.GroupDegraded, topology.GroupQuorumLost, topology.GroupQuorumLost, topology.GroupHealthy}
	if len(edges) != len(want) {
		t.Fatal(edges)
	}
	for i, e := range want {
		if edges[i] != e {
			t.Fatal(edges)
		}
	}
	if *w.Snapshot().Groups[0].ObservedHealth.HealthyVoters != 3 || len(nodes) != 7 {
		t.Fatal("alias or duplicate node callbacks", w.Snapshot(), nodes)
	}
}
func TestWatchObservationLossWarnsOnce(t *testing.T) {
	t.Parallel()
	s := newSource()
	s.doc.Groups = s.doc.Groups[1:]
	known := maps.Clone(s.health)
	s.health = nil
	var buf bytes.Buffer
	w := newWatch(t, s, Config{Logger: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))})
	refresh(t, w)
	if buf.Len() != 0 {
		t.Fatal("boot unknown logged", buf.String())
	}
	s.health = known
	refresh(t, w)
	s.health = nil
	refresh(t, w)
	refresh(t, w)
	if strings.Count(buf.String(), "level=DEBUG") != 1 || strings.Count(buf.String(), "level=WARN") != 1 {
		t.Fatal(buf.String())
	}
}
func TestWatchSnapshotOwnershipAndConcurrentReaders(t *testing.T) {
	t.Parallel()
	s := newSource()
	w := newWatch(t, s, Config{})
	refresh(t, w)
	snapshot := w.Snapshot()
	snapshot.Nodes["n1"] = topology.HealthDead
	*snapshot.Groups[0].ObservedHealth.HealthyVoters = 99
	if w.Snapshot().Nodes["n1"] != topology.HealthAlive || *w.Snapshot().Groups[0].ObservedHealth.HealthyVoters != 3 {
		t.Fatal("snapshot aliased")
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				_ = w.Snapshot()
			}
		})
	}
	for range 100 {
		refresh(t, w)
	}
	wg.Wait()
}
func TestWatchBoundedSourceFailureAndJoinedCancellation(t *testing.T) {
	t.Parallel()
	s := newSource()
	s.blocked = true
	s.entered = make(chan struct{})
	w := newWatch(t, s, Config{Settings: Settings{Interval: 100 * time.Millisecond, Timeout: 20 * time.Millisecond}})
	start := time.Now()
	err := w.Refresh(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatal(err)
	}
	for _, g := range w.Snapshot().Groups {
		if g.ObservedHealth.State != topology.GroupUnknown {
			t.Fatal(g)
		}
	}
	s.once = sync.Once{}
	s.entered = make(chan struct{})
	w = newWatch(t, s, Config{Settings: Settings{Interval: time.Second, Timeout: 900 * time.Millisecond}})
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.entered:
	case <-time.After(time.Second):
		t.Fatal("source was not entered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if !w.Snapshot().At.IsZero() {
		t.Fatal("shutdown manufactured observation loss")
	}
}
func TestWatchSettingsValidation(t *testing.T) {
	t.Parallel()
	for _, settings := range []Settings{{Interval: -1}, {Timeout: -1}, {Interval: time.Second, Timeout: time.Second}} {
		if _, err := New(Config{Source: newSource(), Settings: settings}); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(settings, err)
		}
	}
	if _, err := New(Config{}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
}
