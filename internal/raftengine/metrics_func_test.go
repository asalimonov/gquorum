//go:build functional

package raftengine

import (
	"context"
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3"
	"io"
	"log/slog"
	"testing"
	"time"
)

type metricsBlockedMachine struct {
	machine
	entered, release chan struct{}
}

func (m *metricsBlockedMachine) Apply(ctx context.Context, c Command) (Result, error) {
	close(m.entered)
	select {
	case <-m.release:
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
	return m.machine.Apply(ctx, c)
}

func metricsSingleton(t *testing.T, s storage.Store, m Machine) *Group {
	t.Helper()
	g, err := New(Options{GroupID: "load", NodeName: "1", ClusterID: "cluster", InstanceID: "process", ReplicaID: 1, Mode: Founding, Peers: []raft.Peer{{ID: 1}}, Store: s, Machine: m, Transport: endpoint{network: &network{}}, Timing: Timing{TickInterval: 10 * time.Millisecond}, Metrics: internalmetrics.Options{SampleInterval: 10 * time.Millisecond, MaxSampleAge: 100 * time.Millisecond}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	must(t, err)
	t.Cleanup(func() { must(t, g.Stop(testContext(t))) })
	must(t, g.Start(t.Context()))
	must(t, g.WaitReady(testContext(t)))
	eventually(t, g.IsLeader)
	eventually(t, func() bool { return g.Metrics().State == metrics.Current })
	return g
}

func TestMetricsApplyStallDoesNotFreshenRaft(t *testing.T) {
	t.Parallel()
	m := &metricsBlockedMachine{entered: make(chan struct{}), release: make(chan struct{})}
	g := metricsSingleton(t, openStore(t, t.TempDir(), "1"), m)
	before := g.Metrics()
	done := make(chan error, 1)
	go func() { _, err := g.Propose(testContext(t), Proposal{Data: []byte("blocked")}); done <- err }()
	select {
	case <-m.entered:
	case <-testContext(t).Done():
		t.Fatal("apply not reached")
	}
	eventually(t, func() bool {
		s := g.Metrics()
		return s.Sample.Raft.State == metrics.Stale && s.Sample.Meter.Sequence > before.Sample.Meter.Sequence
	})
	s := g.Metrics()
	if s.Sample.Writer.State != metrics.Current || s.State != metrics.Stale {
		t.Fatal(s)
	}
	close(m.release)
	must(t, <-done)
	eventually(t, func() bool {
		s := g.Metrics()
		return s.State == metrics.Current && s.Sample.Applied > s.Sample.Identity.ReplicaID && s.Sample.Counters.Results == 1
	})
	must(t, g.Stop(testContext(t)))
	if stopped := g.Metrics(); stopped.State != metrics.Stale || stopped.Sample.Running {
		t.Fatal(stopped)
	}
	page, err := g.MetricsHistory(metrics.HistoryQuery{SeriesID: g.Metrics().SeriesID})
	if err != nil || len(page.Points) == 0 || page.Points[0].State != metrics.Stale {
		t.Fatal(page, err)
	}
}

func TestMetricsWriterStallDoesNotFreshenWriter(t *testing.T) {
	t.Parallel()
	s := &failingStore{Store: openStore(t, t.TempDir(), "1"), entered: make(chan struct{})}
	g := metricsSingleton(t, s, &machine{})
	s.blocked.Store(true)
	done := make(chan error, 1)
	go func() { _, err := g.Propose(testContext(t), Proposal{Data: []byte("blocked")}); done <- err }()
	select {
	case <-s.entered:
	case <-testContext(t).Done():
		t.Fatal("write not reached")
	}
	eventually(t, func() bool {
		return g.Metrics().Sample.Writer.State == metrics.Stale && g.Metrics().Sample.WALStall >= g.opts.Metrics.MaxSampleAge
	})
	got := g.Metrics()
	if got.Sample.Raft.State != metrics.Current || got.Sample.Meter.State != metrics.Current || got.Sample.WALStall < 100*time.Millisecond {
		t.Fatal(got)
	}
	must(t, g.Stop(testContext(t)))
	if err := <-done; err == nil {
		t.Fatal("undurable proposal succeeded")
	}
}

func TestMetricsIdleSourcesAndSameProcessRestart(t *testing.T) {
	t.Parallel()
	s := openStore(t, t.TempDir(), "1")
	g := metricsSingleton(t, s, &machine{})
	first := g.Metrics()
	eventually(t, func() bool {
		next := g.Metrics()
		return next.Sample.Raft.Sequence > first.Sample.Raft.Sequence && next.Sample.Writer.Sequence > first.Sample.Writer.Sequence && next.Sample.Rates.State == metrics.Current
	})
	if got := g.Metrics(); got.Sample.Rates.Proposals != 0 || got.Sample.ResultLatency.State != metrics.Unknown {
		t.Fatal(got)
	}
	must(t, g.Stop(testContext(t)))
	restarted := metricsSingleton(t, s, &machine{})
	if restarted.Metrics().SeriesID == first.SeriesID || restarted.Metrics().Sample.Identity.InstanceID != first.Sample.Identity.InstanceID {
		t.Fatal("engine restart did not change run within same process")
	}
	if _, err := restarted.MetricsHistory(metrics.HistoryQuery{SeriesID: first.SeriesID}); err == nil {
		t.Fatal("successor read predecessor history")
	}
}
