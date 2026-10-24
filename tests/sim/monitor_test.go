//go:build sim

package sim_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
)

type probeResult struct {
	name    string
	started time.Time
	elapsed time.Duration
	err     error
}

type quorumMonitor struct {
	stopSending chan struct{}
	done        chan []probeResult
	started     time.Time
	nodes       int
	once        sync.Once
}

func startMonitor(nodes []*simNode) *quorumMonitor {
	m := &quorumMonitor{stopSending: make(chan struct{}), done: make(chan []probeResult, 1), nodes: len(nodes)}
	started := make(chan struct{})
	go func() {
		ticker := time.NewTicker(probeInterval)
		defer ticker.Stop()
		m.started = time.Now()
		close(started)
		var pending sync.WaitGroup
		var mu sync.Mutex
		var results []probeResult
		for {
			select {
			case <-m.stopSending:
				pending.Wait()
				m.done <- results
				return
			case tick := <-ticker.C:
				for _, node := range nodes {
					pending.Add(1)
					go func() {
						defer pending.Done()
						ctx, cancel := context.WithDeadline(context.Background(), tick.Add(probeTimeout))
						defer cancel()
						err := node.call(ctx, func(ctx context.Context, n gquorum.Node) error { return n.Cluster().ReadBarrier(ctx) })
						result := probeResult{node.name, tick, time.Since(tick), err}
						mu.Lock()
						results = append(results, result)
						mu.Unlock()
					}()
				}
			}
		}
	}()
	<-started
	return m
}

func (m *quorumMonitor) stop(t *testing.T) {
	t.Helper()
	m.once.Do(func() { m.check(t) })
}

func (m *quorumMonitor) check(t *testing.T) {
	t.Helper()
	stopped := time.Now()
	close(m.stopSending)
	results := <-m.done
	slices.SortFunc(results, func(a, b probeResult) int { return a.started.Compare(b.started) })
	last := make(map[string]time.Time)
	var slowest time.Duration
	failures := 0
	for _, result := range results {
		slowest = max(slowest, result.elapsed)
		if result.err != nil {
			failures++
			if failures <= 5 {
				t.Errorf("%s quorum check at %s failed after %s: %v", result.name, result.started, result.elapsed, result.err)
			}
		}
		previous, ok := last[result.name]
		if !ok {
			previous = m.started
		}
		if gap := result.started.Sub(previous); gap != probeInterval {
			t.Errorf("%s probe cadence changed: %s", result.name, gap)
		}
		last[result.name] = result.started
	}
	// At an exact tick boundary the stop and tick may be selected in either
	// order. Allow only that final tick; a stalled monitor must fail.
	minimum := max(1, int(stopped.Sub(m.started)/probeInterval)-1) * m.nodes
	if len(results) < minimum || len(last) != m.nodes {
		t.Errorf("incomplete quorum sampling: checks=%d minimum=%d nodes=%d/%d", len(results), minimum, len(last), m.nodes)
	}
	t.Logf("quorum checks=%d failures=%d cadence=%s deadline=%s slowest=%s", len(results), failures, probeInterval, probeTimeout, slowest)
}
