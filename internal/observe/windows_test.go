//go:build !functional

package observe

import (
	"github.com/asalimonov/gquorum/metrics"
	"testing"
	"time"
)

func windowSample(seq uint64, truth bool) EvidenceSample {
	return EvidenceSample{Valid: true, True: truth, Generations: []Generation{{Source: "raft", RunID: "leader-run", Sequence: seq}, {Source: "writer", RunID: "replica-run", Sequence: seq}, {Source: "ack", RunID: "leader-run", Sequence: seq}}}
}
func TestWindowContinuousThresholdsAndCachedReads(t *testing.T) {
	t.Parallel()
	now := time.Now()
	w := ConditionWindow{MaxGap: 15 * time.Second, MaxAge: 15 * time.Second}
	scope := EvidenceScope{SchedulerEpoch: "observer1"}
	for i := 0; i <= 60; i++ {
		at := now.Add(time.Duration(i) * time.Second)
		w.Observe(at, scope, windowSample(uint64(i+1), true))
		got := w.Duration(at)
		if got.State != metrics.Current || got.Value != time.Duration(i)*time.Second {
			t.Fatal(i, got)
		}
		// 29/30 and 59/60 are actual confirmation boundaries, not read timers.
		if cached := w.Duration(at.Add(500 * time.Millisecond)); cached.Value != got.Value {
			t.Fatal("read earned evidence", cached)
		}
	}
	w.Observe(now.Add(65*time.Second), scope, windowSample(61, true))
	if got := w.Duration(now.Add(65 * time.Second)); got.Value != time.Minute {
		t.Fatal("duplicate extended evidence", got)
	}
	if got := w.Duration(now.Add(76 * time.Second)); got.State == metrics.Current {
		t.Fatal("stale window still actionable", got)
	}
	w.Observe(now.Add(77*time.Second), scope, windowSample(62, true))
	if got := w.Duration(now.Add(77 * time.Second)); got.State != metrics.Current || got.Value != 0 {
		t.Fatal("gap did not reset", got)
	}
}

func TestWindowEverySourceAdvancesAndContradictionsReset(t *testing.T) {
	t.Parallel()
	now := time.Now()
	scope := EvidenceScope{SchedulerEpoch: "observer"}
	w := ConditionWindow{MaxGap: 15 * time.Second, MaxAge: 15 * time.Second}
	w.Observe(now, scope, windowSample(1, true))
	w.Observe(now.Add(5*time.Second), scope, windowSample(2, true))
	partial := windowSample(3, true)
	partial.Generations[1].Sequence = 2
	w.Observe(now.Add(10*time.Second), scope, partial)
	if got := w.Duration(now.Add(10 * time.Second)); got.Value != 5*time.Second {
		t.Fatal("fresh leader refreshed old writer", got)
	}
	partial.True = false
	w.Observe(now.Add(11*time.Second), scope, partial)
	if got := w.Duration(now.Add(11 * time.Second)); got.Value != 0 {
		t.Fatal("contradiction kept continuity", got)
	}
	w.Observe(now.Add(12*time.Second), scope, windowSample(4, true))
	if got := w.Duration(now.Add(12 * time.Second)); got.Value != 0 {
		t.Fatal(got)
	}
	stale := windowSample(5, true)
	stale.Generations[0].Age = 16 * time.Second
	w.Observe(now.Add(13*time.Second), scope, stale)
	if got := w.Duration(now.Add(13 * time.Second)); got.State != metrics.Unknown {
		t.Fatal(got)
	}
	w.Observe(now.Add(14*time.Second), scope, windowSample(6, true))
	w.Observe(now.Add(15*time.Second), scope, windowSample(5, true))
	if got := w.Duration(now.Add(15 * time.Second)); got.State != metrics.Unknown {
		t.Fatal("out-of-order refreshed evidence", got)
	}
	w.Observe(now.Add(16*time.Second), scope, windowSample(7, true))
	if got := w.Duration(now.Add(16 * time.Second)); got.Value != 0 {
		t.Fatal("reordered sample preserved window", got)
	}
	scope.Term++
	w.Observe(now.Add(17*time.Second), scope, windowSample(1, true))
	if got := w.Duration(now.Add(17 * time.Second)); got.Value != 0 || got.State != metrics.Current {
		t.Fatal("new scope inherited or rejected old generation", got)
	}
}

func TestWindowAgesDoNotTrustDisplayClockOrEmptySources(t *testing.T) {
	t.Parallel()
	now := time.Now()
	scope := EvidenceScope{}
	w := ConditionWindow{MaxGap: 15 * time.Second, MaxAge: 10 * time.Second}
	s := windowSample(1, true)
	s.Generations[1].Age = 8 * time.Second
	w.Observe(now, scope, s)
	if got := w.Duration(now.Add(3 * time.Second)); got.State != metrics.Stale {
		t.Fatal("source ages ignored", got)
	}
	w.Observe(now, scope, EvidenceSample{Valid: true, True: true})
	if got := w.Duration(now); got.State != metrics.Unknown {
		t.Fatal("empty vector proved stability", got)
	}
}
