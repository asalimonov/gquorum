package observe

import (
	internalmetrics "github.com/asalimonov/gquorum/internal/metrics"
	"github.com/asalimonov/gquorum/metrics"
	"slices"
	"time"
)

// EvidenceScope fences temporal conclusions to one exact replica and policy,
// group leader, native membership, and scheduler leadership epoch.
type EvidenceScope struct {
	Replica          metrics.ReplicaIdentity
	Leader           metrics.ReplicaIdentity
	SchedulerEpoch   string
	Term             uint64
	MembershipDigest string
	PolicyDigest     string
}
type Generation struct {
	Source   string
	RunID    string
	Sequence uint64
	Age      time.Duration
}
type EvidenceSample struct {
	Valid, True bool
	Generations []Generation
}

// ConditionWindow is owned by a serialized collector. It credits only actual
// confirmations, never time spent reading a cached snapshot. Sources have
// independent sequence spaces and every required source must advance.
type ConditionWindow struct {
	MaxGap, MaxAge   time.Duration
	scope            EvidenceScope
	initialized      bool
	valid, truth     bool
	since, confirmed time.Time
	vector           []Generation
}

func (w *ConditionWindow) Reset() {
	w.initialized = false
	w.valid, w.truth = false, false
	w.since, w.confirmed = time.Time{}, time.Time{}
	w.vector = nil
}
func (w *ConditionWindow) resetPredicate() {
	w.valid, w.truth = false, false
	w.since = time.Time{}
}
func (w *ConditionWindow) Observe(now time.Time, scope EvidenceScope, sample EvidenceSample) {
	if !w.initialized || w.scope != scope {
		w.Reset()
		w.scope = scope
		w.initialized = true
	}
	if w.MaxGap <= 0 || w.MaxAge <= 0 || !sample.Valid || len(sample.Generations) == 0 || !w.confirmed.IsZero() && now.Before(w.confirmed) {
		w.resetPredicate()
		return
	}
	vector := slices.Clone(sample.Generations)
	slices.SortFunc(vector, func(a, b Generation) int {
		if a.Source < b.Source {
			return -1
		}
		if a.Source > b.Source {
			return 1
		}
		return 0
	})
	for i, g := range vector {
		if g.Source == "" || g.RunID == "" || g.Sequence == 0 || g.Age < 0 || g.Age > w.MaxAge || i > 0 && vector[i-1].Source == g.Source {
			w.resetPredicate()
			return
		}
	}
	advanced := true
	if len(w.vector) > 0 {
		if len(vector) != len(w.vector) {
			w.resetPredicate()
			w.vector = nil
		} else {
			for i, g := range vector {
				old := w.vector[i]
				if g.Source != old.Source || g.RunID != old.RunID || g.Sequence < old.Sequence {
					// Out-of-order data supplies no new baseline; a new source run
					// must arrive under a correspondingly changed EvidenceScope.
					w.resetPredicate()
					return
				}
				if g.Sequence == old.Sequence {
					advanced = false
				}
			}
		}
	}
	if !w.confirmed.IsZero() && internalmetrics.Age(now, w.confirmed) > w.MaxGap {
		w.resetPredicate()
	}
	// A fresh contradiction always breaks continuity, even if another source
	// did not advance. Replaying the previous true vector cannot undo it.
	if !sample.True {
		w.truth = false
		w.valid = true
		w.since = time.Time{}
		if advanced {
			w.confirmed = now
			w.vector = vector
		}
		return
	}
	if !advanced {
		return
	}
	if !w.truth || !w.valid {
		w.since = now
	}
	w.valid, w.truth = true, true
	w.confirmed = now
	w.vector = vector
}
func (w *ConditionWindow) Duration(now time.Time) metrics.DurationValue {
	if !w.valid || w.confirmed.IsZero() || now.Before(w.confirmed) || internalmetrics.Age(now, w.confirmed) > w.MaxGap {
		return metrics.DurationValue{State: metrics.Unknown}
	}
	elapsed := internalmetrics.Age(now, w.confirmed)
	for _, g := range w.vector {
		if internalmetrics.AddDuration(g.Age, elapsed) > w.MaxAge {
			return metrics.DurationValue{State: metrics.Stale}
		}
	}
	if !w.truth {
		return metrics.DurationValue{State: metrics.Current}
	}
	return metrics.DurationValue{State: metrics.Current, Value: max(0, w.confirmed.Sub(w.since))}
}
