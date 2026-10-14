package metrics

import (
	"math"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	api "github.com/asalimonov/gquorum/metrics"
)

// EWMA ports Meridian's duration-gated delta rule. A short mutex keeps sum and
// count coherent (independent atomic reads can observe only half an
// observation).
// The supplied clock preserves monotonic elapsed time; Update owns the cadence.
type EWMA struct {
	mu                  sync.Mutex
	weight              float64
	minimum             time.Duration
	sum, pending        time.Duration
	count, pendingCount uint64
	value               time.Duration
	seeded, overflow    bool
	last                time.Time
	now                 func() time.Time
}

// NewEWMA validates the weight and minimum accumulated sample duration.
func NewEWMA(weight float64, minSamples time.Duration) (*EWMA, error) {
	if weight <= 0 || weight > 1 || minSamples < 0 || math.IsNaN(weight) {
		return nil, fault.ErrInvalid
	}
	return &EWMA{weight: weight, minimum: minSamples, now: time.Now}, nil
}

// Observe accumulates a nonnegative duration and invalidates the average on
// overflow.
func (e *EWMA) Observe(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	duration := max(d, 0)
	if e.count == math.MaxUint64 || e.pendingCount == math.MaxUint64 || e.sum > time.Duration(math.MaxInt64)-duration ||
		e.pending > time.Duration(math.MaxInt64)-duration {
		e.overflow = true
		e.seeded = false
		return
	}
	e.count++
	e.pendingCount++
	e.sum += duration
	e.pending += duration
	e.last = e.now()
}

// Update advances the moving average once pending samples reach the minimum
// duration.
func (e *EWMA) Update(_ time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.overflow || e.pendingCount == 0 || e.pending < e.minimum {
		return false
	}
	avg := float64(e.pending) / float64(e.pendingCount)
	if e.seeded {
		avg = e.weight*avg + (1-e.weight)*float64(e.value)
	}
	if avg >= float64(math.MaxInt64) {
		e.value = time.Duration(math.MaxInt64)
	} else {
		e.value = time.Duration(avg)
	}
	e.seeded = true
	e.pending, e.pendingCount = 0, 0
	return true
}

// Snapshot reports cumulative latency, the current average and age at the
// supplied time.
func (e *EWMA) Snapshot(now time.Time) api.Latency {
	e.mu.Lock()
	defer e.mu.Unlock()
	state := api.Unknown
	if e.seeded && !e.overflow {
		state = api.Current
	}
	return api.Latency{Count: e.count, Sum: e.sum, EWMA: e.value, LastAge: Age(now, e.last), State: state}
}
