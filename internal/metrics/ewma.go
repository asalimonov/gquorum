package metrics

import (
	"github.com/asalimonov/gquorum/fault"
	api "github.com/asalimonov/gquorum/metrics"
	"math"
	"sync"
	"time"
)

// EWMA ports Meridian's duration-gated delta rule. A short mutex keeps sum and
// count coherent (independent atomic reads can observe only half an observation).
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

func NewEWMA(weight float64, minSamples time.Duration) (*EWMA, error) {
	if math.IsNaN(weight) || weight <= 0 || weight > 1 || minSamples < 0 {
		return nil, fault.ErrInvalid
	}
	return &EWMA{weight: weight, minimum: minSamples, now: time.Now}, nil
}
func (e *EWMA) Observe(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	d = max(d, 0)
	if e.count == math.MaxUint64 || e.pendingCount == math.MaxUint64 || e.sum > time.Duration(math.MaxInt64)-d || e.pending > time.Duration(math.MaxInt64)-d {
		e.overflow = true
		e.seeded = false
		return
	}
	e.count++
	e.pendingCount++
	e.sum += d
	e.pending += d
	e.last = e.now()
}
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
func (e *EWMA) Snapshot(now time.Time) api.Latency {
	e.mu.Lock()
	defer e.mu.Unlock()
	state := api.Unknown
	if e.seeded && !e.overflow {
		state = api.Current
	}
	return api.Latency{Count: e.count, Sum: e.sum, EWMA: e.value, LastAge: Age(now, e.last), State: state}
}
