// Package metrics implements owner-driven measurement and bounded history.
// The ring, EWMA and elapsed-rate rules adapt Meridian's cluster/loadmeter;
// this package owns no goroutines or application policy.
package metrics

import (
	"github.com/asalimonov/gquorum/fault"
	"math"
	"time"
)

type Settings struct {
	SampleInterval   time.Duration
	CollectInterval  time.Duration
	CollectTimeout   time.Duration
	MaxSampleAge     time.Duration
	HistoryRetention time.Duration
	MaxHistoryPoints int
	MaxParallelReads int
}

func (s Settings) Resolve() (Settings, error) {
	if s.SampleInterval < 0 || s.CollectInterval < 0 || s.CollectTimeout < 0 || s.MaxSampleAge < 0 || s.HistoryRetention < 0 || s.MaxHistoryPoints < 0 || s.MaxParallelReads < 0 {
		return Settings{}, fault.New(fault.Invalid, "negative metrics interval or bound", nil)
	}
	if s.SampleInterval == 0 {
		s.SampleInterval = time.Second
	}
	if s.CollectInterval == 0 {
		s.CollectInterval = 5 * time.Second
	}
	if s.CollectTimeout == 0 {
		s.CollectTimeout = 2 * time.Second
	}
	if s.MaxSampleAge == 0 {
		s.MaxSampleAge = 15 * time.Second
	}
	if s.HistoryRetention == 0 {
		s.HistoryRetention = 10 * time.Minute
	}
	if s.MaxHistoryPoints == 0 {
		s.MaxHistoryPoints = 600
	}
	if s.MaxParallelReads == 0 {
		s.MaxParallelReads = 8
	}
	if s.CollectTimeout >= s.CollectInterval || s.SampleInterval > math.MaxInt64-s.CollectTimeout || s.MaxSampleAge < s.SampleInterval+s.CollectTimeout {
		return Settings{}, fault.New(fault.Invalid, "metrics ages must cover sampling and collection budget; timeout must be below interval", nil)
	}
	if int64(s.MaxHistoryPoints) > math.MaxInt64/int64(s.SampleInterval) {
		return Settings{}, fault.New(fault.Invalid, "metrics history duration overflows", nil)
	}
	return s, nil
}

// Options are the resolved settings used by a single replica meter.
type Options struct {
	SampleInterval   time.Duration
	MaxSampleAge     time.Duration
	HistoryRetention time.Duration
	MaxHistoryPoints int
}

func (s Settings) Local() Options {
	return Options{s.SampleInterval, s.MaxSampleAge, s.HistoryRetention, s.MaxHistoryPoints}
}
func (o Options) Resolve() (Options, error) {
	if o.SampleInterval < 0 || o.MaxSampleAge < 0 || o.HistoryRetention < 0 || o.MaxHistoryPoints < 0 {
		return Options{}, fault.ErrInvalid
	}
	if o.SampleInterval == 0 {
		o.SampleInterval = time.Second
	}
	if o.MaxSampleAge == 0 {
		o.MaxSampleAge = 15 * time.Second
	}
	if o.HistoryRetention == 0 {
		o.HistoryRetention = 10 * time.Minute
	}
	if o.MaxHistoryPoints == 0 {
		o.MaxHistoryPoints = 600
	}
	if o.MaxSampleAge < o.SampleInterval || int64(o.MaxHistoryPoints) > math.MaxInt64/int64(o.SampleInterval) {
		return Options{}, fault.ErrInvalid
	}
	return o, nil
}

func Age(now, at time.Time) time.Duration {
	if at.IsZero() {
		return time.Duration(math.MaxInt64)
	}
	return max(0, now.Sub(at))
}
func AddDuration(a, b time.Duration) time.Duration {
	a, b = max(0, a), max(0, b)
	if a > time.Duration(math.MaxInt64)-b {
		return time.Duration(math.MaxInt64)
	}
	return a + b
}
