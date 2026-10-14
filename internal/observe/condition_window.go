package observe

import (
	"time"
)

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
