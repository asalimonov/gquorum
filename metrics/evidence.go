package metrics

import "time"

// UnsignedValue pairs an unsigned measurement with its validity.
type UnsignedValue struct {
	State Validity `json:"state"`
	Value uint64   `json:"value"`
}

// DurationValue pairs a duration measurement with its validity.
type DurationValue struct {
	State Validity      `json:"state"`
	Value time.Duration `json:"value_ns"`
}

// Evidence summarizes observed failure, lag, progress and standby windows.
type Evidence struct {
	UnavailableFor            DurationValue `json:"unavailable_for"`
	ReplicationLagFor         DurationValue `json:"replication_lag_for"`
	NoProgressFor             DurationValue `json:"no_progress_for"`
	StandbyReadyFor           DurationValue `json:"standby_ready_for"`
	DemotionCooldownRemaining time.Duration `json:"demotion_cooldown_remaining_ns"`
	Reasons                   []string      `json:"reasons"` // bounded library vocabulary
}
