package metrics

import "time"

// Latency summarizes cumulative durations and their moving average.
type Latency struct {
	Count   uint64        `json:"count"`
	Sum     time.Duration `json:"sum_ns"`
	EWMA    time.Duration `json:"ewma_ns"`
	LastAge time.Duration `json:"last_age_ns"`
	State   Validity      `json:"state"`
}

// Counters records cumulative proposal, transport and leadership activity.
type Counters struct {
	Proposals     uint64 `json:"proposals"`
	Results       uint64 `json:"results"`
	Timeouts      uint64 `json:"timeouts"`
	Rejected      uint64 `json:"rejected"`
	WALRetries    uint64 `json:"wal_retries"`
	SendErrors    uint64 `json:"send_errors"`
	LeaderChanges uint64 `json:"leader_changes"`
}

// Rates reports proposal activity per second over a sampling window.
type Rates struct {
	State     Validity      `json:"state"`
	Window    time.Duration `json:"window_ns"`
	Proposals float64       `json:"proposals_per_second"`
	Results   float64       `json:"results_per_second"`
	Timeouts  float64       `json:"timeouts_per_second"`
	Rejected  float64       `json:"rejected_per_second"`
}
