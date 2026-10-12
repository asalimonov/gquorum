package topology

import (
	"time"

	"github.com/asalimonov/gquorum/membership"
)

// VoterPolicy is replicated per-group voting intent. It controls voter seats,
// not the number of hosted replicas. A nil Group.VoterPolicy preserves legacy
// behavior; decoding stored topology never installs defaults.
//
// Explicit values must be complete. Start with DefaultVoterPolicy and override
// fields to retain meaningful zero values (such as no surge or strict catch-up).
type VoterPolicy struct {
	TargetVoters                  int           `json:"target_voters"`
	MaxSurgeVoters                int           `json:"max_surge_voters"`
	FailureWindow                 time.Duration `json:"failure_window_ns"`
	StandbyWindow                 time.Duration `json:"standby_window_ns"`
	FailureLagEntries             uint64        `json:"failure_lag_entries"`
	StandbyLagEntries             uint64        `json:"standby_lag_entries"`
	StandbyApplyBacklogEntries    uint64        `json:"standby_apply_backlog_entries"`
	ReplaceNoProgress             bool          `json:"replace_no_progress"`
	NoProgressWindow              time.Duration `json:"no_progress_window_ns"`
	MaxObservationGap             time.Duration `json:"max_observation_gap_ns"`
	SwitchCooldown                time.Duration `json:"switch_cooldown_ns"`
	DemotionCooldown              time.Duration `json:"demotion_cooldown_ns"`
	AllowNewFailureDuringCooldown bool          `json:"allow_new_failure_during_cooldown"`
}

// DefaultVoterPolicy returns a complete policy for a freshly created group.
func DefaultVoterPolicy() VoterPolicy {
	return VoterPolicy{
		TargetVoters: 3, MaxSurgeVoters: 1,
		FailureWindow: 30 * time.Second, StandbyWindow: time.Minute,
		FailureLagEntries: 1000, StandbyLagEntries: 100, StandbyApplyBacklogEntries: 100,
		ReplaceNoProgress: true, NoProgressWindow: 30 * time.Second,
		MaxObservationGap: 15 * time.Second,
		SwitchCooldown:    5 * time.Minute, DemotionCooldown: 5 * time.Minute,
		AllowNewFailureDuringCooldown: true,
	}
}

// Validate checks policy against hard voter bounds, independently of currently
// available hosts. A temporary target shortfall does not lower desired quorum.
func (p VoterPolicy) Validate(minVoters, maxVoters int) error {
	if minVoters < 1 || maxVoters < minVoters || p.TargetVoters < minVoters || p.TargetVoters > maxVoters {
		return invalid("voter target outside placement bounds")
	}
	if p.MaxSurgeVoters < 0 || p.MaxSurgeVoters > 1 {
		return invalid("voter surge must be zero or one")
	}
	if p.FailureWindow <= 0 || p.StandbyWindow <= 0 || p.MaxObservationGap <= 0 {
		return invalid("voter failure, standby and observation windows must be positive")
	}
	if p.NoProgressWindow < 0 || p.ReplaceNoProgress && p.NoProgressWindow == 0 {
		return invalid("enabled no-progress detection requires a positive window")
	}
	if p.SwitchCooldown < 0 || p.DemotionCooldown < 0 {
		return invalid("voter cooldown cannot be negative")
	}
	if p.StandbyLagEntries > membership.PromotionLagLimit || p.FailureLagEntries <= p.StandbyLagEntries {
		return invalid("voter lag thresholds must preserve promotion safety and hysteresis")
	}
	return nil
}
