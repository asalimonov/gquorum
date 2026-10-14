package topology

import (
	"time"

	"github.com/asalimonov/gquorum/membership"
)

const (
	defaultTargetVoters               = 3
	defaultFailureWindow              = 30 * time.Second
	defaultFailureLagEntries          = 1000
	defaultStandbyLagEntries          = 100
	defaultStandbyApplyBacklogEntries = 100
	defaultNoProgressWindow           = 30 * time.Second
	defaultMaxObservationGap          = 15 * time.Second
	defaultSwitchCooldown             = 5 * time.Minute
	defaultDemotionCooldown           = 5 * time.Minute
)

// VoterConfig is replicated per-group voting intent. It controls voter seats,
// not the number of hosted replicas. A nil Group.VoterConfig preserves legacy
// behavior; decoding stored topology never installs defaults.
//
// Explicit values must be complete. Start with DefaultVoterConfig and override
// fields to retain meaningful zero values (such as no surge or strict
// catch-up).
type VoterConfig struct {
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
	DistinctVoterLabel            string        `json:"distinct_voter_label,omitempty"`
}

// DefaultVoterConfig returns a complete configuration for a freshly created
// group.
func DefaultVoterConfig() VoterConfig {
	return VoterConfig{
		TargetVoters: defaultTargetVoters, MaxSurgeVoters: 1,
		FailureWindow: defaultFailureWindow, StandbyWindow: time.Minute,
		FailureLagEntries: defaultFailureLagEntries, StandbyLagEntries: defaultStandbyLagEntries,
		StandbyApplyBacklogEntries: defaultStandbyApplyBacklogEntries,
		ReplaceNoProgress:          true, NoProgressWindow: defaultNoProgressWindow,
		MaxObservationGap: defaultMaxObservationGap,
		SwitchCooldown:    defaultSwitchCooldown, DemotionCooldown: defaultDemotionCooldown,
		AllowNewFailureDuringCooldown: true,
	}
}

// Validate checks configuration against hard voter bounds, independently of
// currently
// available hosts. A temporary target shortfall does not lower desired quorum.
func (p VoterConfig) Validate(minVoters, maxVoters int) error {
	if p.DistinctVoterLabel != "" {
		if err := validateLabelKey(p.DistinctVoterLabel, GroupLabels); err != nil {
			return err
		}
	}
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
