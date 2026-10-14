package scheduling

import "time"

// Options selects repair decisions and configures the node's reconciliation
// loop.
// A custom Policy and nonzero DefaultRepair settings are mutually exclusive.
type Options struct {
	Policy        RepairPolicy
	DefaultRepair DefaultRepairOptions
	Disabled      bool
	Interval      time.Duration
	Timeout       time.Duration
	Cooldown      time.Duration
}
