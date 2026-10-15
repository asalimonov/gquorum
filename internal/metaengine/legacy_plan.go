package metaengine

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Remove at Task 62's scoped codec/dispatch activation, not merely when the new
// record type exists. Legacy records cannot preserve partitions or barriers.
func validateLegacyPlan(plan topology.Plan) error {
	if len(plan.Blocks) != 0 || len(plan.Dependencies) != 0 {
		return fault.New(fault.Unsupported, "scoped plans require the scoped metadata format", nil)
	}
	return nil
}
