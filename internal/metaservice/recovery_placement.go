package metaservice

import (
	"github.com/asalimonov/gquorum/internal/metaengine"
	recoveryplacement "github.com/asalimonov/gquorum/internal/recovery/placement"
)

func planRecoveryContext(read metaengine.PlanRead) (recoveryplacement.Context, error) {
	return recoveryplacement.FromImport(read.Recovery, read.Input.Approved)
}
