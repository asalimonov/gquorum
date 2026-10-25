package metaservice

import (
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/recoveryplacement"
)

func planRecoveryContext(read metaengine.PlanRead) (recoveryplacement.Context, error) {
	return recoveryplacement.FromImport(read.Recovery, read.Input.Approved)
}
