package metaservice

import (
	"time"

	"github.com/asalimonov/gquorum/internal/operator"
)

// OperationControllerConfig binds scoped work and final validation to the
// service-owned controller.
func (g *Group) OperationControllerConfig() operator.Config {
	return operator.Config{Rules: g.rules, Coordinator: &operationCoordinator{g: g}, Authority: g,
		Groups: groupControllers{g}, OwnsGroup: func(string) bool { return true },
		Interval: g.opts.Timing.TickInterval, MaxRetryInterval: time.Second,
		ScopedPlanValidator: g.validateOperationScope, ScopedFinalValidator: g.validateOperationFinal}
}
