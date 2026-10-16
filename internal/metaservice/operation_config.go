package metaservice

import (
	"time"

	"github.com/asalimonov/gquorum/internal/operator"
)

// OperationControllerConfig prepares the existing controller ports. Task78 selects
// this configuration together with complete scoped persistence.
// There is no production switch between authorization models.
func (g *Group) OperationControllerConfig() operator.Config {
	return operator.Config{Rules: g.rules, Coordinator: &operationCoordinator{g: g}, Authority: g,
		Groups: groupControllers{g}, OwnsGroup: func(string) bool { return true },
		Interval: g.opts.Timing.TickInterval, MaxRetryInterval: time.Second,
		ScopedPlanValidator: g.validateOperationScope, ScopedFinalValidator: g.validateOperationFinal}
}
