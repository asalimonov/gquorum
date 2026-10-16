package operator

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/topology"
)

type scopedCoordinatorTest struct {
	CleanupCoordinator
	leader      bool
	work        []WorkItem
	onSelect    func()
	transitions []topology.OperationTransition
}

func (c *scopedCoordinatorTest) IsMetaLeader() bool { return c.leader }
func (c *scopedCoordinatorTest) ReadyBlocks(_ context.Context, limit int) ([]WorkItem, error) {
	if c.onSelect != nil {
		c.onSelect()
	}
	return c.work[:min(limit, len(c.work))], nil
}
func (c *scopedCoordinatorTest) RecordOperationTransition(ctx context.Context, t topology.OperationTransition) error {
	if _, bounded := ctx.Deadline(); !bounded {
		return errors.New("unbounded transition")
	}
	c.transitions = append(c.transitions, t)
	return nil
}

type forbiddenLegacyCoordinator struct{ LegacyCoordinator }
type forbiddenLegacyStore struct{ Store }
type forbiddenScopedEffects struct{ GroupEffects }
type forbiddenScopedAuthority struct{ AuthorityCoordinator }

func scopedControllerConfig(c Coordinator) Config {
	return Config{Rules: Rules{SchemaVersion: 1, Limits: (Limits{}).WithDefaults()}, Coordinator: c,
		Groups: forbiddenScopedEffects{}, Authority: forbiddenScopedAuthority{}, OwnsGroup: func(string) bool { return true },
		ScopedPlanValidator:  func(context.Context, topology.Operation, string) error { return nil },
		ScopedFinalValidator: func(context.Context, topology.Operation, string) error { return nil }}
}
