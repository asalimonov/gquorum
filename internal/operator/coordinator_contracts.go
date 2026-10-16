package operator

import (
	"context"

	"github.com/asalimonov/gquorum/topology"
)

// LegacyCoordinator preserves schema3 execution until the complete Task78 cutover.
// It is not a scoped work source and must not be used by the new dispatcher.
type LegacyCoordinator interface {
	IsMetaLeader() bool
	CurrentOperator(context.Context) (Record, bool)
	RecordOperatorTransition(context.Context, Transition) error
}

// AuthorityCoordinator issues exact grants and reconciles fences and effect
// receipts.
type AuthorityCoordinator interface {
	BeginEffect(context.Context, topology.AuthorizedStep) (topology.AuthorityGrant, error)
	ReconcileFences(context.Context) error
	EffectReceipts(context.Context, topology.AuthorityGrant) ([]topology.EffectReceipt, error)
	RecordEffect(context.Context, topology.EffectReceipt) error
}

// MembershipCoordinator applies granted membership effects and checks learner
// catch-up.
type MembershipCoordinator interface {
	Apply(context.Context, topology.AuthorityGrant) ([]topology.EffectReceipt, error)
	CaughtUp(GroupStatus, uint64) bool
}
