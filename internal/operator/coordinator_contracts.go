package operator

import (
	"context"

	"github.com/asalimonov/gquorum/topology"
)

// Coordinator supplies leadership, current execution and replicated step
// transitions.
type Coordinator interface {
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
