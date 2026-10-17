package operator

import (
	"context"

	"github.com/asalimonov/gquorum/topology"
)

// AuthorityCoordinator issues exact grants and records durable effect receipts.
type AuthorityCoordinator interface {
	BeginEffect(context.Context, topology.AuthorizedStep) (topology.AuthorityGrant, error)
	EffectReceipts(context.Context, topology.AuthorityGrant) ([]topology.EffectReceipt, error)
	RecordEffect(context.Context, topology.EffectReceipt) error
}
