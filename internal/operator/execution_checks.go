package operator

import (
	"context"

	"github.com/asalimonov/gquorum/topology"
)

func (e *Controller) effectComplete(ctx context.Context, grant topology.AuthorityGrant) (bool, error) {
	recorded, err := e.authority.EffectReceipts(ctx, grant)
	if err != nil {
		return false, err
	}
	return receiptSetComplete(grant, recorded)
}
