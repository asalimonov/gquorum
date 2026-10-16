package metaservice

import (
	"context"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// BeginCleanupEffect requests one exact retained host obligation. The full
// action avoids resolving a sequence against an unrelated/current operation.
// Encoding becomes available with the complete scoped schema at Task 78.
func (g *Group) BeginCleanupEffect(ctx context.Context, id storage.Identity,
	action topology.AuthorizedStep) (topology.AuthorityGrant, error) {
	result, err := g.propose(ctx, metaengine.BeginCleanupEffectProposal{Identity: id, Action: action})
	if err != nil {
		return topology.AuthorityGrant{}, err
	}
	return g.issuedEffect(ctx, result, action)
}
