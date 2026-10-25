package metaservice

import (
	"context"
	"encoding/json"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/internal/recoveryplacement"
	"github.com/asalimonov/gquorum/topology"
)

// Plan previews safe convergence to a retained topology version without
// authorizing effects.
func (g *Group) Plan(ctx context.Context, version uint64) (topology.Plan, error) {
	doc, err := g.Get(ctx, version)
	if err != nil {
		return topology.Plan{}, err
	}
	return g.PlanFor(ctx, doc)
}

// PlanFor validates desired topology and builds a plan against observed native
// group state.
func (g *Group) PlanFor(ctx context.Context, doc topology.Document) (topology.Plan, error) {
	if err := g.ensureRunning(ctx); err != nil {
		return topology.Plan{}, err
	}
	if doc.ClusterID != g.currentIdentity().ClusterID {
		return topology.Plan{}, fault.ErrIdentityMismatch
	}
	if err := doc.Validate(g.rules); err != nil {
		return topology.Plan{}, err
	}
	read, err := g.capturePlan(ctx, metaengine.QueryRequest{Kind: "planning", Desired: &doc}, g.readPlan)
	if err != nil {
		return topology.Plan{}, err
	}
	approved := read.Input.Approved
	if approved.ClusterID == "" {
		approved.ClusterID = doc.ClusterID
	}
	placementCtx, err := planRecoveryContext(read)
	if err != nil {
		return topology.Plan{}, err
	}
	if err := recoveryplacement.Validate(placementCtx, approved, approved, doc, g.rules); err != nil {
		return topology.Plan{}, err
	}
	plan, err := g.planOperation(read)
	return plan, explainUnsafePlacementPlan(approved, approved, doc, err)
}

func (g *Group) callLeader(ctx context.Context, method string, request any) ([]byte, error) {
	engine := g.engine.Load()
	if engine == nil || engine.RaftStatus().Lead == 0 {
		return nil, fault.ErrUnavailable
	}
	data, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	address, err := g.resolveMember(ctx, engine.RaftStatus().Lead)
	if err != nil {
		return nil, err
	}
	reply, err := g.transport.CallAddress(ctx, address, method, data)
	if err != nil {
		return nil, err
	}
	if err = metatransport.ReplyError(reply); err != nil {
		return nil, err
	}
	return reply.Data, nil
}
