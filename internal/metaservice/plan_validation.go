package metaservice

import (
	"context"
	"errors"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func (g *Group) validateRemainingPlan(ctx context.Context, ex operator.Record) error {
	var view metaengine.PlanRead
	if err := g.readControl(ctx, metaengine.QueryRequest{Kind: "plan_validation", OperatorID: ex.ID}, &view); err != nil {
		return err
	}
	status := view.Operator.Status
	if status.ID != ex.ID || status.Version != ex.Version || status.Attempt != ex.Attempt || status.CurrentStep != ex.CurrentStep || !slices.EqualFunc(view.Operator.Plan.Steps, ex.Steps, topology.Step.Equal) {
		return fault.ErrOperatorAuthorization
	}
	native := make(map[string]topology.NativeState, len(view.Target.Groups))
	controllers := groupControllers{g}
	for _, group := range view.Target.Groups {
		var candidates []storage.Identity
		for _, doc := range []topology.Document{view.Base, view.Target} {
			if retained := doc.Group(group.ID); retained != nil {
				for _, r := range retained.Replicas {
					candidates = append(candidates, storage.Identity{ClusterID: doc.ClusterID, GroupID: group.ID, NodeName: r.NodeName, ReplicaID: r.ReplicaID})
				}
			}
		}
		state, err := controllers.ReadNative(ctx, group.ID, candidates...)
		if err != nil {
			founding := slices.ContainsFunc(ex.Steps, func(s topology.Step) bool {
				return s.Seq >= max(ex.CurrentStep, 1) && s.Kind == topology.StepCreateFoundingGroup && s.GroupID == group.ID
			})
			// A new or partially started founding group may not yet have quorum.
			// Inherited group fences were checked by the same safe metadata read above.
			// Its immutable fan-out is the only action allowed to establish that quorum.
			if founding && view.Base.Group(group.ID) == nil && (errors.Is(err, fault.ErrUnavailable) || errors.Is(err, fault.ErrReplicaNotReady) || errors.Is(err, context.DeadlineExceeded)) {
				continue
			}
			return err
		}
		native[group.ID] = state
	}
	return operator.ValidateRemainingPlan(view.Base, view.Target, ex, native, g.rules)
}
