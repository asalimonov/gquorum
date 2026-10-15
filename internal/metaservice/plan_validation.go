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
	readControlErr := g.readControl(ctx, metaengine.QueryRequest{Kind: "plan_validation",
		OperatorID: ex.ID}, &view)
	if readControlErr != nil {
		return readControlErr
	}
	status := view.Operator.Status
	if status.ID != ex.ID || status.Version != ex.Version || status.Attempt != ex.Attempt ||
		status.CurrentStep != ex.CurrentStep || !view.Operator.Plan.Equal(ex.Plan()) {
		return fault.ErrOperatorAuthorization
	}
	ex.Grants, ex.Receipts = view.Grants, view.Receipts
	native := make(map[string]topology.NativeState, len(view.Target.Groups))
	controllers := groupControllers{g}
	for _, group := range view.Target.Groups {
		candidates := planNativeCandidates(view.Input, view.Target, group.ID)
		state, err := controllers.ReadNative(ctx, group.ID, candidates...)
		if err != nil {
			founding := slices.ContainsFunc(ex.Steps, func(s topology.Step) bool {
				return s.Kind == topology.StepCreateFoundingGroup && s.GroupID == group.ID &&
					s.Seq >= max(ex.CurrentStep, 1)
			})
			// A new or partially started founding group may not yet have
			// quorum. Inherited group fences were checked by the same safe
			// metadata read above. Its immutable fan-out is the only action
			// allowed to establish that quorum.
			row := view.Input.Groups[group.ID]
			if founding && row.Converged == nil && row.ConvergedVersion == 0 && (errors.Is(err, fault.ErrUnavailable) ||
				errors.Is(err, fault.ErrReplicaNotReady) || errors.Is(err, context.DeadlineExceeded)) {
				continue
			}
			return err
		}
		native[group.ID] = state
	}
	documents := operator.RemainingPlanView{Input: view.Input, Target: view.Target}
	return operator.ValidateRemainingPlan(documents, ex, native, g.rules)
}

func planNativeCandidates(input topology.PlanInput, target topology.Document, id string) []storage.Identity {
	var candidates []storage.Identity
	seen := make(map[storage.Identity]bool)
	add := func(node string, replica uint64) {
		identity := storage.Identity{ClusterID: target.ClusterID, GroupID: id, NodeName: node, ReplicaID: replica}
		if !seen[identity] {
			seen[identity] = true
			candidates = append(candidates, identity)
		}
	}
	for _, group := range []*topology.Group{input.Groups[id].Converged, target.Group(id)} {
		if group != nil {
			for _, replica := range group.Replicas {
				add(replica.NodeName, replica.ReplicaID)
			}
		}
	}
	for _, replica := range input.Retained[id].Replicas {
		add(replica.NodeName, replica.ReplicaID)
	}
	return candidates
}
