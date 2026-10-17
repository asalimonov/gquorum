package metaservice

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type planningGroupRead struct {
	native      topology.NativeState
	observation topology.GroupStatus
}

func (g *Group) readPlanningGroup(ctx context.Context, input topology.PlanInput,
	desired topology.Document, id string) (planningGroupRead, error) {
	controllers := groupControllers{g}
	state, err := controllers.ReadNative(ctx, id, planNativeCandidates(input, desired, id)...)
	if err != nil {
		if freshPlanningGroup(input, id) && (errors.Is(err, fault.ErrUnavailable) ||
			errors.Is(err, fault.ErrReplicaNotReady)) {
			return planningGroupRead{observation: topology.GroupStatus{GroupID: id}}, nil
		}
		return planningGroupRead{}, err
	}
	if state.GroupID != id || state.Applied == 0 || state.ReplicaID == 0 {
		return planningGroupRead{}, fault.ErrIdentityMismatch
	}
	status, err := controllers.Status(ctx, id)
	if err != nil {
		return planningGroupRead{}, err
	}
	if status.GroupID != id {
		return planningGroupRead{}, fault.ErrIdentityMismatch
	}
	return planningGroupRead{native: state.Clone(), observation: status.Clone()}, nil
}

func freshPlanningGroup(input topology.PlanInput, id string) bool {
	row, retained := input.Groups[id], input.Retained[id]
	return row.Converged == nil && row.ConvergedVersion == 0 &&
		!retained.FoundingIssued && len(retained.Replicas) == 0
}
