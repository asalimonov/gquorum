package metaservice

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// planningInput rechecks retained metadata after native reads. Observations are
// separate owned samples; their cached membership never establishes a voter set.
func (g *Group) planningInput(ctx context.Context, desired topology.Document) (topology.PlanInput, error) {
	input, err := g.state().PlanningInput(ctx, desired, nil)
	if err != nil {
		return topology.PlanInput{}, err
	}
	observed := make(map[string]topology.GroupStatus)
	for _, group := range desired.Groups {
		read, err := g.readPlanningGroup(ctx, input, desired, group.ID)
		if err != nil {
			return topology.PlanInput{}, err
		}
		if read.native.Applied != 0 {
			input.Native[group.ID] = read.native
		}
		observed[group.ID] = read.observation
	}
	fresh, err := g.state().PlanningInput(ctx, desired, input.Native)
	if err != nil {
		return topology.PlanInput{}, err
	}
	if fresh.Approved.Version != input.Approved.Version {
		return topology.PlanInput{}, fault.ErrVersionConflict
	}
	for id, status := range observed {
		row := fresh.Groups[id]
		owned := status.Clone()
		row.GroupID, row.Observation = id, &owned
		fresh.Groups[id] = row
	}
	return fresh, nil
}

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
