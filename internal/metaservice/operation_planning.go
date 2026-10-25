package metaservice

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	recoveryplacement "github.com/asalimonov/gquorum/internal/recovery/placement"
	"github.com/asalimonov/gquorum/topology"
)

func (g *Group) readPlan(ctx context.Context, request metaengine.QueryRequest) (metaengine.PlanRead, error) {
	var read metaengine.PlanRead
	err := g.readControl(ctx, request, &read)
	return read, err
}

// Native observations never authorize effects. Capture again after I/O; a
// change outside these scopes must not become a whole-document progress gate.
func (g *Group) capturePlan(ctx context.Context, request metaengine.QueryRequest,
	read func(context.Context, metaengine.QueryRequest) (metaengine.PlanRead, error)) (metaengine.PlanRead, error) {
	before, err := read(ctx, request)
	if err != nil || before.Accepted != "" {
		return before, err
	}
	observed := make(map[string]topology.GroupStatus)
	request.Native = make(map[string]topology.NativeState)
	for _, scope := range before.Scopes {
		if before.Target.Group(scope.GroupID) == nil {
			continue
		}
		sample, err := g.readOperationGroup(ctx, before, scope.GroupID)
		if err != nil {
			return metaengine.PlanRead{}, err
		}
		if sample.native.Applied != 0 {
			request.Native[scope.GroupID] = sample.native
		}
		observed[scope.GroupID] = sample.observation
	}
	after, err := read(ctx, request)
	if err != nil || after.Accepted != "" {
		return after, err
	}
	if err := comparePlanCapture(before, after); err != nil {
		return metaengine.PlanRead{}, fmt.Errorf("recapture plan scopes %v -> %v: %w", before.Scopes, after.Scopes, err)
	}
	for id, status := range observed {
		row := after.Input.Groups[id]
		owned := status.Clone()
		owned.LeaderNodeName = progressLeaderName(row, owned.LeaderID)
		row.GroupID, row.Observation = id, &owned
		after.Input.Groups[id] = row
	}
	return after, nil
}

func comparePlanCapture(before, after metaengine.PlanRead) error {
	if !slices.Equal(before.Scopes, after.Scopes) ||
		!before.Operation.Plan.Equal(after.Operation.Plan) {
		return fault.ErrVersionConflict
	}
	changed := topology.AffectedGroups(before.Input.Approved, after.Input.Approved)
	changed = append(changed, topology.AffectedGroups(before.Target, after.Target)...)
	for _, scope := range before.Scopes {
		if slices.Contains(changed, scope.GroupID) {
			return fault.ErrVersionConflict
		}
		if before.Operation.Status.ID != "" {
			if err := sameOperationScope(before.Operation, after.Operation, scope.GroupID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *Group) readOperationGroup(ctx context.Context, read metaengine.PlanRead,
	group string) (planningGroupRead, error) {
	sample, err := g.readPlanningGroup(ctx, read.Input, read.Target, group)
	if err == nil {
		return sample, nil
	}
	if !errors.Is(err, fault.ErrUnavailable) && !errors.Is(err, fault.ErrReplicaNotReady) &&
		!errors.Is(err, context.DeadlineExceeded) {
		return planningGroupRead{}, err
	}
	row := read.Input.Groups[group]
	founding := slices.ContainsFunc(read.Operation.Plan.Steps, func(step topology.Step) bool {
		return step.GroupID == group && step.Kind == topology.StepCreateFoundingGroup &&
			!slices.Contains(read.Consumed, step.Seq)
	})
	if founding && row.Converged == nil && row.ConvergedVersion == 0 {
		return planningGroupRead{observation: topology.GroupStatus{GroupID: group}}, nil
	}
	return planningGroupRead{}, err
}

func (g *Group) planOperation(read metaengine.PlanRead) (topology.Plan, error) {
	var preferred uint64
	hasMeta := slices.ContainsFunc(read.Scopes, func(scope metaengine.ScopeExpectation) bool {
		return scope.GroupID == topology.MetaGroupID
	})
	if hasMeta {
		var err error
		preferred, err = topology.ValidateMetaDrainSafety(read.Input, read.Target, operationDrainInput(read))
		if err != nil {
			return topology.Plan{}, err
		}
	}
	placementCtx, err := planRecoveryContext(read)
	if err != nil {
		return topology.Plan{}, err
	}
	return recoveryplacement.BuildPlan(placementCtx, read.Input, read.Target,
		topology.PlanOptions{Rules: g.rules, PreferredMetaTransfer: preferred})
}

func operationDrainInput(read metaengine.PlanRead) topology.MetaDrainInput {
	status := read.Input.Groups[topology.MetaGroupID].Observation.Clone()
	native, found := read.Input.Native[topology.MetaGroupID]
	status.Found, status.ConfState = found, native.Clone().ConfState
	health := make(map[string]topology.Health)
	for _, identity := range planNativeCandidates(read.Input, read.Target, topology.MetaGroupID) {
		if status.LiveVoters[identity.ReplicaID] {
			health[identity.NodeName] = topology.HealthAlive
		} else if health[identity.NodeName] == "" {
			health[identity.NodeName] = topology.HealthDead
		}
	}
	return topology.MetaDrainInput{Status: status, Health: health}
}
