package metaservice

import (
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/topology"
)

func (g *Group) validateOperationScope(ctx context.Context, operation topology.Operation, group string) error {
	read, err := g.captureOperationScope(ctx, operation, group)
	if err != nil {
		return err
	}
	return g.validateOperationRead(read, group)
}

func (g *Group) captureOperationScope(ctx context.Context, operation topology.Operation,
	group string) (metaengine.PlanRead, error) {
	if !g.IsLeader() {
		return metaengine.PlanRead{}, fault.ErrNotLeader
	}
	request := metaengine.QueryRequest{Kind: "operation_plan", OperationID: operation.Status.ID, Key: group}
	read, err := g.capturePlan(ctx, request, g.readPlan)
	if err != nil {
		return metaengine.PlanRead{}, err
	}
	if !g.IsLeader() {
		return metaengine.PlanRead{}, fault.ErrNotLeader
	}
	return read, sameOperationScope(operation, read.Operation, group)
}

func sameOperationScope(expected, actual topology.Operation, group string) error {
	if actual.Status.BlocksTruncated || expected.Previous != actual.Previous ||
		expected.Status.ID != actual.Status.ID || expected.Status.Version != actual.Status.Version ||
		expected.Status.Attempt != actual.Status.Attempt || !expected.Plan.Equal(actual.Plan) {
		return fault.ErrOperationAuthorization
	}
	position := slices.IndexFunc(expected.Plan.Blocks, func(block topology.PlanBlock) bool {
		return block.GroupID == group
	})
	if position < 0 || position >= len(expected.Status.Blocks) || position >= len(actual.Status.Blocks) {
		return fault.ErrOperationAuthorization
	}
	a, b := expected.Status.Blocks[position], actual.Status.Blocks[position]
	if a.GroupID != group || b.GroupID != group || a.State.Terminal() != b.State.Terminal() ||
		a.State != b.State && a.State.Terminal() {
		return fault.ErrOperationAuthorization
	}
	steps := expected.Plan.Blocks[position].Steps
	if len(steps) != 0 {
		if a.CurrentStep == 0 {
			a.CurrentStep = steps[0]
		}
		if b.CurrentStep == 0 {
			b.CurrentStep = steps[0]
		}
	}
	if a.CurrentStep != b.CurrentStep {
		return fault.ErrOperationAuthorization
	}
	return nil
}

func (g *Group) validateOperationRead(read metaengine.PlanRead, group string) error {
	view := operator.RemainingPlanView{RecoveryFoundingSeeds: read.RecoveryFoundingSeeds,
		Input: read.Input, Target: read.Target, Previous: read.Previous,
		Grants: read.Grants, Receipts: read.Receipts, Consumed: read.Consumed}
	if err := operator.ValidateRemainingScope(view, read.Operation, group, g.rules); err != nil {
		return err
	}
	if group == topology.MetaGroupID {
		_, err := topology.ValidateMetaDrainSafety(read.Input, read.Target, operationDrainInput(read))
		return err
	}
	return nil
}

func (g *Group) validateOperationFinal(ctx context.Context, operation topology.Operation, group string) error {
	read, err := g.captureOperationScope(ctx, operation, group)
	if err != nil {
		return err
	}
	if err := g.validateOperationRead(read, group); err != nil {
		return err
	}
	if group == topology.MetaGroupID {
		input := operationDrainInput(read)
		input.Final = true
		_, err := topology.ValidateMetaDrainSafety(read.Input, read.Target, input)
		return err
	}
	return nil
}
