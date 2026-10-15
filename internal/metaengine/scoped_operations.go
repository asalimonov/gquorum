package metaengine

import (
	"reflect"
	"slices"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func validateScopedOperations(state *scopedState, index *scopedIndexes) error {
	for _, operation := range state.Operations {
		status := operation.Status
		if status.BlocksTruncated || status.Version == 0 || status.Version > state.Target.Version ||
			status.Attempt == 0 ||
			operation.Plan.Version != status.Version || index.documents[status.Version].Version == 0 ||
			status.TotalBlocks != len(operation.Plan.Blocks) || len(status.Blocks) != status.TotalBlocks ||
			status.ID != topology.MakeOperationID(status.Version, status.Attempt) || !status.State.Valid() ||
			operation.StartedAt.IsZero() || operation.UpdatedAt.Before(operation.StartedAt) {
			return invalidScopedState("invalid complete operation record")
		}
		if err := validateScopedPlan(operation.Plan, index); err != nil {
			return err
		}
		if err := validateScopedBlocks(operation); err != nil {
			return err
		}
		if err := validateScopedRetry(operation, index); err != nil {
			return err
		}
	}
	return nil
}

func validateScopedPlan(plan topology.Plan, index *scopedIndexes) error {
	if len(plan.Steps) != 0 && len(plan.Blocks) == 0 {
		return invalidScopedState("scoped plan lacks required partitions")
	}
	if err := topology.ValidatePlanGuardChain(plan); err != nil {
		return fault.New(fault.Unreadable, "invalid immutable plan graph", err)
	}
	for i, step := range plan.Steps {
		if err := validateScopedStep(step, i); err != nil {
			return err
		}
	}
	for _, block := range plan.Blocks {
		if block.SourceVersion != 0 && index.documents[block.SourceVersion].Version == 0 {
			return invalidScopedState("retained operation lacks its planning source")
		}
	}
	return nil
}

func validateScopedStep(step topology.Step, position int) error {
	if err := validatePlanStepPosition(step, position); err != nil {
		return fault.New(fault.Unreadable, "invalid immutable step", err)
	}
	if err := validatePlanStepScope(step); err != nil {
		return fault.New(fault.Unreadable, "invalid immutable step scope", err)
	}
	wholeGroup := step.Kind == topology.StepCreateFoundingGroup || step.Kind == topology.StepDeleteGroupState
	if !wholeGroup && (step.ReplicaID == 0 || strings.TrimSpace(step.NodeName) == "" ||
		strings.ContainsRune(step.NodeName, 0)) {
		return invalidScopedState("replica step lacks its exact host identity")
	}
	return nil
}

func validateScopedBlocks(operation topology.Operation) error {
	terminal, failed, superseded := true, false, false
	for i, block := range operation.Status.Blocks {
		if err := validateScopedBlock(block, operation.Plan.Blocks[i]); err != nil {
			return err
		}
		terminal = terminal && block.State.Terminal()
		failed = failed || block.State == topology.OperationFailed
		superseded = superseded || block.State == topology.OperationSuperseded
	}
	if terminal != operation.Status.State.Terminal() {
		return invalidScopedState("aggregate operation state contradicts required blocks")
	}
	want := topology.OperationSucceeded
	if superseded {
		want = topology.OperationSuperseded
	}
	if failed {
		want = topology.OperationFailed
	}
	if terminal && operation.Status.State != want {
		return invalidScopedState("aggregate terminal outcome omits failed or superseded blocks")
	}
	return nil
}

func validateScopedBlock(block topology.BlockStatus, definition topology.PlanBlock) error {
	if block.GroupID != definition.GroupID || block.TotalSteps != len(definition.Steps) || !block.State.Valid() ||
		block.CurrentStep != 0 && !slices.Contains(definition.Steps, block.CurrentStep) {
		return invalidScopedState("invalid required block cursor or identity")
	}
	last := 0
	if len(definition.Steps) != 0 {
		last = definition.Steps[len(definition.Steps)-1]
	}
	if block.State == topology.OperationSucceeded && block.CurrentStep != last {
		return invalidScopedState("successful block has incomplete required steps")
	}
	return nil
}

func validateScopedRetry(operation topology.Operation, index *scopedIndexes) error {
	if operation.Status.Attempt == 1 {
		if operation.Previous != "" {
			return invalidScopedState("first operation attempt has a predecessor")
		}
		return nil
	}
	previous := index.operations[operation.Previous]
	if previous == nil {
		return invalidScopedState("missing retry predecessor")
	}
	if previous.Status.State != topology.OperationFailed ||
		previous.Status.Version != operation.Status.Version || previous.Status.Attempt >= operation.Status.Attempt ||
		operation.Status.Attempt-previous.Status.Attempt != 1 ||
		len(previous.Status.Blocks) != len(operation.Status.Blocks) || operation.Actor != previous.Actor ||
		operation.Origin != previous.Origin || operation.StartedAt.Before(previous.UpdatedAt) ||
		!previous.Plan.Equal(operation.Plan) {
		return invalidScopedState("invalid immutable retry predecessor")
	}
	for i, block := range previous.Status.Blocks {
		if block.State != topology.OperationFailed && !reflect.DeepEqual(block, operation.Status.Blocks[i]) {
			return invalidScopedState("retry changed a terminal sibling's retained progress")
		}
	}
	return nil
}
