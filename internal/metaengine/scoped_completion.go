package metaengine

import "github.com/asalimonov/gquorum/topology"

func validateScopedCompletion(state *scopedState, index *scopedIndexes, catalog authorityCatalog) error {
	for i := range state.Operations {
		operation := &state.Operations[i]
		for j, block := range operation.Plan.Blocks {
			for _, seq := range block.Steps {
				if scopedStepCompleted(operation.Status.Blocks[j], seq) &&
					!scopedStepConsumed(operation, operation.Plan.Steps[seq-1], index, catalog) {
					return invalidScopedState("completed required step lacks exact retained consumption proof")
				}
			}
		}
	}
	return nil
}

func scopedStepCompleted(block topology.BlockStatus, seq int) bool {
	return seq < block.CurrentStep || block.State == topology.OperationSucceeded && seq == block.CurrentStep
}

func scopedStepConsumed(operation *topology.Operation, step topology.Step,
	index *scopedIndexes, catalog authorityCatalog) bool {
	if step.Kind == topology.StepWaitCaughtUp {
		return true // Observational wait, as in the existing execution contract.
	}
	return scopedConsumedAuthority(operation, step, index, catalog) != nil
}

func scopedConsumedAuthority(operation *topology.Operation, step topology.Step,
	index *scopedIndexes, catalog authorityCatalog) *authorityRecord {
	// validateScopedRetry has already proved acyclic, contiguous attempts with
	// identical plans. Evidence keeps its original grant/action identity.
	for previous := operation; previous != nil; previous = index.operations[previous.Previous] {
		if catalog.planStepConsumed(previous.Status, step, retainedCompletion) {
			return catalog.planStepAuthority(previous.Status, step)
		}
	}
	return nil
}
