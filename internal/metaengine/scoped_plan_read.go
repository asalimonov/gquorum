package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) collectScopedPlanEvidence(read *PlanRead) error {
	op := &read.Operation
	ancestry, err := s.collectPlanAncestors(read)
	if err != nil {
		return err
	}

	read.Consumed = s.consumedPlanSteps(op)
	for _, record := range s.indexes.grants {
		grant := record.Grant
		if grant.Purpose != topology.AuthorityEffect || grant.Action == nil ||
			!ancestry[topology.OperationID(grant.OperatorID)] {
			continue
		}
		read.Grants = append(read.Grants, grant.Clone())
		for _, receipt := range record.Effects {
			read.Receipts = append(read.Receipts, receipt.Clone())
		}
	}
	slices.SortFunc(read.Grants, func(a, b topology.AuthorityGrant) int { return compareAuthorityKey(a.Key, b.Key) })
	slices.SortFunc(read.Receipts, func(a, b topology.EffectReceipt) int {
		if c := compareAuthorityKey(a.Key, b.Key); c != 0 {
			return c
		}
		if c := a.Resource.Compare(b.Resource); c != 0 {
			return c
		}
		return a.Target.Compare(b.Target)
	})
	return nil
}

func (s *scopedState) collectPlanAncestors(read *PlanRead) (map[topology.OperationID]bool, error) {
	op := &read.Operation
	ancestry := make(map[topology.OperationID]bool)
	for current := op; current != nil; current = s.indexes.operations[current.Previous] {
		if ancestry[current.Status.ID] {
			return nil, fault.ErrUnreadable
		}
		ancestry[current.Status.ID] = true
		if current != op {
			read.Previous = append(read.Previous, current.Clone())
		}
		if current.Previous != "" && s.indexes.operations[current.Previous] == nil {
			return nil, fault.ErrHistoryUnavailable
		}
	}
	return ancestry, nil
}

func (s *scopedState) consumedPlanSteps(op *topology.Operation) []int {
	var consumed []int
	catalog := s.authorityCatalog()
	for _, block := range op.Plan.Blocks {
		for _, seq := range block.Steps {
			if retryStepConsumed(op, op.Plan.Steps[seq-1], s.indexes, catalog) {
				consumed = append(consumed, seq)
			}
		}
	}
	return consumed
}
