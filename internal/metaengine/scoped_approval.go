package metaengine

import (
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type approvalKind uint8

const (
	explicitApproval approvalKind = iota
	automaticApproval
)

// Prepare only. Admission, debt and revocation preparation must all succeed
// before this candidate can replace live state. No application callbacks or I/O.
func prepareScopedApproval(s *scopedState, desired topology.Document,
	p BeginOperationProposal, kind approvalKind) (*scopedState, error) {
	if err := s.validateApprovalCaptures(desired, p, kind); err != nil {
		return nil, err
	}
	if err := topology.ValidateHostingCapacityFor(s.Target, desired, s.Rules); err != nil {
		return nil, err
	}
	if err := topology.ValidatePlacementTransition(s.Target, s.Target, desired); err != nil {
		return nil, err
	}
	collector := s.planningCollector()
	input, err := collector.collect(s.planningDeclarations(), desired)
	if err != nil {
		return nil, err
	}
	shape := p.Plan
	if shape.Blocks == nil {
		shape.Blocks = []topology.PlanBlock{} // Explicitly empty scoped coverage.
	}
	if err := topology.ValidatePlanShape(input, desired, shape); err != nil {
		return nil, err
	}
	if err := validateNewRetirement(collector, input, desired, p.Plan); err != nil {
		return nil, err
	}
	next := s.prepareApprovalOwners(desired, p)
	if err := prepareAdmissionHandovers(s, next); err != nil {
		return nil, err
	}
	if err := prepareCleanupHandovers(s, next, collector); err != nil {
		return nil, err
	}
	if err := prepareDeletionDebts(s, next, collector); err != nil {
		return nil, err
	}
	if err := prepareDeletionCompletions(s, next, collector); err != nil {
		return nil, err
	}
	if err := prepareApprovalCompletion(s, next); err != nil {
		return nil, err
	}
	return next, nil
}

func (s *scopedState) validateApprovalCaptures(desired topology.Document,
	p BeginOperationProposal, kind approvalKind) error {
	if s.indexes == nil || kind != explicitApproval && kind != automaticApproval {
		return fault.ErrOperationAuthorization
	}
	if p.Version == 0 || p.Version != desired.Version || p.Plan.Version != p.Version ||
		p.Version <= s.Target.Version || s.indexes.first[p.Version] != "" || p.At.IsZero() {
		return fault.ErrOperationAuthorization
	}
	if err := validateScopeGroups(p.Scopes, topology.AffectedGroups(s.Target, desired)); err != nil {
		return err
	}
	if err := validatePlanScopes(p.Plan, p.Scopes); err != nil {
		return err
	}
	if len(p.Plan.Steps) != 0 && len(p.Plan.Blocks) == 0 {
		return fault.ErrOperationAuthorization
	}
	for _, scope := range p.Scopes {
		if err := s.validateApprovalScope(scope, p.At, kind); err != nil {
			return err
		}
	}
	return topology.ValidatePlanGuardChain(p.Plan)
}

func (s *scopedState) validateApprovalScope(scope ScopeExpectation, at time.Time, kind approvalKind) error {
	if !scope.matches(s.indexes.groups[scope.GroupID]) {
		return fault.ErrVersionConflict
	}
	if row := s.indexes.groups[scope.GroupID]; row != nil && at.Before(row.CompletedAt) {
		return fault.ErrOperationAuthorization
	}
	if owner := s.indexes.operations[scope.Owner]; scope.Owner != "" &&
		(kind != explicitApproval || owner == nil || at.Before(owner.UpdatedAt)) {
		return fault.ErrOperationAuthorization
	}
	return nil
}

func (s *scopedState) prepareApprovalOwners(desired topology.Document, p BeginOperationProposal) *scopedState {
	next := *s
	next.indexes = nil
	next.Target = desired.Clone()
	next.Groups = slices.Clone(s.Groups)
	next.Operations = make([]topology.Operation, 0, len(s.Operations)+1)
	owned := make(map[string]bool, len(p.Scopes))
	for _, scope := range p.Scopes {
		owned[scope.GroupID] = true
	}
	for _, previous := range s.Operations {
		operation := previous.Clone()
		changed := false
		for i, block := range operation.Status.Blocks {
			if owned[block.GroupID] && !block.State.Terminal() {
				operation.Status.Blocks[i].State = topology.OperationSuperseded
				operation.Status.Blocks[i].Blocked = nil
				changed = true
			}
		}
		if changed {
			operation.Status.State = aggregateBlockStates(operation.Status.Blocks)
			operation.UpdatedAt = p.At
		}
		next.Operations = append(next.Operations, operation)
	}
	operation := newScopedOperation(p)
	declarations := s.planningDeclarations()
	for i := range next.Groups {
		row := &next.Groups[i]
		row.Declaration = declarations.Groups[row.GroupID].Converged
		if owned[row.GroupID] {
			row.Owner = operation.Status.ID
			row.Deleting = desired.Group(row.GroupID) == nil
			delete(owned, row.GroupID)
		}
	}
	for _, block := range p.Plan.Blocks {
		if owned[block.GroupID] {
			next.Groups = append(next.Groups, groupConvergence{GroupID: block.GroupID,
				Owner: operation.Status.ID, Deleting: desired.Group(block.GroupID) == nil})
		}
	}
	next.Operations = append(next.Operations, operation)
	return &next
}

func newScopedOperation(p BeginOperationProposal) topology.Operation {
	operation := topology.Operation{Plan: p.Plan.Clone(), Actor: p.Actor, Origin: p.Origin,
		StartedAt: p.At, UpdatedAt: p.At, Status: topology.OperationStatus{
			ID: topology.MakeOperationID(p.Version, 1), Version: p.Version, Attempt: 1,
			State: topology.OperationPending, TotalBlocks: len(p.Plan.Blocks)}}
	for _, block := range p.Plan.Blocks {
		operation.Status.Blocks = append(operation.Status.Blocks, topology.BlockStatus{
			GroupID: block.GroupID, State: topology.OperationPending, TotalSteps: len(block.Steps)})
	}
	return operation
}

func aggregateBlockStates(blocks []topology.BlockStatus) topology.OperationState {
	state := topology.OperationSucceeded
	var active topology.OperationState
	for _, block := range blocks {
		if !block.State.Terminal() {
			if active == "" || block.State == topology.OperationRunning {
				active = block.State
			}
		}
		if block.State == topology.OperationFailed ||
			block.State == topology.OperationSuperseded && state != topology.OperationFailed {
			state = block.State
		}
	}
	if active != "" {
		return active
	}
	return state
}
