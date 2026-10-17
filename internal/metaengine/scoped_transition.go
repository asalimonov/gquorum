package metaengine

import (
	"reflect"
	"slices"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) applyTransitionOperation(index uint64,
	p TransitionOperationProposal) (proposalApplication, error) {
	t := p.Transition
	op, position := s.transitionBlock(t)
	if op == nil || t.At.IsZero() || !validScopedTransitionDiagnostic(t) {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	block := op.Status.Blocks[position]
	result := proposalApplication{result: scopedApprovalResult(p.ProposalID, op.Status.ID)}
	if scopedTransitionMatches(block, t) {
		return result, nil // Stable retries/polling do not rewrite progress timestamps.
	}
	if t.At.Before(op.UpdatedAt) {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	if err := s.validateScopedTransition(op, t); err != nil {
		return proposalApplication{}, err
	}
	if index <= s.AppliedIndex {
		return proposalApplication{}, fault.ErrOperationAuthorization
	}
	prepare := s.prepareBlockTransition
	if t.State == topology.OperationSucceeded {
		prepare = s.completeGroupLocked
	}
	next, err := prepare(index, t)
	if err != nil {
		return proposalApplication{}, err
	}
	next.AppliedIndex = index
	next.indexes, err = prepareScopedGraph(next, next.Rules)
	if err != nil {
		return proposalApplication{}, err
	}
	*s, result.changed = *next, true
	return result, nil
}

func (s *scopedState) prepareBlockTransition(index uint64, t topology.OperationTransition) (*scopedState, error) {
	op, position := s.transitionBlock(t)
	block := op.Status.Blocks[position]
	next := s.cloneExecutionState()
	owned, _ := next.transitionBlock(t)
	owned.Status.Blocks[position] = scopedTransitionStatus(block.TotalSteps, t)
	owned.Status.State, owned.UpdatedAt = aggregateBlockStates(owned.Status.Blocks), t.At
	if t.StepSeq > block.CurrentStep && block.CurrentStep != 0 {
		if err := next.recordScopedTransitionEffect(index, op, block.CurrentStep, t.At); err != nil {
			return nil, err
		}
	}
	if t.State.Terminal() {
		next.releaseBlockOwner(t.OperationID, t.GroupID)
	}
	return next, nil
}

func (s *scopedState) transitionBlock(t topology.OperationTransition) (*topology.Operation, int) {
	op := s.indexes.operations[t.OperationID]
	if op == nil || op.Status.Version != t.Version || op.Status.Attempt != t.Attempt {
		return nil, 0
	}
	position := slices.IndexFunc(op.Plan.Blocks, func(block topology.PlanBlock) bool {
		return block.GroupID == t.GroupID
	})
	if position < 0 {
		return nil, 0
	}
	return op, position
}

func (s *scopedState) validateScopedTransition(op *topology.Operation, t topology.OperationTransition) error {
	matched, position := s.transitionBlock(t)
	if matched == nil || matched.Status.ID != op.Status.ID {
		return fault.ErrOperationAuthorization
	}
	block, definition := op.Status.Blocks[position], op.Plan.Blocks[position]
	row := s.indexes.groups[t.GroupID]
	if row == nil || row.Owner != op.Status.ID || block.State.Terminal() ||
		!validScopedTransitionDiagnostic(t) {
		return fault.ErrOperationAuthorization
	}
	current, requested := slices.Index(definition.Steps, block.CurrentStep), slices.Index(definition.Steps, t.StepSeq)
	if t.StepSeq != 0 && requested < 0 || requested < current || requested > current+1 ||
		t.StepSeq == 0 && block.CurrentStep != 0 {
		return fault.ErrOperationAuthorization
	}
	if t.State == topology.OperationSucceeded && requested != len(definition.Steps)-1 {
		return fault.ErrOperationAuthorization
	}
	if requested > current && block.CurrentStep != 0 &&
		!scopedStepConsumed(op, op.Plan.Steps[block.CurrentStep-1], s.indexes, s.authorityCatalog()) {
		return fault.ErrAuthorityPending
	}
	return nil
}

func validScopedTransitionDiagnostic(t topology.OperationTransition) bool {
	if t.State == topology.OperationPending || t.State == topology.OperationSuperseded || !t.State.Valid() {
		return false
	}
	if t.State.Waiting() {
		return t.Error == "" && t.ErrorCode == "" && t.Blocked != nil && strings.TrimSpace(t.Blocked.Code) != ""
	}
	if t.Blocked != nil {
		return false
	}
	if t.State == topology.OperationFailed {
		return strings.TrimSpace(t.Error) != ""
	}
	return t.Error == "" && t.ErrorCode == ""
}

func scopedTransitionStatus(total int, t topology.OperationTransition) topology.BlockStatus {
	return topology.BlockStatus{GroupID: t.GroupID, State: t.State, CurrentStep: t.StepSeq, TotalSteps: total,
		Blocked: t.Clone().Blocked, ErrorCode: t.ErrorCode, Error: t.Error}
}

func scopedTransitionMatches(block topology.BlockStatus, t topology.OperationTransition) bool {
	return reflect.DeepEqual(block, scopedTransitionStatus(block.TotalSteps, t))
}

func (s *scopedState) cloneExecutionState() *scopedState {
	next := *s
	next.Operations = make([]topology.Operation, len(s.Operations))
	for i := range s.Operations {
		next.Operations[i] = s.Operations[i].Clone()
	}
	next.Groups, next.Nodes, next.Admissions = slices.Clone(s.Groups), slices.Clone(s.Nodes), slices.Clone(s.Admissions)
	next.VoterSwitches = slices.Clone(s.VoterSwitches)
	// Only operation pointers change here; the complete graph is rebuilt before
	// installation. Other indexes remain borrowed read-only during preparation.
	lookup := *s.indexes
	lookup.operations = make(map[topology.OperationID]*topology.Operation, len(next.Operations))
	for i := range next.Operations {
		lookup.operations[next.Operations[i].Status.ID] = &next.Operations[i]
	}
	next.indexes = &lookup
	return &next
}

func (s *scopedState) releaseBlockOwner(id topology.OperationID, group string) {
	for i := range s.Groups {
		if s.Groups[i].GroupID == group && s.Groups[i].Owner == id {
			s.Groups[i].Owner = ""
		}
	}
	if group == topology.MetaGroupID || s.indexes.operations[id].Status.State.Terminal() {
		for i := range s.Nodes {
			if s.Nodes[i].Owner == id {
				s.Nodes[i].Owner = ""
			}
		}
	}
}
