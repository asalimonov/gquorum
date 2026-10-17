package metaengine

import (
	"errors"
	"maps"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/topology"
)

// ReadyBlocks returns owned candidates, not authority. After is an ephemeral
// group-key continuation; selection wraps once and never reserves durable work.
func (m *Machine) ReadyBlocks(after string, limit int) ([]operator.WorkItem, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return queryReadyBlocks(m.scoped, after, limit)
}

func queryReadyBlocks(s *scopedState, after string, limit int) ([]operator.WorkItem, error) {
	if limit < 1 {
		return nil, fault.ErrInvalid
	}
	if s == nil {
		return nil, fault.ErrUnsupported
	}
	ids := slices.Sorted(maps.Keys(s.indexes.groups))
	start, found := slices.BinarySearch(ids, after)
	if found {
		start++
	}
	var work []operator.WorkItem
	for offset := range ids {
		id := ids[(start+offset)%len(ids)]
		row := s.indexes.groups[id]
		op := s.indexes.operations[row.Owner]
		position := readyBlockPosition(op, id)
		if position < 0 || s.requiredBlocker(op, position) != nil {
			continue
		}
		work = append(work, s.requiredWork(op, position))
		if len(work) == limit {
			break
		}
	}
	return work, nil
}

func (s *scopedState) requiredWork(op *topology.Operation, position int) operator.WorkItem {
	definition, block := op.Plan.Blocks[position], op.Status.Blocks[position]
	work := operator.WorkItem{Operation: op.Clone(), GroupID: definition.GroupID}
	doc := s.indexes.documents[op.Status.Version]
	if group := doc.Group(definition.GroupID); group != nil {
		owned := (topology.Document{Groups: []topology.Group{*group}}).Clone()
		work.Approved = &owned.Groups[0]
	}
	seq := block.CurrentStep
	if seq == 0 && len(definition.Steps) != 0 {
		seq = definition.Steps[0]
	}
	if seq != 0 && op.Plan.Steps[seq-1].Kind != topology.StepWaitCaughtUp {
		work.Consumed = scopedStepConsumed(op, op.Plan.Steps[seq-1], s.indexes, s.authorityCatalog())
	}
	return work
}

func readyBlockPosition(op *topology.Operation, group string) int {
	if op == nil || op.Status.BlocksTruncated || op.Status.State.Terminal() {
		return -1
	}
	position := slices.IndexFunc(op.Plan.Blocks, func(block topology.PlanBlock) bool { return block.GroupID == group })
	if position < 0 || position >= len(op.Status.Blocks) || op.Status.Blocks[position].GroupID != group ||
		op.Status.Blocks[position].State.Terminal() {
		return -1
	}
	return position
}

// Only replicated predicates filter work. Unreachable hosts, catch-up and native
// safety need a bounded worker check. Requires edges explain these state gates;
// replaying a superseded predecessor's cursor would strand valid current state.
func (s *scopedState) requiredBlocker(op *topology.Operation, position int) *topology.BlockedReason {
	definition, block := op.Plan.Blocks[position], op.Status.Blocks[position]
	if len(definition.Steps) == 0 {
		return nil // Zero-step membership/deletion still needs its final check.
	}
	seq := block.CurrentStep
	if seq == 0 {
		seq = definition.Steps[0]
	}
	step := op.Plan.Steps[seq-1]
	action := scopedPlanAction(op, step)
	catalog := s.authorityCatalog()
	if step.Kind != topology.StepWaitCaughtUp && scopedStepConsumed(op, step, s.indexes, catalog) {
		return nil // Consumption may advance/finalize even after its grant closes.
	}
	if err := s.validateActionBarriers(op, action); errors.Is(err, fault.ErrUnavailable) {
		return blockWait(action, topology.BlockedDependency, err)
	} else if err != nil {
		return nil // The worker must record a permanent refusal, not hide it.
	}
	if step.Kind == topology.StepWaitCaughtUp || catalog.records[catalog.actions[action]] != nil {
		return nil // Reuse an issued grant; its own floor is not a prior-work gate.
	}
	resources, err := s.grantResources(action)
	if err != nil {
		return nil
	}
	if err := s.resourcesAvailable(resources.Resources); err != nil {
		return blockWait(action, topology.BlockedRevocationPending, err)
	}
	return nil
}

func blockWait(action topology.AuthorizedStep, code string, err error) *topology.BlockedReason {
	return &topology.BlockedReason{Code: code, Message: err.Error(),
		OperationID: topology.OperationID(action.OperationID),
		StepSeq:     action.StepSeq, NodeName: action.NodeName, ReplicaID: action.ReplicaID}
}
