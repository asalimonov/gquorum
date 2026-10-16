package metaengine

import (
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Operation returns owned local status for one retained attempt. It neither
// confirms quorum nor selects a newer attempt, and always includes every block.
func (m *Machine) Operation(id topology.OperationID) (topology.OperationStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return queryOperation(nil, m.operators, id)
}

func queryOperation(s *scopedState, records map[topology.OperatorID]*operatorRecord,
	id topology.OperationID) (topology.OperationStatus, error) {
	if s != nil {
		return queryOperationStatus(s, id)
	}
	return legacyOperationStatus(records, id)
}

// legacyOperationStatus exposes the exact schema-3 retained record while the
// public Operation API is already available. Schema 4 replaces this adapter.
func legacyOperationStatus(records map[topology.OperatorID]*operatorRecord,
	id topology.OperationID) (topology.OperationStatus, error) {
	if strings.TrimSpace(string(id)) == "" {
		return topology.OperationStatus{}, fault.ErrInvalid
	}
	record := records[topology.OperatorID(id)]
	if record == nil || topology.OperationID(record.ID) != id {
		return topology.OperationStatus{}, fault.New(fault.HistoryUnavailable, "operation outcome is not retained", nil)
	}
	status := topology.OperationStatus{ID: id, Version: record.Version, Attempt: record.Attempt,
		State: topology.OperationState(record.State), Error: record.Error}
	if len(record.Steps) == 0 {
		return status, nil
	}
	group := record.GroupID
	if group == "" && record.CurrentStep == 0 {
		group = record.Steps[0].GroupID
	}
	if group == "" {
		return topology.OperationStatus{}, fault.New(fault.Unreadable, "retained operation has no current group", nil)
	}
	block := topology.BlockStatus{GroupID: group, State: status.State,
		CurrentStep: record.CurrentStep, TotalSteps: len(record.Steps), Error: record.Error}
	if record.WaitingReason != "" && record.State.Waiting() {
		block.Blocked = &topology.BlockedReason{Code: topology.BlockedDependency,
			Message: record.WaitingReason, NodeName: record.NodeName}
	}
	status.Blocks, status.TotalBlocks = []topology.BlockStatus{block}, 1
	return status, nil
}

func queryOperationStatus(s *scopedState, id topology.OperationID) (topology.OperationStatus, error) {
	if strings.TrimSpace(string(id)) == "" {
		return topology.OperationStatus{}, fault.ErrInvalid
	}
	if s == nil {
		return topology.OperationStatus{}, fault.ErrUnsupported
	}
	op := s.indexes.operations[id]
	if op == nil {
		return topology.OperationStatus{}, fault.New(fault.HistoryUnavailable, "operation outcome is not retained", nil)
	}
	status := op.Status.Clone()
	for i := range status.Blocks {
		block := &status.Blocks[i]
		if row := s.indexes.groups[block.GroupID]; row != nil && row.Owner == id {
			block.Blocked = copyCleanupValue(s.progressBlocker(id, block.GroupID))
		}
	}
	return status, nil
}
