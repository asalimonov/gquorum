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
	return queryOperationStatus(m.scoped, id)
}

// OperationStatuses captures the retained operations for the local event
// observer in one read. It avoids paging through unrelated cleanup evidence on
// every Apply; public diagnosis continues to use bounded Status/Progress reads.
func (m *Machine) OperationStatuses() ([]topology.OperationStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.scoped == nil {
		return nil, fault.ErrUnreadable
	}
	statuses := make([]topology.OperationStatus, 0, len(m.scoped.Operations))
	for _, op := range m.scoped.Operations {
		status, err := queryOperationStatus(m.scoped, op.Status.ID)
		if err != nil {
			return nil, err
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
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
