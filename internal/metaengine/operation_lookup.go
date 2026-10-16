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
	return queryOperationStatus(nil, id) // Task78 installs the complete scoped state.
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
