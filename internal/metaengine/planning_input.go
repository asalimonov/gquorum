package metaengine

import (
	"context"

	"github.com/asalimonov/gquorum/topology"
)

// Evidence is borrowed under the FSM lock; planning collects owned output.
type planningEvidence struct {
	documents   []topology.Document
	authority   authorityCatalog
	allocations map[string]uint64
	admissions  []AdmissionRecord
	cleanup     []cleanupRecord
	operations  map[topology.OperationID]*topology.Operation
}

// PlanningInput captures owned local planning evidence. Quorum planning uses
// Query so capture and native observation can be revalidated at one boundary.
func (m *Machine) PlanningInput(ctx context.Context, desired topology.Document,
	native map[string]topology.NativeState) (topology.PlanInput, error) {
	if err := ctx.Err(); err != nil {
		return topology.PlanInput{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	input := m.scoped.planningDeclarations()
	for id, state := range native {
		input.Native[id] = state.Clone()
	}
	collector := m.scoped.planningCollector()
	return collector.collect(input, desired)
}
