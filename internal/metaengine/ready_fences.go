package metaengine

import (
	"sort"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/topology"
)

// ReadyFences selects bounded current boundaries, independently of required work.
func (m *Machine) ReadyFences(after topology.AuthorityResource, limit int) ([]operator.FenceWork, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return queryReadyFences(nil, after, limit) // Task78 installs scoped persistence.
}

func queryReadyFences(s *scopedState, after topology.AuthorityResource, limit int) ([]operator.FenceWork, error) {
	if limit < 1 {
		return nil, fault.ErrInvalid
	}
	if s == nil {
		return nil, fault.ErrUnsupported
	}
	resources := s.indexes.obligationOrder
	start := sort.Search(len(resources), func(i int) bool { return resources[i].Compare(after) > 0 })
	catalog := s.authorityCatalog()
	var work []operator.FenceWork
	for offset := range resources {
		resource := resources[(start+offset)%len(resources)]
		record := catalog.records[catalog.floors[resource]]
		if record == nil || record.Grant.Purpose != topology.AuthorityFence ||
			catalog.fenceConsumed(record.Grant.Key, resource) {
			continue // An issued effect is handled by its own exact worker.
		}
		work = append(work, operator.FenceWork{Grant: record.Grant.Clone(), Resource: resource})
		if len(work) == limit {
			break
		}
	}
	return work, nil
}
