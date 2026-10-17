package metaengine

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// ValidateCleanupCourier checks historical journal ownership after the service's
// confirmed collection_check. It grants no effect and is not itself a barrier.
// The proposal repeats the retained proof checks at Apply.
func (m *Machine) ValidateCleanupCourier(id storage.Identity, member uint64, courier topology.Identity) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return queryScopedCleanupCourier(m.scoped, id, member, courier)
}

func queryScopedCleanupCourier(s *scopedState, id storage.Identity, member uint64, courier topology.Identity) error {
	if s == nil {
		return fault.ErrUnsupported
	}
	if id.ClusterID != s.Target.ClusterID {
		return fault.ErrIdentityMismatch
	}
	debt := s.indexes.cleanup[id]
	if debt == nil {
		return fault.ErrHistoryUnavailable
	}
	if member != debt.MemberID {
		return fault.ErrIdentityMismatch
	}
	return s.validateCollectionCourier(*debt, RecordCollectionProposal{Courier: courier})
}
