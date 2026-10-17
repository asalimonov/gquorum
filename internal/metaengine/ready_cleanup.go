package metaengine

import (
	"sort"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// ReadyCleanup rotates exact retained identities, returning only detached work.
func (m *Machine) ReadyCleanup(after storage.Identity, limit int) ([]operator.CleanupWork, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return queryReadyCleanup(m.scoped, after, limit)
}

func queryReadyCleanup(s *scopedState, after storage.Identity, limit int) ([]operator.CleanupWork, error) {
	if limit < 1 {
		return nil, fault.ErrInvalid
	}
	if s == nil {
		return nil, fault.ErrUnsupported
	}
	if after != (storage.Identity{}) && after.ClusterID != s.Target.ClusterID {
		return nil, fault.ErrIdentityMismatch
	}
	rows := s.indexes.cleanupOrder
	start := sort.Search(len(rows), func(i int) bool { return compareReplicaIdentity(rows[i].Identity, after) > 0 })
	var work []operator.CleanupWork
	for offset := range rows {
		debt := rows[(start+offset)%len(rows)]
		action := pendingCleanupAction(debt)
		if action == nil {
			continue
		}
		if _, err := s.cleanupDebtForAction(debt.Identity, *action); err != nil {
			continue // Includes required stops and changed assignments/bindings.
		}
		work = append(work, operator.CleanupWork{Identity: debt.Identity, MemberID: debt.MemberID, Action: *action})
		if len(work) == limit {
			break
		}
	}
	return work, nil
}

func pendingCleanupAction(debt *cleanupRecord) *topology.AuthorizedStep {
	if debt.Stopped == nil {
		return debt.StopAction
	}
	if debt.Retired == nil {
		return debt.RetireAction
	}
	return nil // Collection belongs to the host's existing retention worker.
}
