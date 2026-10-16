package metaengine

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Task78 connects this to the existing RecoveryCatalog read lock. These rows
// are discovery evidence for exact cleanup/recovery, never a membership document
// or permission to acquire an application. Staged declarations are excluded.
func queryScopedRecoveryCatalog(s *scopedState) ([]topology.Group, error) {
	if s == nil {
		return nil, fault.ErrUnsupported
	}
	catalog := recoveryCatalog{cluster: s.Target.ClusterID, groups: make(map[string]*topology.Group),
		seen: make(map[storage.Identity]bool)}
	if err := catalog.addScopedDeclarations(s); err != nil {
		return nil, err
	}
	for _, record := range s.AuthorityRecords {
		for _, target := range record.Grant.Targets {
			catalog.add(record.Grant.Key.GroupID, topology.Replica{
				NodeName: target.NodeName, ReplicaID: target.ReplicaID})
		}
	}
	for _, debt := range s.Cleanup {
		catalog.add(debt.Identity.GroupID, topology.Replica{
			NodeName: debt.Identity.NodeName, ReplicaID: debt.Identity.ReplicaID})
	}
	return catalog.sorted(), nil
}

func (catalog *recoveryCatalog) addScopedDeclarations(s *scopedState) error {
	for _, op := range s.Operations {
		if s.indexes.documents[op.Status.Version].Version == 0 {
			return fault.ErrHistoryUnavailable
		}
	}
	for _, doc := range s.authorityDocuments() {
		catalog.addDocument(doc)
	}
	for _, row := range s.Groups {
		if row.Declaration == nil {
			continue
		}
		if _, found := checkpointDocument(s, s.indexes, row); !found {
			return fault.ErrHistoryUnavailable
		}
		for _, replica := range row.Declaration.Replicas {
			catalog.add(row.GroupID, replica)
		}
	}
	return nil
}

// RecoveryCatalog copies exact scoped identities for local recovery and cleanup.
// It does not authorize starting or retiring a replica by itself.
func (m *Machine) RecoveryCatalog(ctx context.Context) ([]topology.Group, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return queryScopedRecoveryCatalog(nil) // Task78 installs complete scoped state.
}

// WithGrantedLoadCleanup pins approved assignments through an exact ledger
// mutation. The caller already owns the installed host fence. fn must be bounded
// and must not reenter this Machine; closed retained authority remains proof.
func (m *Machine) WithGrantedLoadCleanup(ctx context.Context, grant topology.AuthorityGrant,
	target topology.AuthorityTarget, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := topology.ValidateLoadID(grant.Key.GroupID); err != nil {
		return err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := queryScopedLoadCleanup(nil, grant, target); err != nil {
		return err
	} // Task78 supplies scoped state.
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn()
}

func queryScopedLoadCleanup(s *scopedState, grant topology.AuthorityGrant, target topology.AuthorityTarget) error {
	if s == nil {
		return fault.ErrUnsupported
	}
	record := s.indexes.grants[grant.Key]
	if record == nil {
		return missingLoadCleanupGrant(grant, s.Target.ClusterID, s.AppliedIndex)
	}
	return validateLoadCleanupTarget(s.Target, record.Grant, grant, target)
}
