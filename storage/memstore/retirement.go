package memstore

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Quarantine removes exactly the expected replica from active use. Its Raft
// state is released at once, since nothing can recover it after the process
// exits; the retired record remains until Collect.
func (s *Store) Quarantine(ctx context.Context, id string, expectedReplicaID uint64) (string, error) {
	if err := topology.ValidateLoadID(id); err != nil {
		return "", err
	}
	if expectedReplicaID == 0 {
		return "", fault.New(fault.Invalid, "expected replica ID must be positive", nil)
	}
	if err := s.enter(ctx); err != nil {
		return "", err
	}
	defer s.mu.Unlock()
	g := s.groups[id]
	if g == nil {
		return "", fault.New(fault.Unreadable, "quarantine requires an identity pin", nil)
	}
	want := storage.Identity{NodeName: s.opts.NodeName, ClusterID: s.opts.ClusterID, GroupID: id,
		ReplicaID: expectedReplicaID}
	if g.identity != want {
		return "", fault.New(fault.IdentityMismatch, "quarantine names another replica", nil)
	}
	s.sequence++
	token := fmt.Sprintf("memory/%s/%d/%d", id, expectedReplicaID, s.sequence)
	s.retired = append(s.retired, storage.RetiredReplica{Identity: want, Token: token, QuarantinedAt: time.Now().UTC()})
	delete(s.groups, id)
	delete(s.bindings, id)
	return token, nil
}

// Retired returns an owned list of quarantined replica records.
func (s *Store) Retired(ctx context.Context) ([]storage.RetiredReplica, error) {
	if err := s.enter(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	return slices.Clone(s.retired), nil
}

// Collect forgets exactly an enumerated record. It is idempotent once the
// record is gone and refuses an identity that is bound or live again.
func (s *Store) Collect(ctx context.Context, record storage.RetiredReplica) error {
	want := record.Identity
	if want.NodeName != s.opts.NodeName || want.ClusterID != s.opts.ClusterID || want.ReplicaID == 0 {
		return fault.ErrIdentityMismatch
	}
	if err := s.enter(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if bound, exists := s.bindings[want.GroupID]; exists && bound == want {
		return fault.ErrIdentityMismatch
	}
	if live := s.groups[want.GroupID]; live != nil && live.identity == want {
		return fault.ErrIdentityMismatch
	}
	i := slices.IndexFunc(s.retired, func(r storage.RetiredReplica) bool { return r.Token == record.Token })
	if i < 0 {
		return nil
	}
	if found := s.retired[i]; found.Identity != want {
		return fault.ErrIdentityMismatch
	} else if !found.QuarantinedAt.Equal(record.QuarantinedAt) {
		return fault.ErrInvalid
	}
	s.retired = slices.Delete(s.retired, i, i+1)
	return nil
}
