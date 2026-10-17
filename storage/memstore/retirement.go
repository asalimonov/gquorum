package memstore

import (
	"context"
	"fmt"
	"os"
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

// Collect releases an enumerated volatile retirement and retains exact local
// proof until ForgetCollection. A fresh process has no such proof and returns
// NotFound; callers must never turn this into durable CollectedAt evidence.
func (s *Store) Collect(ctx context.Context, record storage.RetiredReplica) error {
	if err := s.validateCollection(record); err != nil {
		return err
	}
	if err := s.enter(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	want := record.Identity
	if bound, exists := s.bindings[want.GroupID]; exists && bound == want {
		return fault.ErrIdentityMismatch
	}
	if live := s.groups[want.GroupID]; live != nil && live.identity == want {
		return fault.ErrIdentityMismatch
	}
	i := retiredTokenIndex(s.retired, record.Token)
	if i < 0 {
		if prior := retiredTokenIndex(s.collected, record.Token); prior >= 0 {
			return matchRetiredRecord(s.collected[prior], record)
		}
		return os.ErrNotExist
	}
	if err := matchRetiredRecord(s.retired[i], record); err != nil {
		return err
	}
	s.collected = append(s.collected, s.retired[i])
	s.retired = slices.Delete(s.retired, i, i+1)
	return nil
}

// ForgetCollection drops only exact volatile collection proof. It cannot prove
// anything about a prior process and never creates a collection receipt.
func (s *Store) ForgetCollection(ctx context.Context, record storage.RetiredReplica) error {
	if err := s.validateCollection(record); err != nil {
		return err
	}
	if err := s.enter(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if retiredTokenIndex(s.retired, record.Token) >= 0 {
		return fault.ErrIdentityMismatch
	}
	i := retiredTokenIndex(s.collected, record.Token)
	if i < 0 {
		return nil
	}
	if err := matchRetiredRecord(s.collected[i], record); err != nil {
		return err
	}
	s.collected = slices.Delete(s.collected, i, i+1)
	return nil
}

func (s *Store) validateCollection(record storage.RetiredReplica) error {
	want := record.Identity
	if want.NodeName != s.opts.NodeName || want.ClusterID != s.opts.ClusterID || want.ReplicaID == 0 {
		return fault.ErrIdentityMismatch
	}
	if record.Token == "" || record.QuarantinedAt.IsZero() {
		return fault.ErrInvalid
	}
	return topology.ValidateLoadID(want.GroupID)
}

func retiredTokenIndex(records []storage.RetiredReplica, token string) int {
	return slices.IndexFunc(records, func(r storage.RetiredReplica) bool { return r.Token == token })
}

func matchRetiredRecord(a, b storage.RetiredReplica) error {
	if a.Identity != b.Identity {
		return fault.ErrIdentityMismatch
	}
	if a.Token != b.Token || !a.QuarantinedAt.Equal(b.QuarantinedAt) {
		return fault.ErrInvalid
	}
	return nil
}
