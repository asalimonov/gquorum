package main

import (
	"context"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3/raftpb"
)

// Node borrows this store. Bind selects the fault counters for the exact replica
// before its writer starts; replacement replicas get independent controls.
type fixtureStore struct {
	storage.Store
	app   *application
	mu    sync.RWMutex
	bound map[string]storage.Store
}

func (s *fixtureStore) BindReplica(ctx context.Context, id storage.Identity) error {
	if err := s.Store.BindReplica(ctx, id); err != nil {
		return err
	}
	wrapped := s.app.clusterStore(s.Store, id.GroupID, id.ReplicaID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bound == nil {
		s.bound = map[string]storage.Store{}
	}
	s.bound[id.GroupID] = wrapped
	return nil
}
func (s *fixtureStore) writer(group string) (storage.Store, error) {
	s.mu.RLock()
	writer := s.bound[group]
	s.mu.RUnlock()
	if writer == nil {
		return nil, fault.ErrIdentityMismatch
	}
	return writer, nil
}
func (s *fixtureStore) SaveReady(ctx context.Context, group string, write storage.ReadyWrite) error {
	writer, err := s.writer(group)
	if err != nil {
		return err
	}
	return writer.SaveReady(ctx, group, write)
}
func (s *fixtureStore) SaveApplied(ctx context.Context, group string, index uint64, conf raftpb.ConfState) error {
	writer, err := s.writer(group)
	if err != nil {
		return err
	}
	return writer.SaveApplied(ctx, group, index, conf)
}
func (s *fixtureStore) SaveSnapshot(ctx context.Context, group string, snapshot raftpb.Snapshot) error {
	writer, err := s.writer(group)
	if err != nil {
		return err
	}
	return writer.SaveSnapshot(ctx, group, snapshot)
}
