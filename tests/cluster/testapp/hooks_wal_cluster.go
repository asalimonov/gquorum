package main

import (
	"context"
	"net/http"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type walKey struct {
	group   string
	replica uint64
}
type walCounters struct {
	Attempts          uint64 `json:"attempts"`
	EntryAttempts     uint64 `json:"entry_attempts"`
	Failures          uint64 `json:"failures"`
	Active            uint64 `json:"active"`
	EntryWrites       uint64 `json:"entry_writes"`
	Entries           uint64 `json:"entries"`
	Blocked           bool   `json:"blocked"`
	RemainingFailures uint64 `json:"remaining_failures"`
}
type walControl struct {
	counters walCounters
	release  chan struct{}
}
type controlledStore struct {
	app  *application
	node topology.Identity
	storage.Store
	hooks *clusterHooks
	key   walKey
}

func (a *application) clusterStore(base storage.Store, group string, replica uint64) storage.Store {
	if a.clusterHooks == nil {
		return base
	}
	h := a.clusterHooks
	h.mu.Lock()
	defer h.mu.Unlock()
	key := walKey{group, replica}
	if h.wals[key] == nil {
		h.wals[key] = &walControl{}
	}
	return controlledStore{app: a, node: a.snapshotNodeIdentity(), Store: base, hooks: h, key: key}
}
func (s controlledStore) before(ctx context.Context, entries int) error {
	h := s.hooks
	h.mu.Lock()
	control := h.wals[s.key]
	control.counters.Attempts++
	if entries > 0 {
		control.counters.EntryAttempts++
	}
	control.counters.Active++
	release := control.release
	h.mu.Unlock()
	var err error
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	h.mu.Lock()
	if err == nil && control.counters.RemainingFailures > 0 {
		control.counters.RemainingFailures--
		err = fault.ErrUnavailable
	}
	h.mu.Unlock()
	return err
}
func (s controlledStore) after(err error, entries int) {
	h := s.hooks
	h.mu.Lock()
	defer h.mu.Unlock()
	control := h.wals[s.key]
	control.counters.Active--
	if err != nil {
		control.counters.Failures++
	}
	if err == nil && entries > 0 {
		control.counters.Entries += uint64(entries)
		control.counters.EntryWrites++
	}
}
func (s controlledStore) SaveReady(ctx context.Context, group string, write storage.ReadyWrite) (err error) {
	if group != s.key.group {
		return fault.ErrIdentityMismatch
	}
	err = s.before(ctx, len(write.Entries))
	defer func() { s.after(err, len(write.Entries)) }()
	if err != nil {
		return err
	}
	return s.Store.SaveReady(ctx, group, write)
}
func (s controlledStore) SaveApplied(ctx context.Context, group string, index uint64, conf raftpb.ConfState) (err error) {
	if group != s.key.group {
		return fault.ErrIdentityMismatch
	}
	err = s.before(ctx, 0)
	defer func() { s.after(err, 0) }()
	if err != nil {
		return err
	}
	return s.Store.SaveApplied(ctx, group, index, conf)
}
func (s controlledStore) SaveSnapshot(ctx context.Context, group string, snapshot raftpb.Snapshot) (err error) {
	if group != s.key.group {
		return fault.ErrIdentityMismatch
	}
	err = s.before(ctx, 0)
	defer func() { s.after(err, 0) }()
	if err != nil {
		return err
	}
	if err = s.app.snapshotPhase(ctx, s.node, s.key, "before_persist", 0); err != nil {
		return err
	}
	if err = s.Store.SaveSnapshot(ctx, group, snapshot); err != nil {
		return err
	}
	return s.app.snapshotPhase(ctx, s.node, s.key, "after_persist", 0)
}
func (h *clusterHooks) walMetrics(key walKey) walCounters {
	h.mu.Lock()
	defer h.mu.Unlock()
	if control := h.wals[key]; control != nil {
		return control.counters
	}
	return walCounters{}
}
func (a *application) registerWALRoutes(mux *http.ServeMux, protect func(http.HandlerFunc) http.HandlerFunc) {
	mutate := func(w http.ResponseWriter, r *http.Request) {
		if a.store == nil {
			httpError(w, fault.ErrUnsupported)
			return
		}
		group := r.PathValue("group")
		if err := topology.ValidateLoadID(group); err != nil {
			httpError(w, err)
			return
		}
		var body struct {
			ReplicaID uint64 `json:"replica_id"`
			Stall     bool   `json:"stall"`
			FailNext  uint64 `json:"fail_next"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		local, found := a.currentNode().LoadGroup(group)
		if !found || body.ReplicaID == 0 || local.ReplicaID() != body.ReplicaID {
			httpError(w, fault.ErrIdentityMismatch)
			return
		}
		h := a.clusterHooks
		h.mu.Lock()
		control := h.wals[walKey{group, body.ReplicaID}]
		if control == nil {
			h.mu.Unlock()
			httpError(w, fault.ErrReplicaNotReady)
			return
		}
		if r.Method == "DELETE" {
			if control.release != nil {
				close(control.release)
				control.release = nil
			}
			control.counters.Blocked = false
			control.counters.RemainingFailures = 0
		} else {
			if !body.Stall && body.FailNext == 0 || body.FailNext > 100 || control.release != nil || control.counters.RemainingFailures > 0 {
				h.mu.Unlock()
				httpError(w, fault.ErrInvalid)
				return
			}
			if body.Stall {
				control.release = make(chan struct{})
				control.counters.Blocked = true
			}
			control.counters.RemainingFailures = body.FailNext
		}
		counters := control.counters
		h.mu.Unlock()
		jsonResponse(w, http.StatusOK, counters)
	}
	mux.HandleFunc("PUT /internal/test/wal/{group}", protect(mutate))
	mux.HandleFunc("DELETE /internal/test/wal/{group}", protect(mutate))
}
