package metastore

import (
	"context"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3/raftpb"
)

// Adapter exposes the dedicated meta database to the shared Raft engine. Its
// owner closes the database after stopping the engine and all FSM operations.
type Adapter struct {
	Store    *Store
	mu       sync.RWMutex
	identity storage.Identity
}

// HasState reports whether the meta database contains retained identity or
// consensus evidence.
func (a *Adapter) HasState(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	var retained bool
	err := a.Store.db.View(func(tx *bolt.Tx) error {
		retained = !diskIsFresh(tx)
		return ctx.Err()
	})
	return retained, err
}

// BindReplica verifies and binds the exact meta storage identity.
func (a *Adapter) BindReplica(ctx context.Context, id storage.Identity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id.GroupID != topology.MetaGroupID {
		return fault.ErrInvalid
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.identity.ReplicaID != 0 && a.identity != id {
		return fault.ErrIdentityMismatch
	}
	if err := a.Store.EnsureIdentity(id.NodeName, id.ClusterID, id.ReplicaID); err != nil {
		return err
	}
	a.identity = id
	return nil
}

func (a *Adapter) check(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.identity.ReplicaID == 0 || id != topology.MetaGroupID {
		return fault.ErrIdentityMismatch
	}
	return nil
}

// Probe reports the bound meta identity and retained Raft log evidence.
func (a *Adapter) Probe(ctx context.Context, id string) (storage.Probe, error) {
	if err := a.check(ctx, id); err != nil {
		return storage.Probe{}, err
	}
	var hasLog bool
	err := a.Store.db.View(func(tx *bolt.Tx) error {
		first, _ := tx.Bucket(bucketEntries).Cursor().First()
		hasLog = first != nil || len(tx.Bucket(bucketHardState).Get(keyHardState)) > 0 ||
			len(tx.Bucket(bucketSnapshot).Get(keySnapshot)) > 0
		return nil
	})
	a.mu.RLock()
	identity := a.identity
	a.mu.RUnlock()
	return storage.Probe{Exists: true, HasLog: hasLog, Identity: identity}, err
}

// Ensure verifies that the requested group is the bound meta replica.
func (a *Adapter) Ensure(ctx context.Context, id string) error {
	return a.check(ctx, id)
}

// SaveReady persists a Ready batch after verifying the meta binding.
func (a *Adapter) SaveReady(ctx context.Context, id string, w storage.ReadyWrite) error {
	if err := a.check(ctx, id); err != nil {
		return err
	}
	return a.Store.SaveReady(w.Entries, w.HardState)
}

// SaveApplied records application progress within the durable meta commit
// boundary.
func (a *Adapter) SaveApplied(ctx context.Context, id string, index uint64, cs raftpb.ConfState) error {
	if err := a.check(ctx, id); err != nil {
		return err
	}
	hs, err := a.hardState()
	if err != nil {
		return err
	}
	if index > hs.Commit {
		return fault.New(fault.Invalid, "applied meta index exceeds durable commit", nil)
	}
	return a.Store.SetApplied(index, cs)
}

// SaveSnapshot installs complete meta snapshot state with an advancing
// hard-state boundary.
func (a *Adapter) SaveSnapshot(ctx context.Context, id string, ss raftpb.Snapshot) error {
	if err := a.check(ctx, id); err != nil {
		return err
	}
	if ss.Metadata.Index == 0 || ss.Metadata.Term == 0 || len(ss.Data) == 0 || len(ss.Metadata.ConfState.Voters) == 0 {
		return fault.ErrInvalid
	}
	hs, err := a.hardState()
	if err != nil {
		return err
	}
	if ss.Metadata.Term > hs.Term {
		hs.Term = ss.Metadata.Term
		hs.Vote = 0
	}
	hs.Commit = max(hs.Commit, ss.Metadata.Index)
	return a.Store.InstallSnapshot(ss, hs)
}

// Load returns the full retained meta Raft state and applied hints.
func (a *Adapter) Load(ctx context.Context, id string) (storage.Recovery, error) {
	if err := a.check(ctx, id); err != nil {
		return storage.Recovery{}, err
	}
	recovery, err := a.Store.LoadAll()
	if err != nil {
		return storage.Recovery{}, err
	}
	recovery.AppliedHint, err = a.Store.AppliedIndex()
	if err != nil {
		return storage.Recovery{}, err
	}
	membership, err := a.Store.AppliedConfState()
	recovery.AppliedConfState = membership.ConfState
	return recovery, err
}

// Compact removes entries covered by a durable meta snapshot.
func (a *Adapter) Compact(ctx context.Context, id string, index uint64) error {
	if err := a.check(ctx, id); err != nil {
		return err
	}
	return a.Store.CompactEntries(index)
}

// Quarantine refuses meta retirement because readmission requires coordinated
// ownership teardown.
func (a *Adapter) Quarantine(ctx context.Context, id string, _ uint64) (string, error) {
	if err := a.check(ctx, id); err != nil {
		return "", err
	}
	return "", fault.New(fault.Unsupported, "meta readmission requires coordinated shutdown and caller hook", nil)
}

// Close releases the underlying meta database.
func (a *Adapter) Close() error { return a.Store.Close() }

var _ storage.Store = (*Adapter)(nil)

func (a *Adapter) hardState() (raftpb.HardState, error) {
	var hs raftpb.HardState
	err := a.Store.db.View(func(tx *bolt.Tx) error {
		return hs.Unmarshal(tx.Bucket(bucketHardState).Get(keyHardState))
	})
	return hs, err
}
