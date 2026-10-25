package metastore

import (
	"context"
	"errors"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"
	"github.com/asalimonov/gquorum/storage"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3/raftpb"
)

// Reader owns a read-only metadata descriptor; it never initializes source state.
type Reader struct {
	mu       sync.Mutex
	db       *bolt.DB
	maxBytes uint64
}

// OpenReadOnly validates the existing schema, complete pin and ledger without writes.
func OpenReadOnly(ctx context.Context, path string, maxBytes uint64) (*Reader, error) {
	db, err := recoverydisk.OpenReadOnlyDB(ctx, path, maxBytes)
	if err != nil {
		return nil, err
	}
	r := &Reader{db: db, maxBytes: maxBytes}
	err = db.View(func(tx *bolt.Tx) error {
		if err := validateDiskSchema(tx); err != nil {
			return err
		}
		id, err := readIdentityChecked(tx)
		if err != nil {
			return err
		}
		if !id.Found {
			return fault.ErrIdentityMismatch
		}
		return loadReplicaRecordsTx(tx, make(map[string]ReplicaRecord))
	})
	if err != nil {
		return nil, errors.Join(err, r.Close())
	}
	return r, nil
}

// Identity returns only the retained durable pin, never a guessed process identity.
func (r *Reader) Identity() (Identity, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.db == nil {
		return Identity{}, fault.ErrStopped
	}
	var result Identity
	err := r.db.View(func(tx *bolt.Tx) error {
		id, err := readIdentityChecked(tx)
		if err != nil {
			return err
		}
		if !id.Found {
			return fault.ErrIdentityMismatch
		}
		result = id.Identity
		return nil
	})
	return result, err
}

// Digest binds all retained logical metadata records without hashing unused pages.
func (r *Reader) Digest(ctx context.Context) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.db == nil {
		return "", fault.ErrStopped
	}
	return recoverydisk.DigestDB(ctx, r.db, r.maxBytes)
}

// Recovery reads one complete retained transaction; applied state remains a hint.
//
//revive:disable-next-line:function-result-limit Target returns recovery and ledger from one transaction.
func (r *Reader) Recovery(ctx context.Context) (storage.Recovery, map[string]ReplicaRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.db == nil {
		return storage.Recovery{}, nil, fault.ErrStopped
	}
	var retained storage.Recovery
	ledger := make(map[string]ReplicaRecord)
	err := r.db.View(func(tx *bolt.Tx) error {
		if err := recoverydisk.CheckReadBudget(ctx, tx, r.maxBytes); err != nil {
			return err
		}
		if err := loadAllTx(tx, &retained); err != nil {
			return err
		}
		if err := readOfflineHints(tx, &retained); err != nil {
			return err
		}
		if err := validateEntryKeys(tx); err != nil {
			return err
		}
		return loadReplicaRecordsTx(tx, ledger)
	})
	if err != nil {
		return storage.Recovery{}, nil, err
	}
	return retained, ledger, nil
}
func readOfflineHints(tx *bolt.Tx, retained *storage.Recovery) error {
	applied := tx.Bucket(bucketApplied)
	index := applied.Get(keyApplied)
	if index != nil && len(index) != uint64ByteCount {
		return fault.ErrUnreadable
	}
	retained.AppliedHint = btoi(index)
	if data := applied.Get(keyConfState); data != nil {
		if err := retained.AppliedConfState.Unmarshal(data); err != nil {
			return err
		}
	}
	if retained.AppliedHint > retained.HardState.Commit {
		return fault.ErrUnreadable
	}
	return nil
}
func validateEntryKeys(tx *bolt.Tx) error {
	return tx.Bucket(bucketEntries).ForEach(func(key, value []byte) error {
		if len(key) != uint64ByteCount || value == nil {
			return fault.ErrUnreadable
		}
		var entry raftpb.Entry
		if err := entry.Unmarshal(value); err != nil {
			return err
		}
		if entry.Index == 0 || entry.Index != btoi(key) {
			return fault.ErrUnreadable
		}
		return nil
	})
}

// Close releases the read-only descriptor after the inspector joins its use.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.db == nil {
		return nil
	}
	if err := r.db.Close(); err != nil {
		return err
	}
	r.db = nil
	return nil
}
