package authoritystore

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
)

// Reader owns one read-only source authority descriptor and no effect workers.
type Reader struct {
	store    *Store
	maxBytes uint64
}

// OpenReadOnly validates all retained journal facts without migration or initialization.
func OpenReadOnly(ctx context.Context, path string, maxBytes uint64) (*Reader, error) {
	db, err := recoverydisk.OpenReadOnlyDB(ctx, path, maxBytes)
	if err != nil {
		return nil, err
	}
	store := &Store{db: db}
	err = db.View(func(tx *bolt.Tx) error {
		if err := validateBuckets(tx); err != nil {
			return err
		}
		var pinned identityPin
		data := tx.Bucket([]byte("identity")).Get([]byte("current"))
		if err := model.DecodeStrict(data, &pinned, maxBytes); err != nil {
			return err
		}
		store.identity = topology.Identity{NodeName: pinned.NodeName, ClusterID: pinned.ClusterID,
			MemberID: pinned.MemberID, InstanceID: "offline-inspection"}
		if !validIdentity(store.identity) {
			return fault.ErrUnreadable
		}
		return store.validate(tx)
	})
	if err != nil {
		return nil, errors.Join(err, store.Close())
	}
	return &Reader{store: store, maxBytes: maxBytes}, nil
}

// Identity returns the validated durable identity with no process incarnation.
func (r *Reader) Identity() (topology.Identity, error) {
	r.store.mu.RLock()
	defer r.store.mu.RUnlock()
	if r.store.db == nil {
		return topology.Identity{}, fault.ErrStopped
	}
	id := r.store.identity
	id.InstanceID = ""
	return id, nil
}

// Digest binds all retained logical journal records, including allocation counters.
func (r *Reader) Digest(ctx context.Context) (string, error) {
	r.store.mu.RLock()
	defer r.store.mu.RUnlock()
	if r.store.db == nil {
		return "", fault.ErrStopped
	}
	return recoverydisk.DigestDB(ctx, r.store.db, r.maxBytes)
}

// Evidence returns owned source records after complete retained validation.
func (r *Reader) Evidence(ctx context.Context) (model.AuthorityEvidence, error) {
	id, err := r.Identity()
	if err != nil {
		return model.AuthorityEvidence{}, err
	}
	evidence := model.AuthorityEvidence{Identity: id}
	err = r.store.readTransaction(ctx, func(tx *bolt.Tx) error {
		if err := recoverydisk.CheckReadBudget(ctx, tx, r.maxBytes); err != nil {
			return err
		}
		if err := r.store.validate(tx); err != nil {
			return err
		}
		identity := tx.Bucket([]byte("identity"))
		evidence.MetadataAcquired = identity.Get([]byte("metadata_acquired")) != nil
		if data := identity.Get([]byte("readmission")); data != nil {
			var proof rebindRecord
			if err := model.DecodeStrict(data, &proof, r.maxBytes); err != nil {
				return err
			}
			evidence.AdmissionID = proof.AdmissionID
			evidence.PreviousMemberID = proof.Previous.MemberID
		}
		return readAuthorityRows(tx, &evidence, r.maxBytes)
	})
	if err != nil {
		return model.AuthorityEvidence{}, err
	}
	return evidence, nil
}
func readAuthorityRows(tx *bolt.Tx, out *model.AuthorityEvidence, maxBytes uint64) error {
	fenceErr := tx.Bucket([]byte("fences")).ForEach(func(_, data []byte) error {
		var record model.AuthorityFence
		if err := model.DecodeStrict(data, &record, maxBytes); err != nil {
			return err
		}
		out.Fences = append(out.Fences, record)
		return nil
	})
	if fenceErr != nil {
		return fenceErr
	}
	effectErr := tx.Bucket([]byte("effects")).ForEach(func(_, data []byte) error {
		var record model.AuthorityEffect
		if err := model.DecodeStrict(data, &record, maxBytes); err != nil {
			return err
		}
		out.Effects = append(out.Effects, record)
		return nil
	})
	if effectErr != nil {
		return effectErr
	}
	return tx.Bucket([]byte(collectionBucket)).ForEach(func(_, data []byte) error {
		var record model.AuthorityCollection
		if err := model.DecodeStrict(data, &record, maxBytes); err != nil {
			return err
		}
		out.Collections = append(out.Collections, record)
		return nil
	})
}

// Close joins read transactions before releasing its read-only descriptor.
func (r *Reader) Close() error { return r.store.Close() }
