package authoritystore

import (
	"context"
	"math"
	"reflect"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	bolt "go.etcd.io/bbolt"
)

const collectionBucket = "collection"

// PrepareCollection pins proof before destructive storage work. New callers
// supply prepared state and zero Sequence; retries may supply the assigned one.
// Repeating preparation after completion returns the original collected row.
func (s *Store) PrepareCollection(ctx context.Context, record CollectionRecord) (CollectionRecord, error) {
	var result CollectionRecord
	err := s.writeTransaction(ctx, func(tx *bolt.Tx) error {
		var err error
		result, err = s.prepareCollectionTx(tx, record)
		return err
	})
	return result, err
}

func (s *Store) prepareCollectionTx(tx *bolt.Tx, record CollectionRecord) (CollectionRecord, error) {
	owned := record.Clone()
	owned.Sequence = max(owned.Sequence, 1) // Validate before assigning durable sequence.
	if owned.State != collectionPrepared || owned.CollectedAt != nil {
		return CollectionRecord{}, fault.ErrInvalid
	}
	if err := owned.Validate(s.identity); err != nil {
		return CollectionRecord{}, err
	}
	key, _ := collectionKey(owned.Retired.Identity)
	bucket := tx.Bucket([]byte(collectionBucket))
	if data := bucket.Get(key); data != nil {
		old, err := s.decodeCollection(bucket, key, data)
		if err != nil {
			return CollectionRecord{}, err
		}
		if record.Sequence != 0 && record.Sequence != old.Sequence || !sameCollectionProof(old, owned) {
			return CollectionRecord{}, fault.ErrIdentityMismatch
		}
		return old, nil
	}
	if record.Sequence != 0 {
		return CollectionRecord{}, fault.ErrIdentityMismatch
	}
	if bucket.Sequence() == math.MaxUint64 {
		return CollectionRecord{}, fault.ErrUnavailable
	}
	var err error
	owned.Sequence, err = bucket.NextSequence()
	if err != nil {
		return CollectionRecord{}, err
	}
	if err := put(bucket, key, owned); err != nil {
		return CollectionRecord{}, err
	}
	return owned, nil
}

func sameCollectionProof(a, b CollectionRecord) bool {
	return a.Retired.Identity == b.Retired.Identity && a.Retired.Token == b.Retired.Token && a.MemberID == b.MemberID &&
		a.Retired.QuarantinedAt.Equal(b.Retired.QuarantinedAt) && a.Grant.Equal(b.Grant) &&
		reflect.DeepEqual(a.Retirement, b.Retirement)
}

// CompleteCollection records successful physical collection for the exact
// prepared sequence. A duplicate returns the first timestamp, never a new fact.
func (s *Store) CompleteCollection(ctx context.Context, id storage.Identity,
	sequence uint64, at time.Time) (CollectionReceipt, error) {
	if sequence == 0 || at.IsZero() {
		return CollectionReceipt{}, fault.ErrInvalid
	}
	var result CollectionReceipt
	err := s.writeTransaction(ctx, func(tx *bolt.Tx) error {
		key, err := s.localCollectionKey(id)
		if err != nil {
			return err
		}
		bucket := tx.Bucket([]byte(collectionBucket))
		data := bucket.Get(key)
		if data == nil {
			return fault.ErrHistoryUnavailable
		}
		record, err := s.decodeCollection(bucket, key, data)
		if err != nil {
			return err
		}
		if record.Sequence != sequence {
			return fault.ErrIdentityMismatch
		}
		if record.State == collectionPrepared {
			record.State, record.CollectedAt = collectionCollected, &at
			if err := put(bucket, key, record); err != nil {
				return err
			}
		}
		result = collectionReceipt(record)
		return nil
	})
	return result, err
}

func collectionReceipt(record CollectionRecord) CollectionReceipt {
	return CollectionReceipt{Identity: record.Retired.Identity, MemberID: record.MemberID,
		RetirementKey: record.Grant.Key, Token: record.Retired.Token,
		LocalSequence: record.Sequence, CollectedAt: *record.CollectedAt}
}

// RemoveCollection releases only an exact collected row after the caller has
// verified reporting and storage-proof release. Missing rows are repeatable.
func (s *Store) RemoveCollection(ctx context.Context, receipt CollectionReceipt) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	return s.writeTransaction(ctx, func(tx *bolt.Tx) error {
		key, err := s.localCollectionKey(receipt.Identity)
		if err != nil {
			return err
		}
		if receipt.MemberID > s.identity.MemberID {
			return fault.ErrIdentityMismatch
		}
		bucket := tx.Bucket([]byte(collectionBucket))
		data := bucket.Get(key)
		if data == nil {
			return nil
		}
		record, err := s.decodeCollection(bucket, key, data)
		if err != nil {
			return err
		}
		if record.State != collectionCollected || !collectionReceipt(record).Equal(receipt) {
			return fault.ErrIdentityMismatch
		}
		return bucket.Delete(key)
	})
}

func (s *Store) localCollectionKey(id storage.Identity) ([]byte, error) {
	if id.ClusterID != s.identity.ClusterID || id.NodeName != s.identity.NodeName {
		return nil, fault.ErrIdentityMismatch
	}
	return collectionKey(id)
}
