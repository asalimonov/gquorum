package authoritystore

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	bolt "go.etcd.io/bbolt"
)

const (
	defaultCollectionPageLimit = 100
	maxCollectionPageLimit     = 1000
)

// CollectionPage owns bounded journal rows in stable identity-key order.
// Next is an exclusive cursor, valid across removal and journal rebind.
type CollectionPage struct {
	Records []CollectionRecord
	Next    string
}

// CollectionLookup distinguishes an absent journal row from either durable state.
type CollectionLookup struct {
	Record CollectionRecord
	Found  bool
}

// Collections scans one transaction, not a snapshot spanning pages. Concurrent
// insertions behind the cursor are found on the next scan from the beginning.
// Limit zero selects 100 rows; the maximum is 1000.
func (s *Store) Collections(ctx context.Context, after string, limit int) (CollectionPage, error) {
	if limit < 0 || limit > maxCollectionPageLimit {
		return CollectionPage{}, fault.ErrInvalid
	}
	pageLimit := limit
	if pageLimit == 0 {
		pageLimit = defaultCollectionPageLimit
	}
	var result CollectionPage
	err := s.readTransaction(ctx, func(tx *bolt.Tx) error {
		start, err := s.collectionCursorKey(after)
		if err != nil {
			return err
		}
		result, err = s.readCollectionPage(tx.Bucket([]byte(collectionBucket)), start, pageLimit)
		return err
	})
	if err != nil {
		return CollectionPage{}, err
	}
	return result, nil
}

func (s *Store) readCollectionPage(bucket *bolt.Bucket, start []byte, limit int) (CollectionPage, error) {
	cursor := bucket.Cursor()
	key, data := cursor.First()
	if start != nil {
		key, data = cursor.Seek(start)
		if bytes.Equal(key, start) {
			key, data = cursor.Next()
		}
	}
	var page CollectionPage
	for key != nil && len(page.Records) < limit {
		record, err := s.decodeCollection(bucket, key, data)
		if err != nil {
			return CollectionPage{}, err
		}
		page.Records = append(page.Records, record)
		key, data = cursor.Next()
	}
	if key != nil {
		last := page.Records[len(page.Records)-1].Retired.Identity
		encoded, err := json.Marshal(last)
		if err != nil {
			return CollectionPage{}, err
		}
		page.Next = base64.RawURLEncoding.EncodeToString(encoded)
	}
	return page, nil
}

func (s *Store) collectionCursorKey(after string) ([]byte, error) {
	if after == "" {
		return nil, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(after)
	if err != nil {
		return nil, fault.ErrInvalid
	}
	var id storage.Identity
	if err := decode(data, &id); err != nil {
		return nil, fault.ErrInvalid
	}
	return s.localCollectionKey(id)
}

func (s *Store) decodeCollection(bucket *bolt.Bucket, key, data []byte) (CollectionRecord, error) {
	var record CollectionRecord
	if err := decode(data, &record); err != nil {
		return CollectionRecord{}, err
	}
	expected, err := collectionKey(record.Retired.Identity)
	if err != nil || !bytes.Equal(key, expected) || record.Sequence > bucket.Sequence() ||
		record.Validate(s.identity) != nil {
		return CollectionRecord{}, fault.ErrUnreadable
	}
	return record, nil
}

func (s *Store) validateCollections(tx *bolt.Tx) error {
	bucket := tx.Bucket([]byte(collectionBucket))
	sequences := map[uint64]bool{}
	return bucket.ForEach(func(key, data []byte) error {
		record, err := s.decodeCollection(bucket, key, data)
		if err != nil {
			return err
		}
		if sequences[record.Sequence] {
			return fault.ErrUnreadable
		}
		sequences[record.Sequence] = true
		return nil
	})
}
