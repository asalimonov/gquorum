package authoritystore

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
)

// MetadataAcquired reports whether this durable member has opened metadata.
// Rebind resets it only when installing a strictly newer member. Once acquired,
// missing metadata cannot authorize creating the same member again.
func (s *Store) MetadataAcquired(ctx context.Context) (bool, error) {
	var acquired bool
	err := s.readTransaction(ctx, func(tx *bolt.Tx) error {
		data := tx.Bucket([]byte("identity")).Get([]byte("metadata_acquired"))
		if data == nil {
			return nil
		}
		var recorded identityPin
		if err := decode(data, &recorded); err != nil {
			return err
		}
		if recorded != pin(s.identity) {
			return fault.ErrUnreadable
		}
		acquired = true
		return nil
	})
	return acquired, err
}

// MarkMetadataAcquired runs after the exact metadata pin is durable and before
// its engine is published or started. It is idempotent across retained
// restarts.
func (s *Store) MarkMetadataAcquired(ctx context.Context, identity topology.Identity) error {
	return s.writeTransaction(ctx, func(tx *bolt.Tx) error {
		if identity != s.identity {
			return fault.ErrIdentityMismatch
		}
		return put(tx.Bucket([]byte("identity")), []byte("metadata_acquired"), pin(identity))
	})
}
