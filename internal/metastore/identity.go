package metastore

import (
	"github.com/asalimonov/gquorum/fault"
	bolt "go.etcd.io/bbolt"
)

// VerifyIdentity checks an already acquired pin without creating or repairing
// it.
func (s *Store) VerifyIdentity(expected Identity) error {
	return s.db.View(func(tx *bolt.Tx) error {
		stored, err := readIdentityChecked(tx)
		if err != nil {
			return err
		}
		if !stored.Found {
			return fault.ErrUnreadable
		}
		if stored.Identity != expected {
			return fault.ErrIdentityMismatch
		}
		return nil
	})
}
