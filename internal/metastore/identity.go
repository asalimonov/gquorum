package metastore

import (
	"github.com/asalimonov/gquorum/fault"
	bolt "go.etcd.io/bbolt"
)

// VerifyIdentity checks an already acquired pin without creating or repairing it.
func (s *Store) VerifyIdentity(expected Identity) error {
	return s.db.View(func(tx *bolt.Tx) error {
		stored, found, err := readIdentityChecked(tx)
		if err != nil {
			return err
		}
		if !found {
			return fault.ErrUnreadable
		}
		if stored != expected {
			return fault.ErrIdentityMismatch
		}
		return nil
	})
}
