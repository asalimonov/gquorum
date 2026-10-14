package authoritystore

import (
	"errors"
	"os"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
)

// LoadIdentity inspects a closed database without creating or rewriting it.
// The process incarnation is intentionally absent from the durable identity.
func LoadIdentity(path string) (topology.IdentityLookup, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return topology.IdentityLookup{}, nil
	} else if err != nil {
		return topology.IdentityLookup{}, err
	}
	db, err := bolt.Open(path, fileMode, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return topology.IdentityLookup{Found: true}, fault.New(fault.Unreadable, "inspect authority identity", err)
	}
	defer db.Close()
	var identity topology.Identity
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("identity"))
		if b == nil {
			return fault.ErrUnreadable
		}
		var pinned identityPin
		if err := decode(b.Get([]byte("current")), &pinned); err != nil {
			return err
		}
		identity = topology.Identity{
			ClusterID:  pinned.ClusterID,
			NodeName:   pinned.NodeName,
			MemberID:   pinned.MemberID,
			InstanceID: "inspection",
		}
		if !validIdentity(identity) {
			return fault.ErrUnreadable
		}
		return (&Store{identity: identity}).validate(tx)
	})
	identity.InstanceID = ""
	return topology.IdentityLookup{Identity: identity, Found: true}, err
}

// Identity returns the store's current node and process identity.
func (s *Store) Identity() topology.Identity {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.identity
}
