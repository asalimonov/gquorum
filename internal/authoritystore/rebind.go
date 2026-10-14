package authoritystore

import (
	"context"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
)

type rebindRecord struct {
	Previous    identityPin `json:"previous"`
	Granted     identityPin `json:"granted"`
	AdmissionID string      `json:"admission_id"`
}

// VerifyRebind checks retained transition proof without mutating either
// identity or acquisition evidence. Receipts alone do not prove which successor
// was bound.
func (s *Store) VerifyRebind(ctx context.Context, previous, granted topology.Identity, admissionID string) error {
	return s.readTransaction(ctx, func(tx *bolt.Tx) error {
		if granted != s.identity {
			return fault.ErrIdentityMismatch
		}
		data := tx.Bucket([]byte("identity")).Get([]byte("readmission"))
		if data == nil {
			return fault.ErrOperatorAuthorization
		}
		var recorded rebindRecord
		if err := decode(data, &recorded); err != nil {
			return err
		}
		if recorded != (rebindRecord{pin(previous), pin(granted), admissionID}) {
			return fault.ErrIdentityMismatch
		}
		return nil
	})
}

// Rebind is reachable only after Node validates the readmission proof and joins
// the previous owner. It preserves all resource floors and effect evidence.
func (s *Store) Rebind(ctx context.Context, previous, granted topology.Identity, admissionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if previous.ClusterID != granted.ClusterID || previous.NodeName != granted.NodeName ||
		granted.MemberID <= previous.MemberID || !validIdentity(previous) || !validIdentity(granted) ||
		strings.TrimSpace(admissionID) == "" {
		return fault.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return fault.ErrStopped
	}
	proof := rebindRecord{pin(previous), pin(granted), admissionID}
	err := s.db.Update(func(tx *bolt.Tx) error {
		return rebindIdentity(ctx, tx, proof)
	})
	if err == nil {
		s.identity = granted
	}
	return err
}

func rebindIdentity(ctx context.Context, tx *bolt.Tx, proof rebindRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b := tx.Bucket([]byte("identity"))
	var current identityPin
	if err := decode(b.Get([]byte("current")), &current); err != nil {
		return err
	}
	if current == proof.Granted {
		var recorded rebindRecord
		if err := decode(b.Get([]byte("readmission")), &recorded); err != nil {
			return err
		}
		if recorded != proof {
			return fault.ErrIdentityMismatch
		}
		return nil
	}
	if current != proof.Previous {
		return fault.ErrIdentityMismatch
	}
	if err := put(b, []byte("readmission"), proof); err != nil {
		return err
	}
	if err := b.Delete([]byte("metadata_acquired")); err != nil {
		return err
	}
	return put(b, []byte("current"), proof.Granted)
}
