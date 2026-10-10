package metastore

import (
	"encoding/json"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
)

type ReplicaRecord struct {
	NodeName  string    `json:"node_name"`
	ClusterID string    `json:"cluster_id"`
	ReplicaID uint64    `json:"replica_id"`
	Origin    string    `json:"origin"`
	CreatedAt time.Time `json:"created_at"`
}

func (s *Store) RecordReplica(group string, record ReplicaRecord) error {
	if err := topology.ValidateLoadID(group); err != nil {
		return err
	}
	if record.ReplicaID == 0 {
		return fault.New(fault.Invalid, "replica ID must be positive", nil)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		id, pinned := readIdentity(tx)
		if !pinned || record.NodeName != id.NodeName || record.ClusterID != id.ClusterID {
			return fault.New(fault.IdentityMismatch, "replica ledger identity differs from meta pin", nil)
		}
		b := tx.Bucket(bucketLoadReplicas)
		if raw := b.Get([]byte(group)); raw != nil {
			var old ReplicaRecord
			if err := json.Unmarshal(raw, &old); err != nil {
				return err
			}
			if old.NodeName != record.NodeName || old.ClusterID != record.ClusterID || old.ReplicaID != record.ReplicaID {
				return fault.New(fault.IdentityMismatch, "replica record already belongs to another identity", nil)
			}
			return nil
		}
		data, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return b.Put([]byte(group), data)
	})
}

func (s *Store) LookupReplica(group string) (record ReplicaRecord, found bool, err error) {
	if err := topology.ValidateLoadID(group); err != nil {
		return ReplicaRecord{}, false, err
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketLoadReplicas).Get([]byte(group))
		if raw == nil {
			return nil
		}
		if err := json.Unmarshal(raw, &record); err != nil {
			return err
		}
		id, pinned := readIdentity(tx)
		if !pinned || record.NodeName != id.NodeName || record.ClusterID != id.ClusterID {
			return fault.New(fault.IdentityMismatch, "replica ledger identity differs from meta pin", nil)
		}
		found = true
		return nil
	})
	return
}

// ForgetReplica refuses to erase the ledger of a replacement replica.
func (s *Store) ForgetReplica(group string, expected uint64) error {
	if err := topology.ValidateLoadID(group); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLoadReplicas)
		if raw := b.Get([]byte(group)); raw != nil {
			var record ReplicaRecord
			if err := json.Unmarshal(raw, &record); err != nil {
				return err
			}
			if record.ReplicaID != expected {
				return fault.New(fault.IdentityMismatch, "refusing to erase replacement replica", nil)
			}
		}
		return b.Delete([]byte(group))
	})
}
