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

// LoadReplicaRecords inspects the closed old database during readmission. It
// does not open a writer, create a file, or clear evidence before retirement.
func LoadReplicaRecords(path string) (map[string]ReplicaRecord, error) {
	result := make(map[string]ReplicaRecord)
	err := withReadOnly(path, func(tx *bolt.Tx) error {
		if err := validateDiskSchema(tx); err != nil {
			return err
		}
		pin, pinned, err := readIdentityChecked(tx)
		if err != nil {
			return err
		}
		bucket := tx.Bucket(bucketLoadReplicas)
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(key, value []byte) error {
			if topology.ValidateLoadID(string(key)) != nil {
				return fault.ErrUnreadable
			}
			var record ReplicaRecord
			if err := json.Unmarshal(value, &record); err != nil {
				return fault.New(fault.Unreadable, "invalid retained replica record", err)
			}
			if !pinned || record.ReplicaID == 0 || record.NodeName != pin.NodeName || record.ClusterID != pin.ClusterID {
				return fault.ErrIdentityMismatch
			}
			result[string(key)] = record
			return nil
		})
	})
	return result, err
}

func (s *Store) RecordReplica(group string, record ReplicaRecord) error {
	return s.RecordReplicaCAS(group, record, 0)
}

// RecordReplicaCAS replaces only the exact previous local identity. Repeating
// the new identity is idempotent and preserves its original audit fields.
func (s *Store) RecordReplicaCAS(group string, record ReplicaRecord, previous uint64) error {
	if err := topology.ValidateLoadID(group); err != nil {
		return err
	}
	if record.ReplicaID == 0 {
		return fault.New(fault.Invalid, "replica ID must be positive", nil)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		id, pinned, err := readIdentityChecked(tx)
		if err != nil {
			return err
		}
		if !pinned || record.NodeName != id.NodeName || record.ClusterID != id.ClusterID {
			return fault.New(fault.IdentityMismatch, "replica ledger identity differs from meta pin", nil)
		}
		b := tx.Bucket(bucketLoadReplicas)
		if raw := b.Get([]byte(group)); raw != nil {
			var old ReplicaRecord
			if err := json.Unmarshal(raw, &old); err != nil {
				return err
			}
			if old.NodeName != record.NodeName || old.ClusterID != record.ClusterID || old.ReplicaID == 0 {
				return fault.New(fault.IdentityMismatch, "replica record already belongs to another identity", nil)
			}
			if old.ReplicaID == record.ReplicaID {
				return nil
			}
			if previous == 0 || old.ReplicaID != previous {
				return fault.New(fault.IdentityMismatch, "replica replacement differs from expected identity", nil)
			}
		} else if previous != 0 {
			return fault.New(fault.IdentityMismatch, "expected previous replica record is missing", nil)
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
		id, pinned, err := readIdentityChecked(tx)
		if err != nil {
			return err
		}
		if !pinned || record.ReplicaID == 0 || record.NodeName != id.NodeName || record.ClusterID != id.ClusterID {
			return fault.New(fault.IdentityMismatch, "replica ledger identity differs from meta pin", nil)
		}
		found = true
		return nil
	})
	return
}

// ForgetReplica refuses to erase the ledger of a replacement replica.
func (s *Store) ForgetReplica(group string, expected uint64) error {
	if expected == 0 {
		return fault.New(fault.Invalid, "expected replica ID must be positive", nil)
	}
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
			id, pinned, err := readIdentityChecked(tx)
			if err != nil {
				return err
			}
			if !pinned || record.NodeName != id.NodeName || record.ClusterID != id.ClusterID {
				return fault.ErrIdentityMismatch
			}
			if record.ReplicaID != expected {
				return fault.New(fault.IdentityMismatch, "refusing to erase replacement replica", nil)
			}
		}
		return b.Delete([]byte(group))
	})
}
