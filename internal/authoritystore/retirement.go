package authoritystore

import (
	"bytes"
	"context"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
)

func retirementGrant(grant topology.AuthorityGrant) bool {
	return grant.Purpose == topology.AuthorityReadmit ||
		grant.Purpose == topology.AuthorityEffect && grant.Action != nil &&
			(grant.Action.Kind == topology.StepCleanupReplicaState ||
				grant.Action.Kind == topology.StepDeleteGroupState)
}

// CompletedRetirement reads the latest complete retirement for one exact local
// replica, including former members after Rebind. Newer fences or preparation
// effects cannot substitute for the original consumed retirement proof.
func (s *Store) CompletedRetirement(ctx context.Context, id storage.Identity) (EffectLookup, error) {
	var result EffectLookup
	err := s.readTransaction(ctx, func(tx *bolt.Tx) error {
		if _, err := s.localCollectionKey(id); err != nil {
			return err
		}
		var err error
		result, err = s.findCompletedRetirement(tx, id)
		return err
	})
	return result, err
}

func (s *Store) findCompletedRetirement(tx *bolt.Tx, id storage.Identity) (EffectLookup, error) {
	var result EffectLookup
	prefix := append(appendUint32Length(nil, len(id.GroupID)), id.GroupID...)
	cursor := tx.Bucket([]byte("effects")).Cursor()
	for key, data := cursor.Seek(prefix); bytes.HasPrefix(key, prefix); key, data = cursor.Next() {
		var record EffectRecord
		if err := decode(data, &record); err != nil {
			return EffectLookup{}, err
		}
		if record.State != effectComplete || record.Target.NodeName != id.NodeName ||
			record.Target.ReplicaID != id.ReplicaID || !retirementGrant(record.Grant) {
			continue
		}
		if err := s.validateStoredEffect(tx, key, data); err != nil {
			return EffectLookup{}, err
		}
		if !result.Found || record.Grant.Key.Revision > result.Record.Grant.Key.Revision {
			result.Record, result.Found = record, true
		}
	}
	return result, nil
}
