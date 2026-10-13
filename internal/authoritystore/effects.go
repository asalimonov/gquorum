package authoritystore

import (
	"context"
	"encoding/binary"
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
)

type EffectRecord struct {
	Grant    topology.AuthorityGrant  `json:"grant"`
	Target   topology.AuthorityTarget `json:"target"`
	Sequence uint64                   `json:"sequence"`
	State    string                   `json:"state"`
	Receipt  *topology.EffectReceipt  `json:"receipt,omitempty"`
}

func effectKey(key topology.AuthorityKey, target topology.AuthorityTarget) []byte {
	b := binary.BigEndian.AppendUint32(nil, uint32(len(key.GroupID)))
	b = append(b, key.GroupID...)
	b = binary.BigEndian.AppendUint64(b, key.Revision)
	b = binary.BigEndian.AppendUint32(b, uint32(len(target.NodeName)))
	b = append(b, target.NodeName...)
	return binary.BigEndian.AppendUint64(b, target.ReplicaID)
}
func (s *Store) validateEffect(r EffectRecord) error {
	if s.hostGrant(r.Grant, false) != nil || r.Grant.Purpose == topology.AuthorityFence || r.Sequence == 0 ||
		r.Target.NodeName != s.identity.NodeName || !slices.Contains(r.Grant.Targets, r.Target) {
		return fault.ErrInvalid
	}
	proof := topology.EffectReceipt{Key: r.Grant.Key, Resource: resource(r.Grant.Key.GroupID, s.identity.NodeName), Action: r.Grant.Action, AdmissionID: r.Grant.AdmissionID, Target: r.Target, Outcome: topology.EffectApplied, LocalSequence: r.Sequence}
	if err := proof.Validate(r.Grant); err != nil {
		return err
	}
	switch r.State {
	case "running", "fenced":
		if r.Receipt != nil {
			return fault.ErrUnreadable
		}
	case "complete":
		if r.Receipt == nil || r.Receipt.Target != r.Target || r.Receipt.Resource != resource(r.Grant.Key.GroupID, s.identity.NodeName) || r.Receipt.LocalSequence != r.Sequence {
			return fault.ErrUnreadable
		}
		return r.Receipt.Validate(r.Grant)
	default:
		return fault.ErrUnreadable
	}
	return nil
}
func (s *Store) BeginEffect(ctx context.Context, grant topology.AuthorityGrant, target topology.AuthorityTarget) (receipt *topology.EffectReceipt, err error) {
	err = s.transaction(ctx, true, func(tx *bolt.Tx) error {
		if err := s.hostGrant(grant, true); err != nil {
			return err
		}
		floor, exists, err := current(tx, grant.Key.GroupID)
		if err != nil {
			return err
		}
		if !exists || !floor.Grant.Equal(grant) {
			return fault.ErrAuthorityFenced
		}
		if !floor.Complete {
			return fault.ErrAuthorityPending
		}
		if target.MemberID != s.identity.MemberID {
			return fault.ErrIdentityMismatch
		}
		r := EffectRecord{Grant: grant.Clone(), Target: target, Sequence: floor.Sequence, State: "running"}
		if err := s.validateEffect(r); err != nil {
			return err
		}
		key := effectKey(grant.Key, target)
		b := tx.Bucket([]byte("effects"))
		if data := b.Get(key); data != nil {
			var old EffectRecord
			if err := decode(data, &old); err != nil {
				return err
			}
			if !old.Grant.Equal(grant) || old.Target != target || old.Sequence != floor.Sequence {
				return fault.ErrIdentityMismatch
			}
			if old.State != "complete" {
				return fault.ErrAuthorityPending
			}
			if old.Receipt == nil {
				return fault.ErrUnreadable
			}
			r := old.Receipt.Clone()
			receipt = &r
			return nil
		}
		return put(b, key, r)
	})
	return
}

// FinishEffect also accepts an older owned operation while a newer fence waits
// for it. Its original sequence remains pinned in the running record.
func (s *Store) FinishEffect(ctx context.Context, receipt topology.EffectReceipt) error {
	return s.transaction(ctx, true, func(tx *bolt.Tx) error {
		key := effectKey(receipt.Key, receipt.Target)
		b := tx.Bucket([]byte("effects"))
		if b.Get(key) == nil {
			return fault.ErrAuthorityFenced
		}
		var old EffectRecord
		if err := decode(b.Get(key), &old); err != nil {
			return err
		}
		r := old
		r.State = "complete"
		owned := receipt.Clone()
		r.Receipt = &owned
		if err := s.validateEffect(r); err != nil {
			return err
		}
		if old.State == "complete" {
			if !reflect.DeepEqual(old.Receipt, r.Receipt) {
				return fault.ErrIdentityMismatch
			}
			return nil
		}
		if old.State != "running" {
			return fault.ErrAuthorityFenced
		}
		floor, exists, err := current(tx, receipt.Key.GroupID)
		if err != nil {
			return err
		}
		if !exists || floor.Grant.Key.Revision < receipt.Key.Revision || floor.Grant.Key.Revision > receipt.Key.Revision && floor.Complete {
			return fault.ErrAuthorityFenced
		}
		return put(b, key, r)
	})
}

// FenceEffect retains an uncertain outcome after the host has joined its owner.
// It grants no completion receipt and never removes application/storage evidence.
// The caller must own the group action gate and join all delayed callbacks before
// calling this method. Only a strictly newer installed fence can end the intent.
func (s *Store) FenceEffect(ctx context.Context, record EffectRecord, boundary topology.AuthorityKey) error {
	return s.transaction(ctx, true, func(tx *bolt.Tx) error {
		if record.State != "running" || s.validateEffect(record) != nil {
			return fault.ErrInvalid
		}
		floor, exists, err := current(tx, record.Grant.Key.GroupID)
		if err != nil {
			return err
		}
		if !exists || floor.Grant.Key != boundary || boundary.Revision <= record.Grant.Key.Revision {
			return fault.ErrAuthorityFenced
		}
		key := effectKey(record.Grant.Key, record.Target)
		b := tx.Bucket([]byte("effects"))
		var old EffectRecord
		if err := decode(b.Get(key), &old); err != nil {
			return err
		}
		if !old.Grant.Equal(record.Grant) || old.Target != record.Target || old.Sequence != record.Sequence {
			return fault.ErrIdentityMismatch
		}
		if old.State == "complete" || old.State == "fenced" {
			return nil
		}
		if old.State != "running" || floor.Complete {
			return fault.ErrUnreadable
		}
		old.State = "fenced"
		return put(b, key, old)
	})
}
func (s *Store) Running(ctx context.Context, group string) (records []EffectRecord, err error) {
	err = s.transaction(ctx, false, func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("effects")).ForEach(func(_, data []byte) error {
			var r EffectRecord
			if err := decode(data, &r); err != nil {
				return err
			}
			if r.State == "running" && (group == "" || r.Grant.Key.GroupID == group) {
				records = append(records, r)
			}
			return nil
		})
	})
	return
}

// Receipt reads historical consumption without authorizing execution. In
// particular, a readmission receipt must remain retrievable after Rebind changes
// the local member pin. BeginEffect continues to reject that former identity.
func (s *Store) Receipt(ctx context.Context, grant topology.AuthorityGrant, target topology.AuthorityTarget) (receipt topology.EffectReceipt, found bool, err error) {
	err = s.transaction(ctx, false, func(tx *bolt.Tx) error {
		if err := s.hostGrant(grant, false); err != nil {
			return err
		}
		if !slices.Contains(grant.Targets, target) || target.NodeName != s.identity.NodeName {
			return fault.ErrIdentityMismatch
		}
		data := tx.Bucket([]byte("effects")).Get(effectKey(grant.Key, target))
		if data == nil {
			return nil
		}
		var record EffectRecord
		if err := decode(data, &record); err != nil {
			return err
		}
		if !record.Grant.Equal(grant) || record.Target != target {
			return fault.ErrIdentityMismatch
		}
		if err := s.validateEffect(record); err != nil {
			return err
		}
		if record.Receipt != nil {
			receipt, found = record.Receipt.Clone(), true
		}
		return nil
	})
	return
}

// MetaParticipationStopped retains an uncertain or completed stop across process
// crashes and newer administrative floors. Only an exact readmission to a fresh
// member may permit participation again; a new process incarnation cannot.
func (s *Store) MetaParticipationStopped(ctx context.Context) (stopped bool, err error) {
	err = s.transaction(ctx, false, func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("effects")).ForEach(func(_, data []byte) error {
			var record EffectRecord
			if err := decode(data, &record); err != nil {
				return err
			}
			if record.Grant.Key.GroupID == topology.MetaGroupID && record.Target.MemberID == s.identity.MemberID && record.Grant.Action != nil && record.Grant.Action.Kind == topology.StepStopReplica {
				stopped = true
			}
			return nil
		})
	})
	return
}

// CompletedMetaStop returns this member's newest durable stop receipt even when
// a later fence has replaced its current floor. It never borrows a former
// member's completion after readmission rebind.
func (s *Store) CompletedMetaStop(ctx context.Context) (result EffectRecord, found bool, err error) {
	err = s.transaction(ctx, false, func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("effects")).ForEach(func(_, data []byte) error {
			var record EffectRecord
			if err := decode(data, &record); err != nil {
				return err
			}
			if record.Grant.Key.GroupID != topology.MetaGroupID || record.Target.MemberID != s.identity.MemberID || record.Grant.Action == nil || record.Grant.Action.Kind != topology.StepStopReplica || record.State != "complete" {
				return nil
			}
			if err := s.validateEffect(record); err != nil {
				return err
			}
			if !found || record.Grant.Key.Revision > result.Grant.Key.Revision {
				result, found = record, true // decoded bytes own both grant and receipt
			}
			return nil
		})
	})
	return
}
