package authoritystore

import (
	"context"
	"encoding/binary"
	"math"
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
)

const (
	effectRunning  = "running"
	effectFenced   = "fenced"
	effectComplete = "complete"
)

// EffectRecord retains an exact host grant's execution state and optional
// completion receipt.
type EffectRecord struct {
	Grant    topology.AuthorityGrant  `json:"grant"`
	Target   topology.AuthorityTarget `json:"target"`
	Sequence uint64                   `json:"sequence"`
	State    string                   `json:"state"`
	Receipt  *topology.EffectReceipt  `json:"receipt,omitempty"`
}

func effectKey(key topology.AuthorityKey, target topology.AuthorityTarget) ([]byte, error) {
	if len(key.GroupID) > math.MaxUint32 || len(target.NodeName) > math.MaxUint32 {
		return nil, fault.ErrInvalid
	}
	b := appendUint32Length(nil, len(key.GroupID))
	b = append(b, key.GroupID...)
	b = binary.BigEndian.AppendUint64(b, key.Revision)
	b = appendUint32Length(b, len(target.NodeName))
	b = append(b, target.NodeName...)
	return binary.BigEndian.AppendUint64(b, target.ReplicaID), nil
}

func appendUint32Length(data []byte, length int) []byte {
	return append(data, byte(length>>24&0xff), byte(length>>16&0xff), byte(length>>8&0xff), byte(length&0xff))
}
func (s *Store) validateEffect(r EffectRecord) error {
	if r.Grant.Purpose == topology.AuthorityFence || r.Sequence == 0 || r.Target.NodeName != s.identity.NodeName ||
		s.hostGrant(r.Grant) != nil || !slices.Contains(r.Grant.Targets, r.Target) {
		return fault.ErrInvalid
	}
	proof := topology.EffectReceipt{
		Key:           r.Grant.Key,
		Resource:      resource(r.Grant.Key.GroupID, s.identity.NodeName),
		Action:        r.Grant.Action,
		AdmissionID:   r.Grant.AdmissionID,
		Target:        r.Target,
		Outcome:       topology.EffectApplied,
		LocalSequence: r.Sequence,
	}
	if err := proof.Validate(r.Grant); err != nil {
		return err
	}
	switch r.State {
	case effectRunning, effectFenced:
		if r.Receipt != nil {
			return fault.ErrUnreadable
		}
	case effectComplete:
		if r.Receipt == nil || r.Receipt.Target != r.Target ||
			r.Receipt.LocalSequence != r.Sequence ||
			r.Receipt.Resource != resource(r.Grant.Key.GroupID, s.identity.NodeName) {
			return fault.ErrUnreadable
		}
		return r.Receipt.Validate(r.Grant)
	default:
		return fault.ErrUnreadable
	}
	return nil
}

// BeginEffect admits a host effect under its completed current fence or returns
// its prior receipt.
func (s *Store) BeginEffect(ctx context.Context, grant topology.AuthorityGrant,
	target topology.AuthorityTarget) (receipt *topology.EffectReceipt, err error) {
	err = s.writeTransaction(ctx, func(tx *bolt.Tx) error {
		var txErr error
		receipt, txErr = s.beginEffectTx(tx, grant, target)
		return txErr
	})
	return receipt, err
}

// FinishEffect also accepts an older owned operation while a newer fence waits
// for it. Its original sequence remains pinned in the running record.
func (s *Store) FinishEffect(ctx context.Context, receipt topology.EffectReceipt) error {
	return s.writeTransaction(ctx, func(tx *bolt.Tx) error {
		return s.finishEffectTx(tx, receipt)
	})
}

// FenceEffect retains an uncertain outcome after the host has joined its owner.
// It grants no completion receipt and never removes application/storage
// evidence. The caller must own the group action gate and join all delayed
// callbacks before calling this method. Only a strictly newer installed fence
// can end the intent.
func (s *Store) FenceEffect(ctx context.Context, record EffectRecord, boundary topology.AuthorityKey) error {
	return s.writeTransaction(ctx, func(tx *bolt.Tx) error {
		return s.fenceEffectTx(tx, record, boundary)
	})
}

// Running returns retained running effects for a group, or all groups when the
// name is empty.
func (s *Store) Running(ctx context.Context, group string) (records []EffectRecord, err error) {
	err = s.readTransaction(ctx, func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("effects")).ForEach(func(_, data []byte) error {
			var r EffectRecord
			if err := decode(data, &r); err != nil {
				return err
			}
			if r.State == effectRunning && (group == "" || r.Grant.Key.GroupID == group) {
				records = append(records, r)
			}
			return nil
		})
	})
	return records, err
}

// Receipt reads historical consumption without authorizing execution. In
// particular, a readmission receipt must remain retrievable after Rebind
// changes
// the local member pin. BeginEffect continues to reject that former identity.
func (s *Store) Receipt(ctx context.Context, grant topology.AuthorityGrant,
	target topology.AuthorityTarget) (result ReceiptLookup, err error) {
	err = s.readTransaction(ctx, func(tx *bolt.Tx) error {
		return s.readReceiptTx(tx, grant, target, &result)
	})
	return result, err
}

// MetaParticipationStopped retains an uncertain or completed stop across
// process
// crashes and newer administrative floors. Only an exact readmission to a fresh
// member may permit participation again; a new process incarnation cannot.
func (s *Store) MetaParticipationStopped(ctx context.Context) (stopped bool, err error) {
	err = s.readTransaction(ctx, func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("effects")).ForEach(func(_, data []byte) error {
			var record EffectRecord
			if err := decode(data, &record); err != nil {
				return err
			}
			if record.Grant.Key.GroupID == topology.MetaGroupID && record.Target.MemberID == s.identity.MemberID &&
				record.Grant.Action != nil && record.Grant.Action.Kind == topology.StepStopReplica {
				stopped = true
			}
			return nil
		})
	})
	return stopped, err
}

// CompletedMetaStop returns this member's newest durable stop receipt even when
// a later fence has replaced its current floor. It never borrows a former
// member's completion after readmission rebind.
func (s *Store) CompletedMetaStop(ctx context.Context) (result EffectLookup, err error) {
	err = s.readTransaction(ctx, func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("effects")).ForEach(func(_, data []byte) error {
			var record EffectRecord
			if err := decode(data, &record); err != nil {
				return err
			}
			if record.Grant.Key.GroupID != topology.MetaGroupID || record.Target.MemberID != s.identity.MemberID ||
				record.Grant.Action == nil || record.Grant.Action.Kind != topology.StepStopReplica ||
				record.State != effectComplete {
				return nil
			}
			if err := s.validateEffect(record); err != nil {
				return err
			}
			if !result.Found || record.Grant.Key.Revision > result.Record.Grant.Key.Revision {
				result.Record, result.Found = record, true // decoded bytes own both grant and receipt
			}
			return nil
		})
	})
	return result, err
}

func (s *Store) beginEffectTx(tx *bolt.Tx, grant topology.AuthorityGrant,
	target topology.AuthorityTarget) (*topology.EffectReceipt, error) {
	if err := s.currentMemberGrant(grant); err != nil {
		return nil, err
	}
	floor, err := current(tx, grant.Key.GroupID)
	if err != nil {
		return nil, err
	}
	if !floor.Found || !floor.Record.Grant.Equal(grant) {
		return nil, fault.ErrAuthorityFenced
	}
	if !floor.Record.Complete {
		return nil, fault.ErrAuthorityPending
	}
	if target.MemberID != s.identity.MemberID {
		return nil, fault.ErrIdentityMismatch
	}
	r := EffectRecord{Grant: grant.Clone(), Target: target, Sequence: floor.Record.Sequence, State: effectRunning}
	if err := s.validateEffect(r); err != nil {
		return nil, err
	}
	key, err := effectKey(grant.Key, target)
	if err != nil {
		return nil, err
	}
	b := tx.Bucket([]byte("effects"))
	if data := b.Get(key); data != nil {
		return completedEffectReceipt(data, r)
	}
	return nil, put(b, key, r)
}

func completedEffectReceipt(data []byte, record EffectRecord) (*topology.EffectReceipt, error) {
	var old EffectRecord
	if err := decode(data, &old); err != nil {
		return nil, err
	}
	if old.Target != record.Target || old.Sequence != record.Sequence || !old.Grant.Equal(record.Grant) {
		return nil, fault.ErrIdentityMismatch
	}
	if old.State != effectComplete {
		return nil, fault.ErrAuthorityPending
	}
	if old.Receipt == nil {
		return nil, fault.ErrUnreadable
	}
	receipt := old.Receipt.Clone()
	return &receipt, nil
}

func (s *Store) finishEffectTx(tx *bolt.Tx, receipt topology.EffectReceipt) error {
	key, err := effectKey(receipt.Key, receipt.Target)
	if err != nil {
		return err
	}
	b := tx.Bucket([]byte("effects"))
	if b.Get(key) == nil {
		return fault.ErrAuthorityFenced
	}
	var old EffectRecord
	if err := decode(b.Get(key), &old); err != nil {
		return err
	}
	r := old
	r.State = effectComplete
	owned := receipt.Clone()
	r.Receipt = &owned
	if err := s.validateEffect(r); err != nil {
		return err
	}
	if old.State == effectComplete {
		if !reflect.DeepEqual(old.Receipt, r.Receipt) {
			return fault.ErrIdentityMismatch
		}
		return nil
	}
	if old.State != effectRunning {
		return fault.ErrAuthorityFenced
	}
	floor, err := current(tx, receipt.Key.GroupID)
	if err != nil {
		return err
	}
	if !floor.Found || floor.Record.Grant.Key.Revision < receipt.Key.Revision ||
		floor.Record.Grant.Key.Revision > receipt.Key.Revision && floor.Record.Complete {
		return fault.ErrAuthorityFenced
	}
	return put(b, key, r)
}

func (s *Store) fenceEffectTx(tx *bolt.Tx, record EffectRecord, boundary topology.AuthorityKey) error {
	if record.State != effectRunning || s.validateEffect(record) != nil {
		return fault.ErrInvalid
	}
	floor, err := current(tx, record.Grant.Key.GroupID)
	if err != nil {
		return err
	}
	if !floor.Found || floor.Record.Grant.Key != boundary || boundary.Revision <= record.Grant.Key.Revision {
		return fault.ErrAuthorityFenced
	}
	key, err := effectKey(record.Grant.Key, record.Target)
	if err != nil {
		return err
	}
	b := tx.Bucket([]byte("effects"))
	var old EffectRecord
	if err := decode(b.Get(key), &old); err != nil {
		return err
	}
	if old.Target != record.Target || old.Sequence != record.Sequence || !old.Grant.Equal(record.Grant) {
		return fault.ErrIdentityMismatch
	}
	if old.State == effectComplete || old.State == effectFenced {
		return nil
	}
	if old.State != effectRunning || floor.Record.Complete {
		return fault.ErrUnreadable
	}
	old.State = effectFenced
	return put(b, key, old)
}

func (s *Store) readReceiptTx(tx *bolt.Tx, grant topology.AuthorityGrant,
	target topology.AuthorityTarget, result *ReceiptLookup) error {
	if err := s.hostGrant(grant); err != nil {
		return err
	}
	if target.NodeName != s.identity.NodeName || !slices.Contains(grant.Targets, target) {
		return fault.ErrIdentityMismatch
	}
	key, err := effectKey(grant.Key, target)
	if err != nil {
		return err
	}
	data := tx.Bucket([]byte("effects")).Get(key)
	if data == nil {
		return nil
	}
	var record EffectRecord
	if err := decode(data, &record); err != nil {
		return err
	}
	if record.Target != target || !record.Grant.Equal(grant) {
		return fault.ErrIdentityMismatch
	}
	if err := s.validateEffect(record); err != nil {
		return err
	}
	if record.Receipt != nil {
		result.Receipt, result.Found = record.Receipt.Clone(), true
	}
	return nil
}
