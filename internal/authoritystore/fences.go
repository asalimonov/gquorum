package authoritystore

import (
	"context"
	"math"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
)

// FenceRecord retains a host's current grant revision and local fence
// completion state.
type FenceRecord struct {
	Grant    topology.AuthorityGrant `json:"grant"`
	Sequence uint64                  `json:"sequence"`
	Complete bool                    `json:"complete"`
}

func resource(group, node string) topology.AuthorityResource {
	return topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: group, NodeName: node}
}
func (s *Store) hostGrant(g topology.AuthorityGrant) error {
	if err := g.Validate(); err != nil {
		return err
	}
	if g.Key.ClusterID != s.identity.ClusterID ||
		!slices.Contains(g.Resources, resource(g.Key.GroupID, s.identity.NodeName)) ||
		!slices.ContainsFunc(g.Targets, func(t topology.AuthorityTarget) bool {
			return t.NodeName == s.identity.NodeName
		}) {
		return fault.ErrIdentityMismatch
	}
	return nil
}

func (s *Store) currentMemberGrant(g topology.AuthorityGrant) error {
	if err := s.hostGrant(g); err != nil {
		return err
	}
	if !slices.ContainsFunc(g.Targets, func(t topology.AuthorityTarget) bool {
		return t.NodeName == s.identity.NodeName && t.MemberID == s.identity.MemberID
	}) {
		return fault.ErrIdentityMismatch
	}
	return nil
}
func current(tx *bolt.Tx, group string) (FenceLookup, error) {
	data := tx.Bucket([]byte("fences")).Get([]byte(group))
	if data == nil {
		return FenceLookup{}, nil
	}
	var r FenceRecord
	err := decode(data, &r)
	return FenceLookup{Record: r, Found: err == nil}, err
}

// Current reads the retained fence for the named group without authorizing an
// effect.
func (s *Store) Current(ctx context.Context, group string) (FenceLookup, error) {
	var result FenceLookup
	err := s.readTransaction(ctx, func(tx *bolt.Tx) (err error) {
		result, err = current(tx, group)
		return err
	})
	return result, err
}

// Fence persists the new admission floor before callbacks can be canceled or
// joined. same is true only for a complete structural match of the old record.
func (s *Store) Fence(ctx context.Context, grant topology.AuthorityGrant) (result FenceResult, err error) {
	err = s.writeTransaction(ctx, func(tx *bolt.Tx) error {
		return s.fenceTx(tx, grant, &result)
	})
	return result, err
}

// CompleteFence records exact fence completion after older running effects have
// joined.
func (s *Store) CompleteFence(ctx context.Context, key topology.AuthorityKey,
	sequence uint64) (e topology.FenceEvidence, err error) {
	err = s.writeTransaction(ctx, func(tx *bolt.Tx) error {
		return s.completeFenceTx(tx, key, sequence, &e)
	})
	return e, err
}

func (s *Store) fenceTx(tx *bolt.Tx, grant topology.AuthorityGrant, result *FenceResult) error {
	if err := s.currentMemberGrant(grant); err != nil {
		return err
	}
	old, err := current(tx, grant.Key.GroupID)
	if err != nil {
		return err
	}
	if old.Found {
		if grant.Key.Revision < old.Record.Grant.Key.Revision {
			return fault.ErrAuthorityFenced
		}
		if grant.Key.Revision == old.Record.Grant.Key.Revision {
			if !grant.Equal(old.Record.Grant) {
				return fault.ErrIdentityMismatch
			}
			result.Record, result.Same = old.Record, true
			return nil
		}
	}
	if old.Record.Sequence == math.MaxUint64 {
		return fault.ErrUnavailable
	}
	result.Record = FenceRecord{Grant: grant.Clone(), Sequence: old.Record.Sequence + 1}
	return put(tx.Bucket([]byte("fences")), []byte(grant.Key.GroupID), result.Record)
}

func (s *Store) completeFenceTx(tx *bolt.Tx, key topology.AuthorityKey,
	sequence uint64, e *topology.FenceEvidence) error {
	r, err := current(tx, key.GroupID)
	if err != nil {
		return err
	}
	if !r.Found || r.Record.Grant.Key != key || r.Record.Sequence != sequence {
		return fault.ErrAuthorityFenced
	}
	if err := s.currentMemberGrant(r.Record.Grant); err != nil {
		return err
	}
	*e = topology.FenceEvidence{
		Key:           key,
		Resource:      resource(key.GroupID, s.identity.NodeName),
		Receiver:      s.identity,
		LocalSequence: sequence,
	}
	if err := e.Validate(r.Record.Grant); err != nil {
		return err
	}
	forEachErr := tx.Bucket([]byte("effects")).ForEach(func(_, data []byte) error {
		var effect EffectRecord
		if err := decode(data, &effect); err != nil {
			return err
		}
		if effect.State == effectRunning && effect.Grant.Key.GroupID == key.GroupID &&
			effect.Grant.Key.Revision < key.Revision {
			return fault.ErrAuthorityPending
		}
		return nil
	})
	if forEachErr != nil {
		return forEachErr
	}
	r.Record.Complete = true
	return put(tx.Bucket([]byte("fences")), []byte(key.GroupID), r.Record)
}
