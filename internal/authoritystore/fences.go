package authoritystore

import (
	"context"
	"math"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
)

type FenceRecord struct {
	Grant    topology.AuthorityGrant `json:"grant"`
	Sequence uint64                  `json:"sequence"`
	Complete bool                    `json:"complete"`
}

func resource(group, node string) topology.AuthorityResource {
	return topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: group, NodeName: node}
}
func (s *Store) hostGrant(g topology.AuthorityGrant, currentMember bool) error {
	if err := g.Validate(); err != nil {
		return err
	}
	if g.Key.ClusterID != s.identity.ClusterID || !slices.Contains(g.Resources, resource(g.Key.GroupID, s.identity.NodeName)) ||
		!slices.ContainsFunc(g.Targets, func(t topology.AuthorityTarget) bool {
			return t.NodeName == s.identity.NodeName && (!currentMember || t.MemberID == s.identity.MemberID)
		}) {
		return fault.ErrIdentityMismatch
	}
	return nil
}
func current(tx *bolt.Tx, group string) (FenceRecord, bool, error) {
	data := tx.Bucket([]byte("fences")).Get([]byte(group))
	if data == nil {
		return FenceRecord{}, false, nil
	}
	var r FenceRecord
	err := decode(data, &r)
	return r, err == nil, err
}
func (s *Store) Current(ctx context.Context, group string) (FenceRecord, bool, error) {
	var r FenceRecord
	var found bool
	err := s.transaction(ctx, false, func(tx *bolt.Tx) (err error) { r, found, err = current(tx, group); return err })
	return r, found, err
}

// Fence persists the new admission floor before callbacks can be canceled or
// joined. same is true only for a complete structural match of the old record.
func (s *Store) Fence(ctx context.Context, grant topology.AuthorityGrant) (r FenceRecord, same bool, err error) {
	err = s.transaction(ctx, true, func(tx *bolt.Tx) error {
		if err := s.hostGrant(grant, true); err != nil {
			return err
		}
		old, exists, err := current(tx, grant.Key.GroupID)
		if err != nil {
			return err
		}
		if exists {
			if grant.Key.Revision < old.Grant.Key.Revision {
				return fault.ErrAuthorityFenced
			}
			if grant.Key.Revision == old.Grant.Key.Revision {
				if !grant.Equal(old.Grant) {
					return fault.ErrIdentityMismatch
				}
				r, same = old, true
				return nil
			}
		}
		if old.Sequence == math.MaxUint64 {
			return fault.ErrUnavailable
		}
		r = FenceRecord{Grant: grant.Clone(), Sequence: old.Sequence + 1}
		return put(tx.Bucket([]byte("fences")), []byte(grant.Key.GroupID), r)
	})
	return
}
func (s *Store) CompleteFence(ctx context.Context, key topology.AuthorityKey, sequence uint64) (e topology.FenceEvidence, err error) {
	err = s.transaction(ctx, true, func(tx *bolt.Tx) error {
		r, exists, err := current(tx, key.GroupID)
		if err != nil {
			return err
		}
		if !exists || r.Grant.Key != key || r.Sequence != sequence {
			return fault.ErrAuthorityFenced
		}
		if err := s.hostGrant(r.Grant, true); err != nil {
			return err
		}
		e = topology.FenceEvidence{Key: key, Resource: resource(key.GroupID, s.identity.NodeName), Receiver: s.identity, LocalSequence: sequence}
		if err := e.Validate(r.Grant); err != nil {
			return err
		}
		if err := tx.Bucket([]byte("effects")).ForEach(func(_, data []byte) error {
			var effect EffectRecord
			if err := decode(data, &effect); err != nil {
				return err
			}
			if effect.State == "running" && effect.Grant.Key.GroupID == key.GroupID && effect.Grant.Key.Revision < key.Revision {
				return fault.ErrAuthorityPending
			}
			return nil
		}); err != nil {
			return err
		}
		r.Complete = true
		return put(tx.Bucket([]byte("fences")), []byte(key.GroupID), r)
	})
	return
}
