package metastore

import (
	"github.com/asalimonov/gquorum/fault"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3/raftpb"
)

// MemberSeen is a local pin-scoped marker. It survives snapshot compaction and
// distinguishes a removed member from a joining replica that has never entered.
func (s *Store) MemberSeen() (bool, error) {
	var seen bool
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketIdentity).Get(keyMemberSeen)
		if raw == nil {
			return nil
		}
		if len(raw) != 1 || raw[0] != 1 {
			return fault.New(fault.Unreadable, "invalid local membership marker", nil)
		}
		seen = true
		return nil
	})
	return seen, err
}
func recordMemberSeen(tx *bolt.Tx, cs raftpb.ConfState) error {
	id, pinned := readIdentity(tx)
	if !pinned || id.MemberID == 0 {
		return nil
	}
	for _, members := range [][]uint64{cs.Voters, cs.Learners, cs.VotersOutgoing, cs.LearnersNext} {
		for _, member := range members {
			if member == id.MemberID {
				return tx.Bucket(bucketIdentity).Put(keyMemberSeen, []byte{1})
			}
		}
	}
	return nil
}
