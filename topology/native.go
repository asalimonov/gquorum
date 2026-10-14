package topology

import (
	"slices"

	"go.etcd.io/raft/v3/raftpb"
)

// NativeState is the applied membership of one exact replica, captured on its
// Raft owner after quorum confirmation and local application. It is a point-in-
// time read, not authorization for a subsequent membership change.
type NativeState struct {
	GroupID   string           `json:"group_id"`
	ReplicaID uint64           `json:"replica_id"`
	Applied   uint64           `json:"applied"`
	ConfState raftpb.ConfState `json:"conf_state"`
	Grant     *AuthorityGrant  `json:"grant,omitempty"`
	Receipt   *EffectReceipt   `json:"receipt,omitempty"`
}

// Clone copies the confirmed native membership and its authority receipts.
func (s NativeState) Clone() NativeState {
	s.ConfState.Voters = slices.Clone(s.ConfState.Voters)
	s.ConfState.Learners = slices.Clone(s.ConfState.Learners)
	s.ConfState.VotersOutgoing = slices.Clone(s.ConfState.VotersOutgoing)
	s.ConfState.LearnersNext = slices.Clone(s.ConfState.LearnersNext)
	if s.Grant != nil {
		grant := s.Grant.Clone()
		s.Grant = &grant
	}
	if s.Receipt != nil {
		receipt := s.Receipt.Clone()
		s.Receipt = &receipt
	}
	return s
}
