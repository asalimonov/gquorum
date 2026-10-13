package raftstate

import (
	"github.com/asalimonov/gquorum/fault"
	"go.etcd.io/raft/v3/raftpb"
)

// DecodeRaftSnapshot validates engine authority against the enclosing Raft
// index and term, independently of the application's own snapshot encoding.
func DecodeRaftSnapshot(ss raftpb.Snapshot, cluster, group string) (Snapshot, error) {
	s, err := DecodeSnapshot(ss.Data, cluster, group)
	if err != nil {
		return Snapshot{}, err
	}
	if ss.Metadata.Index == 0 || ss.Metadata.Term == 0 {
		return Snapshot{}, fault.ErrUnreadable
	}
	if err := ValidateAuthority(s.Authority, ss.Metadata.Index); err != nil {
		return Snapshot{}, err
	}
	if s.Authority.InstallTerm > ss.Metadata.Term || s.Authority.DemotionTransferTerm > ss.Metadata.Term || s.Authority.Receipt != nil && s.Authority.Receipt.AppliedTerm > ss.Metadata.Term {
		return Snapshot{}, fault.New(fault.Unreadable, "authority exceeds snapshot term", nil)
	}
	return s, nil
}
