package load

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func validateSnapshot(ss raftpb.Snapshot) error {
	if ss.Metadata.Index == 0 || ss.Metadata.Term == 0 || len(ss.Data) == 0 || len(ss.Metadata.ConfState.Voters) == 0 {
		return fault.New(fault.Invalid, "snapshot requires complete state and voters", nil)
	}
	cs := ss.Metadata.ConfState
	if cs.AutoLeave || len(cs.VotersOutgoing) > 0 || len(cs.LearnersNext) > 0 {
		return fault.New(fault.Unsupported, "joint membership is unsupported", nil)
	}
	seen := make(map[uint64]bool)
	for _, ids := range [][]uint64{cs.Voters, cs.Learners} {
		for _, id := range ids {
			if id == 0 || seen[id] {
				return fault.New(fault.Invalid, "zero or duplicate snapshot member", nil)
			}
			seen[id] = true
		}
	}
	return nil
}
func validateRecovery(r storage.Recovery) error {
	if !raft.IsEmptySnap(r.Snapshot) {
		if err := validateSnapshot(r.Snapshot); err != nil {
			return fault.New(fault.Unreadable, "invalid recovered snapshot", err)
		}
	}
	last := r.Snapshot.Metadata.Index
	for _, e := range r.Entries {
		if e.Index != last+1 || e.Term == 0 || e.Term > r.HardState.Term {
			return fault.New(fault.Unreadable, "invalid recovered log prefix", nil)
		}
		last = e.Index
	}
	if r.HardState.Commit < r.Snapshot.Metadata.Index || r.HardState.Commit > last || r.HardState.Term < r.Snapshot.Metadata.Term || r.AppliedHint > r.HardState.Commit {
		return fault.New(fault.Unreadable, "hard state outside recovery boundary", nil)
	}
	return nil
}
