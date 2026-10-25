package raftengine

import (
	"math"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func validateSnapshot(ss raftpb.Snapshot) error {
	if ss.Metadata.Index == 0 || ss.Metadata.Term == 0 || len(ss.Data) == 0 ||
		len(ss.Metadata.ConfState.Voters) == 0 {
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
	if (len(r.Snapshot.Data) != 0 || r.Snapshot.Metadata.Term != 0 ||
		len(r.Snapshot.Metadata.ConfState.Voters) != 0 || len(r.Snapshot.Metadata.ConfState.Learners) != 0) &&
		raft.IsEmptySnap(r.Snapshot) {
		return fault.New(fault.Unreadable, "snapshot state has no recovery index", nil)
	}
	if !raft.IsEmptySnap(r.Snapshot) {
		if err := validateSnapshot(r.Snapshot); err != nil {
			return fault.New(fault.Unreadable, "invalid recovered snapshot", err)
		}
	}
	last := r.Snapshot.Metadata.Index
	previousTerm := r.Snapshot.Metadata.Term
	for _, e := range r.Entries {
		if last == math.MaxUint64 || e.Index != last+1 || e.Term == 0 || e.Term > r.HardState.Term ||
			e.Term < previousTerm {
			return fault.New(fault.Unreadable, "invalid recovered log prefix", nil)
		}
		if err := validateEntryFormat(e); err != nil {
			return err
		}
		last = e.Index
		previousTerm = e.Term
	}
	if r.HardState.Commit < r.Snapshot.Metadata.Index || r.HardState.Commit > last ||
		r.HardState.Term < r.Snapshot.Metadata.Term || r.AppliedHint > r.HardState.Commit {
		return fault.New(fault.Unreadable, "hard state outside recovery boundary", nil)
	}
	return nil
}
