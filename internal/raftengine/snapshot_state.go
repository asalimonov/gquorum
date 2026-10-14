package raftengine

import (
	"reflect"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"go.etcd.io/raft/v3/raftpb"
)

func (g *Group) decodeSnapshot(ss raftpb.Snapshot) (raftstate.Snapshot, error) {
	s, err := raftstate.DecodeRaftSnapshot(ss, g.opts.ClusterID, g.opts.GroupID)
	if err != nil {
		return raftstate.Snapshot{}, err
	}
	if err := validateAuthoritySnapshot(g.opts, s); err != nil {
		return raftstate.Snapshot{}, err
	}
	if err := validateSnapshotAuthorityProgress(g.authority, s.Authority); err != nil {
		return raftstate.Snapshot{}, err
	}
	return s, nil
}

func validateSnapshotAuthorityProgress(old, next raftstate.AuthorityState) error {
	if old.Grant == nil {
		return nil
	}
	if next.Grant == nil || next.Grant.Key.Revision < old.Grant.Key.Revision {
		return fault.New(fault.Unreadable, "snapshot regresses installed authority", nil)
	}
	previousIndex, previousTerm := max(old.InstallIndex, old.DemotionTransferIndex),
		max(old.InstallTerm, old.DemotionTransferTerm)
	if old.Receipt != nil {
		previousIndex, previousTerm = max(previousIndex, old.Receipt.AppliedIndex),
			max(previousTerm, old.Receipt.AppliedTerm)
	}
	if next.Grant.Key.Revision > old.Grant.Key.Revision && (next.InstallIndex <= previousIndex ||
		next.InstallTerm < previousTerm) {
		return fault.New(fault.Unreadable,
			"newer authority regresses its installation boundary", nil)
	}
	if old.Grant.Key == next.Grant.Key && (old.InstallIndex != next.InstallIndex ||
		old.InstallTerm != next.InstallTerm ||
		!old.Grant.Equal(*next.Grant) || old.Receipt != nil && !reflect.DeepEqual(old.Receipt, next.Receipt)) {
		return fault.New(fault.Unreadable, "snapshot contradicts installed authority", nil)
	}
	if old.Grant.Key == next.Grant.Key &&
		old.DemotionTransferIndex != 0 && (old.DemotionTransferIndex != next.DemotionTransferIndex ||
		old.DemotionTransferTerm != next.DemotionTransferTerm) {
		return fault.New(fault.Unreadable, "snapshot contradicts demotion transfer intent",
			nil)
	}
	return nil
}

func encodeSnapshot(opts Options, application []byte, authority raftstate.AuthorityState) ([]byte, error) {
	return raftstate.EncodeSnapshot(raftstate.Snapshot{
		Schema:      raftstate.Schema,
		ClusterID:   opts.ClusterID,
		GroupID:     opts.GroupID,
		Application: application,
		Authority:   authority,
	})
}
