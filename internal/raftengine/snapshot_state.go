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
	old, next := g.authority, s.Authority
	if old.Grant != nil {
		if next.Grant == nil || next.Grant.Key.Revision < old.Grant.Key.Revision {
			return raftstate.Snapshot{}, fault.New(fault.Unreadable, "snapshot regresses installed authority", nil)
		}
		previousIndex, previousTerm := max(old.InstallIndex, old.DemotionTransferIndex), max(old.InstallTerm, old.DemotionTransferTerm)
		if old.Receipt != nil {
			previousIndex, previousTerm = max(previousIndex, old.Receipt.AppliedIndex), max(previousTerm, old.Receipt.AppliedTerm)
		}
		if next.Grant.Key.Revision > old.Grant.Key.Revision && (next.InstallIndex <= previousIndex || next.InstallTerm < previousTerm) {
			return raftstate.Snapshot{}, fault.New(fault.Unreadable, "newer authority regresses its installation boundary", nil)
		}
		if old.Grant.Key == next.Grant.Key && (!old.Grant.Equal(*next.Grant) || old.InstallIndex != next.InstallIndex || old.InstallTerm != next.InstallTerm || old.Receipt != nil && !reflect.DeepEqual(old.Receipt, next.Receipt)) {
			return raftstate.Snapshot{}, fault.New(fault.Unreadable, "snapshot contradicts installed authority", nil)
		}
		if old.Grant.Key == next.Grant.Key && old.DemotionTransferIndex != 0 && (old.DemotionTransferIndex != next.DemotionTransferIndex || old.DemotionTransferTerm != next.DemotionTransferTerm) {
			return raftstate.Snapshot{}, fault.New(fault.Unreadable, "snapshot contradicts demotion transfer intent", nil)
		}
	}
	return s, nil
}
func encodeSnapshot(opts Options, application []byte, authority raftstate.AuthorityState) ([]byte, error) {
	return raftstate.EncodeSnapshot(raftstate.Snapshot{Schema: raftstate.Schema, ClusterID: opts.ClusterID, GroupID: opts.GroupID, Application: application, Authority: authority})
}
