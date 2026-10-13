package raftengine

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"go.etcd.io/raft/v3/raftpb"
)

func decodeConfiguration(entry raftpb.Entry) (raftpb.ConfChangeV2, raftstate.Entry, error) {
	var cc raftpb.ConfChangeV2
	if err := cc.Unmarshal(entry.Data); err != nil {
		return cc, raftstate.Entry{}, fault.New(fault.Unreadable, "decode membership entry", err)
	}
	if cc.Transition != raftpb.ConfChangeTransitionAuto || len(cc.Changes) != 1 || cc.Changes[0].NodeID == 0 {
		return cc, raftstate.Entry{}, fault.ErrUnsafeMembership
	}
	if typ := cc.Changes[0].Type; typ != raftpb.ConfChangeAddNode && typ != raftpb.ConfChangeAddLearnerNode && typ != raftpb.ConfChangeRemoveNode {
		return cc, raftstate.Entry{}, fault.ErrUnsafeMembership
	}
	e, err := raftstate.DecodeEntry(cc.Context)
	if err != nil {
		return cc, e, err
	}
	if e.Kind != raftstate.ManagedConfiguration && e.Kind != raftstate.StandaloneConfiguration {
		return cc, e, fault.New(fault.Unreadable, "configuration has a non-configuration context", nil)
	}
	return cc, e, nil
}
func validateEntryFormat(entry raftpb.Entry) error {
	switch entry.Type {
	case raftpb.EntryNormal:
		if len(entry.Data) == 0 {
			return nil
		}
		e, err := raftstate.DecodeEntry(entry.Data)
		if err != nil {
			return err
		}
		if e.Kind == raftstate.ManagedConfiguration || e.Kind == raftstate.StandaloneConfiguration {
			return fault.New(fault.Unreadable, "configuration context in ordinary log entry", nil)
		}
		return nil
	case raftpb.EntryConfChangeV2:
		_, _, err := decodeConfiguration(entry)
		return err
	default:
		return fault.New(fault.Unsupported, "unsupported Raft entry type", nil)
	}
}
