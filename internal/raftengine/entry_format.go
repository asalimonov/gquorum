package raftengine

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"go.etcd.io/raft/v3/raftpb"
)

type configurationEntry struct {
	Change  raftpb.ConfChangeV2
	Context raftstate.Entry
}

func decodeConfiguration(entry raftpb.Entry) (configurationEntry, error) {
	var cc raftpb.ConfChangeV2
	if err := cc.Unmarshal(entry.Data); err != nil {
		return configurationEntry{Change: cc}, fault.New(fault.Unreadable, "decode membership entry", err)
	}
	if cc.Transition != raftpb.ConfChangeTransitionAuto || len(cc.Changes) != 1 || cc.Changes[0].NodeID == 0 {
		return configurationEntry{Change: cc}, fault.ErrUnsafeMembership
	}
	if typ := cc.Changes[0].Type; typ != raftpb.ConfChangeAddNode && typ != raftpb.ConfChangeAddLearnerNode &&
		typ != raftpb.ConfChangeRemoveNode {
		return configurationEntry{Change: cc}, fault.ErrUnsafeMembership
	}
	e, err := raftstate.DecodeEntry(cc.Context)
	if err != nil {
		return configurationEntry{Change: cc, Context: e}, err
	}
	if e.Kind != raftstate.ManagedConfiguration && e.Kind != raftstate.StandaloneConfiguration {
		return configurationEntry{Change: cc, Context: e}, fault.New(fault.Unreadable,
			"configuration has a non-configuration context", nil)
	}
	return configurationEntry{Change: cc, Context: e}, nil
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
		_, err := decodeConfiguration(entry)
		return err
	default:
		return fault.New(fault.Unsupported, "unsupported Raft entry type", nil)
	}
}
