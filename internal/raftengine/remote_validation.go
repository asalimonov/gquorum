package raftengine

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"go.etcd.io/raft/v3/raftpb"
)

func (g *Group) validateRemotePayload(msg raftpb.Message) error {
	if msg.Type == raftpb.MsgReadIndex || msg.Type == raftpb.MsgReadIndexResp {
		if _, ok := readMessageContext(msg); !ok {
			return fault.ErrInvalid
		}
	}
	if msg.Type == raftpb.MsgProp {
		if err := validateForwardedProposal(g.opts, msg); err != nil {
			return err
		}
	}
	if err := g.validateRemoteSnapshot(msg); err != nil {
		return err
	}
	return g.validateRemoteAppend(msg)
}

func (g *Group) validateRemoteSnapshot(msg raftpb.Message) error {
	if msg.Type == raftpb.MsgSnap {
		if msg.Snapshot == nil {
			return fault.New(fault.Invalid, "snapshot message has no state", nil)
		}
		if err := validateSnapshot(*msg.Snapshot); err != nil {
			return err
		}
		state, err := raftstate.DecodeRaftSnapshot(*msg.Snapshot, g.opts.ClusterID, g.opts.GroupID)
		if err != nil {
			return err
		}
		if err := validateAuthoritySnapshot(g.opts, state); err != nil {
			return err
		}
	}
	return nil
}

func (g *Group) validateRemoteAppend(msg raftpb.Message) error {
	if msg.Type == raftpb.MsgApp {
		for i, e := range msg.Entries {
			if e.Term == 0 || e.Term > msg.Term || e.Type != raftpb.EntryNormal && e.Type != raftpb.EntryConfChangeV2 ||
				e.Index != msg.Index+uint64(i)+1 {
				return fault.New(fault.Invalid, "invalid append message entries", nil)
			}
			if err := validateEntryMode(g.opts, e); err != nil {
				return err
			}
		}
	}
	return nil
}
