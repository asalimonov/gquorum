//go:build !functional

package raftengine

import (
	"encoding/json"
	"errors"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
	"testing"
)

func TestForwardedEnvelopeCannotCarryMembershipOrControl(t *testing.T) {
	t.Parallel()
	data, _ := json.Marshal(envelope{Schema: 1, Correlation: "origin-1", RequestID: "request", Data: []byte("value")})
	good := raftpb.Message{Type: raftpb.MsgProp, From: 2, To: 1, Entries: []raftpb.Entry{{Type: raftpb.EntryNormal, Data: data}}}
	opts := Options{GroupID: "load", MaxUncommittedBytes: 1024}
	if err := validateForwardedProposal(opts, good); err != nil {
		t.Fatal(err)
	}
	for name, alter := range map[string]func(*raftpb.Message){
		"membership": func(m *raftpb.Message) { m.Entries[0].Type = raftpb.EntryConfChangeV2 },
		"multi":      func(m *raftpb.Message) { m.Entries = append(m.Entries, m.Entries[0]) },
		"empty":      func(m *raftpb.Message) { m.Entries = nil },
		"bad envelope": func(m *raftpb.Message) {
			m.Entries[0].Data = []byte(`{"schema_version":2,"correlation":"x","request_id":"y"}`)
		},
		"missing request": func(m *raftpb.Message) { m.Entries[0].Data = []byte(`{"schema_version":1,"correlation":"x"}`) },
		"commit":          func(m *raftpb.Message) { m.Commit = 99 },
		"responses":       func(m *raftpb.Message) { m.Responses = []raftpb.Message{{Type: raftpb.MsgHup}} },
		"snapshot":        func(m *raftpb.Message) { m.Snapshot = &raftpb.Snapshot{} },
		"term":            func(m *raftpb.Message) { m.Term = 8 },
		"entry index":     func(m *raftpb.Message) { m.Entries[0].Index = 9 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m, err := copyMessage(good)
			if err != nil {
				t.Fatal(err)
			}
			alter(&m)
			if err := validateForwardedProposal(opts, m); !errors.Is(err, fault.ErrInvalid) {
				t.Fatal(err)
			}
		})
	}
	metaOpts := opts
	metaOpts.GroupID = topology.MetaGroupID
	if err := validateForwardedProposal(metaOpts, good); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("raw meta proposal accepted", err)
	}
	smallOpts := opts
	smallOpts.MaxUncommittedBytes = 1
	if err := validateForwardedProposal(smallOpts, good); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal("unbounded proposal", err)
	}
}
