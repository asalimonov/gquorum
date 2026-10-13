//go:build !functional

package raftengine

import (
	"testing"

	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3/raftpb"
)

func TestEntryFormatRejectsLegacyAndMisplacedConfiguration(t *testing.T) {
	t.Parallel()
	app, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.Application, Correlation: "c", RequestID: "r"})
	if err != nil {
		t.Fatal(err)
	}
	conf, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.StandaloneConfiguration, Correlation: "c"})
	if err != nil {
		t.Fatal(err)
	}
	configuration := func(context []byte) []byte {
		data, err := (&raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 2}}, Context: context}).Marshal()
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	for _, tc := range []struct {
		name  string
		typ   raftpb.EntryType
		data  []byte
		valid bool
	}{
		{"election no-op", raftpb.EntryNormal, nil, true},
		{"application", raftpb.EntryNormal, app, true},
		{"configuration", raftpb.EntryConfChangeV2, configuration(conf), true},
		{"legacy application", raftpb.EntryNormal, []byte(`{"schema":1,"correlation":"c","request_id":"r"}`), false},
		{"legacy configuration", raftpb.EntryConfChangeV2, configuration([]byte("c")), false},
		{"configuration in ordinary entry", raftpb.EntryNormal, conf, false},
		{"application in configuration", raftpb.EntryConfChangeV2, configuration(app), false},
		{"v1 configuration", raftpb.EntryConfChange, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := raftpb.Entry{Index: 1, Term: 1, Type: tc.typ, Data: tc.data}
			if err := validateEntryFormat(entry); (err == nil) != tc.valid {
				t.Fatalf("format: %v", err)
			}
			// Uncommitted tails must also be checked before application recovery.
			if err := validateRecovery(storage.Recovery{HardState: raftpb.HardState{Term: 1}, Entries: []raftpb.Entry{entry}}); (err == nil) != tc.valid {
				t.Fatalf("recovery: %v", err)
			}
		})
	}
}
