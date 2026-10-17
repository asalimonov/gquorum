//go:build functional

package metaengine

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"go.etcd.io/raft/v3/raftpb"
)

func TestFrozenScopedStoreRestartsWithPinsMembershipAndCommittedTail(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "meta.bolt")
	if err := os.WriteFile(path, currentFormatBytes(t, "meta.bolt"), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := metastore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.VerifyIdentity(metastore.Identity{NodeName: "n", ClusterID: "c", MemberID: 1}); err != nil {
		t.Fatal("lost identity pin", err)
	}
	var saved scopedState
	if err := saved.UnmarshalJSON(currentFormatBytes(t, "snapshot.json")); err != nil {
		t.Fatal(err)
	}
	m, err := NewMachine(saved.Rules, store)
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.LoadAll()
	if err != nil || state.HardState != (raftpb.HardState{Term: 4, Vote: 1, Commit: 31}) || state.Snapshot.Metadata.Index != 30 || state.Snapshot.Metadata.Term != 4 || len(state.Entries) != 1 {
		t.Fatal("lost durable Raft boundary", state, err)
	}
	cs := raftpb.ConfState{Voters: []uint64{1}}
	membership, err := store.AppliedConfState()
	if err != nil || !membership.Found || !reflect.DeepEqual(membership.ConfState, cs) || !reflect.DeepEqual(state.Snapshot.Metadata.ConfState, cs) {
		t.Fatal("lost applied or snapshot membership", membership, err)
	}
	if index, err := store.AppliedIndex(); err != nil || index != 30 {
		t.Fatal("lost applied hint", index, err)
	}
	raw, err := store.LoadState()
	if err != nil || !bytes.Equal(raw, state.Snapshot.Data) {
		t.Fatal("materialized state differs from captured checkpoint", err)
	}
	if err := m.Restore(t.Context(), state.Snapshot.Data); err != nil {
		t.Fatal(err)
	}
	entry := state.Entries[0]
	if entry.Index != 31 || entry.Term != 4 {
		t.Fatal("lost committed tail", entry)
	}
	result, err := m.Apply(t.Context(), raftengine.Command{Index: entry.Index, Term: entry.Term, Data: entry.Data, ConfState: cs})
	if err != nil || result.Code != "" {
		t.Fatal("tail replay failed", result, err)
	}
	if err := m.AfterApply(t.Context(), 31, 4, cs); err != nil {
		t.Fatal(err)
	}
	if session, found := m.Session("tail-session"); !found || session.InstanceID != "tail-instance" {
		t.Fatal("committed tail was skipped", session)
	}
	if override, err := store.LoadOverride(); err != nil || override != 2 {
		t.Fatal("lost unrelated serve override", override, err)
	}
	if replica, err := store.LookupReplica("live"); err != nil || !replica.Found || replica.Record.NodeName != "n" || replica.Record.ReplicaID != 1 {
		t.Fatal("lost unrelated load ledger", replica, err)
	}
	if seen, err := store.MemberSeen(); err != nil || !seen {
		t.Fatal("lost durable native membership evidence", seen, err)
	}
}
