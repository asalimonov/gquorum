//go:build functional

package raftengine

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type offlineMachine struct {
	restored []byte
	commands []Command
	observed uint64
	fail     error
}

func (m *offlineMachine) Restore(_ context.Context, data []byte) error {
	m.restored = bytes.Clone(data)
	return m.fail
}
func (m *offlineMachine) Apply(_ context.Context, c Command) (Result, error) {
	m.commands = append(m.commands, c)
	return Result{Code: "business-rejection"}, m.fail
}
func (*offlineMachine) Snapshot(context.Context) ([]byte, error) {
	return nil, errors.New("offline replay must never snapshot an application")
}
func (m *offlineMachine) AfterApply(_ context.Context, index, _ uint64, _ raftpb.ConfState) error {
	m.observed = index
	return nil
}

func offlineReplayFixture(t *testing.T) (ReplayOptions, storage.Recovery) {
	t.Helper()
	opts := ReplayOptions{Identity: storage.Identity{NodeName: "n1", ClusterID: "cluster", GroupID: topology.MetaGroupID, ReplicaID: 1}, MemberID: 1, InstanceID: "offline"}
	data, err := raftstate.EncodeSnapshot(raftstate.Snapshot{Schema: raftstate.Schema, ClusterID: "cluster", GroupID: topology.MetaGroupID, Application: []byte("snapshot")})
	if err != nil {
		t.Fatal(err)
	}
	retained := storage.Recovery{Snapshot: raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}}}, HardState: raftpb.HardState{Term: 1, Commit: 3}, AppliedHint: 1}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "cluster", GroupID: topology.MetaGroupID, Revision: 10}, Purpose: topology.AuthorityFence, Resources: []topology.AuthorityResource{{Kind: topology.AuthorityGroup, GroupID: topology.MetaGroupID}}, Targets: []topology.AuthorityTarget{{NodeName: "n1", MemberID: 1, ReplicaID: 1}}}
	authority, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.InstallAuthority, Correlation: "fence", Grant: &grant})
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.Application, Correlation: "command", RequestID: "stable-request", Data: []byte("committed")})
	if err != nil {
		t.Fatal(err)
	}
	tail, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.Application, Correlation: "uncommitted", RequestID: "never-committed", Data: []byte("tail")})
	if err != nil {
		t.Fatal(err)
	}
	retained.Entries = []raftpb.Entry{{Index: 2, Term: 1, Data: authority}, {Index: 3, Term: 1, Data: command}, {Index: 4, Term: 1, Data: tail}}
	return opts, retained
}
func TestOfflineReplayUsesLiveAuthorityInterpreterAndCommitOnly(t *testing.T) {
	t.Parallel()
	opts, retained := offlineReplayFixture(t)
	m := &offlineMachine{}
	original := bytes.Clone(retained.Snapshot.Data)
	result, err := Replay(t.Context(), opts, retained, m)
	if err != nil {
		t.Fatal(err)
	}
	if result.Index != 3 || result.Term != 1 || !reflect.DeepEqual(result.ConfState.Voters, []uint64{1}) || m.observed != 3 || string(m.restored) != "snapshot" || len(m.commands) != 1 {
		t.Fatal(result, m)
	}
	command := m.commands[0]
	if command.Index != 3 || command.Term != 1 || command.RequestID != "stable-request" || string(command.Data) != "committed" {
		t.Fatal(command)
	}
	if result.Authority.Grant == nil || result.Authority.Grant.Key.Revision != 10 || result.Authority.InstallIndex != 2 {
		t.Fatal("native authority entries were skipped", result.Authority)
	}
	if !bytes.Equal(original, retained.Snapshot.Data) {
		t.Fatal("input snapshot changed")
	}
	m.commands[0].Data[0] = 'X'
	_, entryErr := raftstate.DecodeEntry(retained.Entries[1].Data)
	if entryErr != nil {
		t.Fatal(entryErr)
	}
}
func TestOfflineReplayRejectsContradictoryBoundaryAndJoinsCancellation(t *testing.T) {
	t.Parallel()
	opts, retained := offlineReplayFixture(t)
	bad := retained
	bad.AppliedHint = 4
	if _, err := Replay(t.Context(), opts, bad, &offlineMachine{}); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("hint raised commit", err)
	}
	bad = retained
	bad.Entries = append([]raftpb.Entry(nil), retained.Entries...)
	bad.Entries[1].Term = 0
	if _, err := Replay(t.Context(), opts, bad, &offlineMachine{}); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	m := &offlineMachine{}
	if _, err := Replay(ctx, opts, retained, m); !errors.Is(err, context.Canceled) || m.restored != nil {
		t.Fatal("cancelled replay touched machine", err)
	}
	want := errors.New("restore failure")
	m = &offlineMachine{fail: want}
	if _, err := Replay(t.Context(), opts, retained, m); !errors.Is(err, want) {
		t.Fatal("restore error lost", err)
	}
}

func TestOfflineDiscardMachineRetainsNativeAuthorityWithoutApplication(t *testing.T) {
	t.Parallel()
	options, retained := offlineReplayFixture(t)
	result, err := Replay(t.Context(), options, retained, DiscardMachine{})
	if err != nil || result.Index != 3 || result.Authority.Grant == nil || result.Authority.Grant.Key.Revision != 10 {
		t.Fatal(result, err)
	}
	if _, err := (DiscardMachine{}).Snapshot(t.Context()); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("inspection manufactured an application snapshot", err)
	}
}
