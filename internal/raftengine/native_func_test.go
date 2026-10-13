//go:build functional

package raftengine

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestNativeReadWorksWithoutApplicationQueryAndOwnsResult(t *testing.T) {
	t.Parallel()
	g, _ := readFixture(t, &machine{}, ReadOptions{}, false)
	state, err := g.ReadNative(testContext(t))
	must(t, err)
	if state.GroupID != "load" || state.ReplicaID != 1 || state.Applied == 0 || !reflect.DeepEqual(state.ConfState.Voters, []uint64{1}) {
		t.Fatal(state)
	}
	state.ConfState.Voters[0] = 99
	next, err := g.ReadNative(testContext(t))
	must(t, err)
	if !reflect.DeepEqual(next.ConfState.Voters, []uint64{1}) {
		t.Fatal("caller mutated native membership", next)
	}
}

func TestNativeReadLearnerWaitsForQuorumAndAppliedMembership(t *testing.T) {
	t.Parallel()
	g, capture := readFixture(t, &machine{}, ReadOptions{}, true)
	type outcome struct {
		state topology.NativeState
		err   error
	}
	done := make(chan outcome, 1)
	go func() { state, err := g.ReadNative(testContext(t)); done <- outcome{state, err} }()
	round := capturedRead(t, capture)
	select {
	case got := <-done:
		t.Fatal("answered without quorum confirmation", got)
	default:
	}
	answerRead(t, g, round, 2)
	select {
	case got := <-done:
		t.Fatal("answered before local apply", got)
	default:
	}
	cc := raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 3}}}
	entry, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.StandaloneConfiguration, Correlation: "native-read-membership"})
	must(t, err)
	cc.Context = entry
	data, err := cc.Marshal()
	must(t, err)
	must(t, g.Step(testContext(t), raftpb.Message{Type: raftpb.MsgApp, From: 1, To: 2, Term: 2, Index: 1, LogTerm: 1, Commit: 2,
		Entries: []raftpb.Entry{{Type: raftpb.EntryConfChangeV2, Index: 2, Term: 2, Data: data}}}))
	select {
	case got := <-done:
		must(t, got.err)
		if got.state.Applied != 2 || got.state.ReplicaID != 2 || !reflect.DeepEqual(got.state.ConfState.Learners, []uint64{2, 3}) {
			t.Fatal("did not capture applied configuration", got)
		}
	case <-testContext(t).Done():
		t.Fatal("native read did not finish")
	}
	// An earlier confirmed read is not permission to reuse that membership.
	ctx, cancel := context.WithTimeout(testContext(t), 100*time.Millisecond)
	defer cancel()
	go func() { state, err := g.ReadNative(ctx); done <- outcome{state, err} }()
	next := capturedRead(t, capture)
	if string(next.Entries[0].Data) == string(round.Entries[0].Data) {
		t.Fatal("reused read round")
	}
	got := <-done
	if !errors.Is(got.err, context.DeadlineExceeded) || got.state.Applied != 0 {
		t.Fatal(got)
	}
}
