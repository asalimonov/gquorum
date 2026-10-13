//go:build functional

package main

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestReadFaultsKeepConfirmationIndependentAndPreserveSnapshotFailure(t *testing.T) {
	t.Parallel()
	id := topology.Identity{ClusterID: "cluster", NodeName: "one", MemberID: 1, InstanceID: "process"}
	node := &nativeProtocolNode{id: id}
	h := newClusterHooks()
	key := walKey{"work", 7}
	h.reads[key] = &readControl{counters: readCounters{DropAppend: true}}
	a := &application{node: node, clusterHooks: h}
	batch := load.RaftBatch{To: load.Peer{Node: id, Replica: load.ReplicaRef{GroupID: "work", ReplicaID: 7}}, Messages: []raftpb.Message{{Type: raftpb.MsgApp}, {Type: raftpb.MsgReadIndexResp, Index: 13}}}
	filtered, err := a.filterReadRaft(batch)
	if err != nil || len(filtered.Messages) != 1 || filtered.Messages[0].Type != raftpb.MsgReadIndexResp || len(batch.Messages) != 2 {
		t.Fatal("append drop lost confirmation or mutated caller batch", filtered, err)
	}
	a.recordReadReceive(filtered)
	counters := h.readMetrics(key)
	if counters.AppendDropped != 1 || counters.ReadResponsesReceived != 1 || counters.ReadResponseIndex != 13 {
		t.Fatal(counters)
	}
	batch.Messages = []raftpb.Message{{Type: raftpb.MsgSnap}}
	if _, err := a.filterReadRaft(batch); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("dropped snapshot fabricated successful delivery", err)
	}
	batch.To.Replica.ReplicaID++
	if untouched, err := a.filterReadRaft(batch); err != nil || len(untouched.Messages) != 1 {
		t.Fatal("older control affected successor replica", untouched, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	machine := a.clusterMachine(newMachine(), walKey{"other", 8}).(*controlledMachine)
	control := h.reads[machine.key]
	control.applyRelease = make(chan struct{})
	control.counters.HoldApply = true
	cancel()
	if _, err := machine.Apply(ctx, load.Command{Index: 15}); !errors.Is(err, context.Canceled) {
		t.Fatal("owned Apply barrier ignored shutdown", err)
	}
	counters = h.readMetrics(machine.key)
	if counters.ApplyActive != 0 || counters.ApplyCompleted != 0 || counters.AppliedIndex != 0 {
		t.Fatal("canceled callback manufactured application progress", counters)
	}
	batch.From.Replica = load.ReplicaRef{GroupID: "work", ReplicaID: 7}
	first := []byte("round-one")
	batch.Messages = []raftpb.Message{{Type: raftpb.MsgReadIndex, Entries: []raftpb.Entry{{Data: first}}}}
	a.recordReadSend(batch)
	a.recordReadSend(batch)
	first[0] = 'X'
	if counters := h.readMetrics(key); counters.ReadRequestsSent != 2 || counters.ReadContextsSent != 1 || !bytes.Equal(h.reads[key].lastContext, []byte("round-one")) {
		t.Fatal("round retransmit grew contexts or retained caller memory", counters)
	}
}

func TestElectionFaultDropsOnlySelectedVoteRequestsAndOwnsObservations(t *testing.T) {
	t.Parallel()
	id := topology.Identity{ClusterID: "cluster", NodeName: "one", MemberID: 1, InstanceID: "process"}
	h := newClusterHooks()
	key := walKey{"work", 7}
	h.reads[key] = &readControl{counters: readCounters{DropVoteRequestsFrom: []uint64{2, 4}}}
	a := &application{node: &nativeProtocolNode{id: id}, clusterHooks: h}
	batch := load.RaftBatch{To: load.Peer{Node: id, Replica: load.ReplicaRef{GroupID: "work", ReplicaID: 7}}, Messages: []raftpb.Message{
		{Type: raftpb.MsgVote, From: 2}, {Type: raftpb.MsgPreVote, From: 4},
		{Type: raftpb.MsgVote, From: 3}, {Type: raftpb.MsgVoteResp, From: 2},
		{Type: raftpb.MsgPreVoteResp, From: 4}, {Type: raftpb.MsgApp, From: 2},
		{Type: raftpb.MsgHeartbeat, From: 4}, {Type: raftpb.MsgReadIndexResp, From: 2},
	}}
	filtered, err := a.filterReadRaft(batch)
	if err != nil || len(filtered.Messages) != 6 || len(batch.Messages) != 8 || filtered.Messages[0].From != 3 {
		t.Fatal("campaign fault suppressed votes, replication or read confirmation", filtered, err)
	}
	observed := h.readMetrics(key)
	if observed.VoteRequestsDropped != 2 || observed.RaftDropped != 2 {
		t.Fatal(observed)
	}
	observed.DropVoteRequestsFrom[0] = 99
	if h.readMetrics(key).DropVoteRequestsFrom[0] != 2 {
		t.Fatal("diagnostic observation mutated installed fault")
	}
}
