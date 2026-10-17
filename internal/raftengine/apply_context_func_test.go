//go:build functional

package raftengine

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type membershipMachine struct {
	machine
	applied chan Command
}

func (m *membershipMachine) Apply(_ context.Context, c Command) (Result, error) {
	owned := c
	owned.ConfState = copyConf(c.ConfState)
	// Machine ownership must not let application code corrupt the engine's
	// native membership or the evidence supplied to the next command.
	if len(c.ConfState.Voters) > 0 {
		c.ConfState.Voters[0] = 99
	}
	m.applied <- owned
	return Result{}, nil
}

func TestApplyContextIncludesEarlierConfigurationInSameBatch(t *testing.T) {
	t.Parallel()
	m := &membershipMachine{applied: make(chan Command, 2)}
	store, err := metastore.Open(filepath.Join(t.TempDir(), "meta.bolt"))
	must(t, err)
	t.Cleanup(func() { must(t, store.Close()) })
	g, err := NewMeta(Options{GroupID: topology.MetaGroupID, NodeName: "2", ClusterID: "cluster", ReplicaID: 2,
		Mode: Founding, Peers: []raft.Peer{{ID: 1}}, FoundingLearners: []raft.Peer{{ID: 2}},
		Store: &metastore.Adapter{Store: store}, Machine: m, Transport: readCapture{requests: make(chan raftpb.Message, 16)}})
	must(t, err)
	t.Cleanup(func() { must(t, g.Stop(testContext(t))) })
	must(t, g.Start(t.Context()))
	must(t, g.WaitReady(testContext(t)))
	setForwardingLeader(t, g)
	configuration, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema,
		Kind: raftstate.StandaloneConfiguration, Correlation: "add-learner"})
	must(t, err)
	cc := raftpb.ConfChangeV2{Context: configuration,
		Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 3}}}
	encoded, err := cc.Marshal()
	must(t, err)
	command, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema,
		Kind: raftstate.Application, RequestID: "request", Correlation: "application", Data: []byte("proof")})
	must(t, err)
	// One follower protocol double supplies a committed batch. This is not a
	// multi-node fixture and does not depend on two Ready batches being scheduled.
	must(t, g.Step(testContext(t), raftpb.Message{Type: raftpb.MsgApp, From: 1, To: 2,
		Term: 2, Index: 1, LogTerm: 1, Commit: 4, Entries: []raftpb.Entry{
			{Type: raftpb.EntryConfChangeV2, Index: 2, Term: 2, Data: encoded},
			{Type: raftpb.EntryNormal, Index: 3, Term: 2, Data: command},
			{Type: raftpb.EntryNormal, Index: 4, Term: 2, Data: command},
		}}))
	for _, index := range []uint64{3, 4} {
		select {
		case applied := <-m.applied:
			if applied.Index != index || applied.Term != 2 || !slices.Equal(applied.ConfState.Voters, []uint64{1}) ||
				!slices.Equal(applied.ConfState.Learners, []uint64{2, 3}) {
				t.Fatal("application saw stale or borrowed native membership", applied)
			}
		case <-testContext(t).Done():
			t.Fatal("application did not apply")
		}
	}
}

func TestLoadApplyDoesNotCopyMetadataContext(t *testing.T) {
	t.Parallel()
	m := &membershipMachine{applied: make(chan Command, 1)}
	g, _ := readFixture(t, m, ReadOptions{}, false)
	_, err := g.Propose(testContext(t), Proposal{RequestID: "plain", Data: []byte("data")})
	must(t, err)
	applied := <-m.applied
	if len(applied.ConfState.Voters) != 0 || len(applied.ConfState.Learners) != 0 {
		t.Fatal("load command copied metadata context", applied)
	}
}
