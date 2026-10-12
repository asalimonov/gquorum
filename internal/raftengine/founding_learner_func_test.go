//go:build functional

package raftengine

import (
	"slices"
	"testing"

	"go.etcd.io/raft/v3"
)

func TestFoundingLearnerSeedAndRetainedRecovery(t *testing.T) {
	t.Parallel()
	store := openStore(t, t.TempDir(), "learner")
	opts := Options{NodeName: "learner", ClusterID: "cluster", GroupID: "work", ReplicaID: 4,
		Mode: Founding, Peers: []raft.Peer{{ID: 3}, {ID: 1}, {ID: 2}}, FoundingLearners: []raft.Peer{{ID: 5}, {ID: 4}},
		Store: store, Machine: &machine{}, Transport: endpoint{network: &network{}}}
	g, err := New(opts)
	must(t, err)
	if cs := g.Status().ConfState; !slices.Equal(cs.Voters, []uint64{1, 2, 3}) || !slices.Equal(cs.Learners, []uint64{4, 5}) {
		t.Fatal("wrong founding seed", cs)
	}
	must(t, g.Stop(t.Context()))
	// Restart ignores caller founding choices and retains the durable role.
	opts.Mode, opts.Peers, opts.FoundingLearners, opts.Machine = Joining, nil, nil, &machine{}
	recovered, err := New(opts)
	must(t, err)
	defer func() { must(t, recovered.Stop(t.Context())) }()
	if cs := recovered.Status().ConfState; !slices.Equal(cs.Voters, []uint64{1, 2, 3}) || !slices.Equal(cs.Learners, []uint64{4, 5}) {
		t.Fatal("restart changed membership", cs)
	}
}
