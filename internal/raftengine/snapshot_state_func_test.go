//go:build functional

package raftengine

import (
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestCompleteAuthoritySnapshotSurvivesTailAndResnapshot(t *testing.T) {
	t.Parallel()
	s := openStore(t, t.TempDir(), "1")
	must(t, s.BindReplica(t.Context(), storage.Identity{NodeName: "1", ClusterID: "cluster", GroupID: "load", ReplicaID: 1}))
	must(t, s.Ensure(t.Context(), "load"))
	a := topology.AuthorizedStep{OperatorID: topology.MakeOperatorID(1, 1), Version: 1, Attempt: 1, StepSeq: 1, GroupID: "load", NodeName: "1", ReplicaID: 1, Kind: topology.StepTransferLeader}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "cluster", GroupID: "load", Revision: 100}, Purpose: topology.AuthorityEffect, OperatorID: a.OperatorID, Action: &a, Resources: []topology.AuthorityResource{{Kind: topology.AuthorityGroup, GroupID: "load"}}, Targets: []topology.AuthorityTarget{{NodeName: "1", MemberID: 1, ReplicaID: 1}}}
	receipt := topology.EffectReceipt{Key: grant.Key, Resource: grant.Resources[0], Action: &a, Target: grant.Targets[0], Outcome: topology.EffectTransferStarted, AppliedIndex: 5, AppliedTerm: 1}
	authority := raftstate.AuthorityState{Grant: &grant, InstallIndex: 4, InstallTerm: 1, Receipt: &receipt}
	data, err := raftstate.EncodeSnapshot(raftstate.Snapshot{Schema: raftstate.Schema, ClusterID: "cluster", GroupID: "load", Application: []byte(`{"commands":["snapshot"]}`), Authority: authority})
	must(t, err)
	ss := raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: 5, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}}}
	must(t, s.SaveSnapshot(t.Context(), "load", ss))
	tail, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.Application, Correlation: "tail", RequestID: "tail", Data: []byte("tail")})
	must(t, err)
	must(t, s.SaveReady(t.Context(), "load", storage.ReadyWrite{Entries: []raftpb.Entry{{Index: 6, Term: 1, Data: tail}}, HardState: raftpb.HardState{Term: 1, Commit: 6}}))
	n := &network{nodes: make(map[uint64]*Group), disconnected: make(map[uint64]bool)}
	m := &machine{}
	g := newManagedTestGroup(t, s, m, n, nil)
	if !reflect.DeepEqual(g.authority, authority) {
		t.Fatal("recovery dropped authority", g.authority)
	}
	must(t, g.Start(t.Context()))
	must(t, g.WaitReady(testContext(t)))
	eventually(t, g.IsLeader)
	if !reflect.DeepEqual(m.values(), []string{"snapshot", "tail"}) {
		t.Fatal(m.values())
	}
	propose(t, g, "fresh")
	must(t, g.Snapshot(testContext(t)))
	must(t, g.Stop(testContext(t)))
	recovery, err := s.Load(t.Context(), "load")
	must(t, err)
	decoded, err := raftstate.DecodeRaftSnapshot(recovery.Snapshot, "cluster", "load")
	must(t, err)
	if !reflect.DeepEqual(decoded.Authority, authority) {
		t.Fatal("compaction dropped authority", decoded.Authority)
	}
	m2 := &machine{}
	g2 := newManagedTestGroup(t, s, m2, n, nil)
	if !reflect.DeepEqual(g2.authority, authority) || !reflect.DeepEqual(m2.values(), []string{"snapshot", "tail", "fresh"}) {
		t.Fatal(g2.authority, m2.values())
	}
}
