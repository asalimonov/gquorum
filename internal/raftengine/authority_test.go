//go:build !functional

package raftengine

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestAuthorityReplayAndAdministrativeTail(t *testing.T) {
	t.Parallel()
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "c", GroupID: "g", Revision: 10}, Purpose: topology.AuthorityFence, Resources: []topology.AuthorityResource{{Kind: topology.AuthorityGroup, GroupID: "g"}}, Targets: []topology.AuthorityTarget{{NodeName: "n", MemberID: 4, ReplicaID: 1}}}
	g := &Group{opts: Options{ManagedAuthority: true, GroupID: "g", ClusterID: "c"}, ms: raft.NewMemoryStorage(), changes: make(map[string]*request)}
	e := raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.InstallAuthority, Correlation: "install", Grant: &grant}
	if err := g.applyAuthority(raftpb.Entry{Index: 2, Term: 1}, e); err != nil {
		t.Fatal(err)
	}
	grant.Key.Revision--
	if g.authority.Grant.Key.Revision != 10 {
		t.Fatal("installed grant aliases input")
	}
	if err := g.applyAuthority(raftpb.Entry{Index: 3, Term: 1}, e); err != nil {
		t.Fatal(err)
	}
	if g.authority.InstallIndex != 2 {
		t.Fatal("stale replay moved fence")
	}
	grant.Key.Revision++
	grant.OperatorID = "altered"
	if err := g.applyAuthority(raftpb.Entry{Index: 4, Term: 1}, e); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal(err)
	}
	grant.OperatorID = ""
	data, err := raftstate.EncodeEntry(e)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.ms.Append([]raftpb.Entry{{Index: 1, Term: 1, Data: data}}); err != nil {
		t.Fatal(err)
	}
	if !g.pendingAdministrativeEntry() {
		t.Fatal("recovered administrative tail did not block admission")
	}
	g.applied = 1
	if g.pendingAdministrativeEntry() {
		t.Fatal("applied entry still blocks admission")
	}
}

func TestAuthorityRecoveryModeBoundaries(t *testing.T) {
	t.Parallel()
	o := Options{ManagedAuthority: true, GroupID: "g", ClusterID: "c", Mode: Founding, MemberID: 4, InstanceID: "i"}
	if err := validateAuthorityRecovery(o, storage.Recovery{}, raftstate.Snapshot{}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	r := storage.Recovery{Snapshot: raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1}}}
	if err := validateAuthorityRecovery(o, r, raftstate.Snapshot{}); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal(err)
	}
	o.GroupID = topology.MetaGroupID
	if err := validateAuthorityRecovery(o, r, raftstate.Snapshot{}); err != nil {
		t.Fatal("explicit meta genesis", err)
	}
	o.ManagedAuthority = false
	if err := validateAuthoritySnapshot(o, raftstate.Snapshot{Authority: raftstate.AuthorityState{Grant: &topology.AuthorityGrant{}}}); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
}

func TestManagedEngineRejectsForwardedRawTransfer(t *testing.T) {
	t.Parallel()
	g := &Group{opts: Options{ManagedAuthority: true, ReplicaID: 1}}
	if err := g.Step(context.Background(), raftpb.Message{Type: raftpb.MsgTransferLeader, From: 2, To: 1}); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
}
