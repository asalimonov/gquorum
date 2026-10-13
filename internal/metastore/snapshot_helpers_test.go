//go:build functional

package metastore

import (
	"testing"

	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3/raftpb"
)

func completeSnapshot(t *testing.T, s *Store, ss raftpb.Snapshot) raftpb.Snapshot {
	t.Helper()
	var id Identity
	var found bool
	if err := s.db.View(func(tx *bolt.Tx) error { id, found = readIdentity(tx); return nil }); err != nil {
		t.Fatal(err)
	}
	if !found {
		id = Identity{NodeName: "node", ClusterID: "test-cluster", MemberID: 1}
		if err := s.EnsureIdentity(id.NodeName, id.ClusterID, id.MemberID); err != nil {
			t.Fatal(err)
		}
	}
	if len(ss.Data) == 0 {
		ss.Data = []byte("snapshot")
	}
	data, err := raftstate.EncodeSnapshot(raftstate.Snapshot{Schema: raftstate.Schema, ClusterID: id.ClusterID, GroupID: topology.MetaGroupID, Application: ss.Data})
	if err != nil {
		t.Fatal(err)
	}
	ss.Data = data
	return ss
}
