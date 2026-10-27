//go:build !functional

package main

import (
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
	"testing"
)

func TestMetaSnapshotObservationBindsClusterMemberAndMessage(t *testing.T) {
	t.Parallel()
	id := topology.Identity{ClusterID: "cluster", MemberID: 2, NodeName: "two", InstanceID: "run"}
	msg := raftpb.Message{Type: raftpb.MsgSnap, To: 2, Snapshot: &raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 99, Term: 1}}}
	raw, err := msg.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	bytesField := func(field protowire.Number, data []byte) []byte {
		return protowire.AppendBytes(protowire.AppendTag(nil, field, protowire.BytesType), data)
	}
	header := bytesField(1, []byte("cluster"))
	frame := bytesField(1, header)
	frame = protowire.AppendVarint(protowire.AppendTag(frame, 2, protowire.VarintType), 2)
	frame = append(frame, bytesField(3, raw)...)
	if got := proxySnapshotIndex(frame, id); got != 99 {
		t.Fatal(got)
	}
	for _, foreign := range []topology.Identity{{ClusterID: "foreign", MemberID: 2}, {ClusterID: "cluster", MemberID: 3}} {
		if proxySnapshotIndex(frame, foreign) != 0 {
			t.Fatal("foreign snapshot counted")
		}
	}
	if proxySnapshotIndex([]byte{255}, id) != 0 {
		t.Fatal("invalid snapshot counted")
	}
}
