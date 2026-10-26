package main

import (
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// This observes an accepted inbound snapshot, not its application. Tests must
// additionally observe the same process's applied index reaching this boundary.
func proxySnapshotIndex(frame []byte, identity topology.Identity) uint64 {
	target, err := proxyVarintField(frame, 2)
	if err != nil || target == 0 || target != identity.MemberID {
		return 0
	}
	header, err := proxyBytesField(frame, 1)
	if err != nil {
		return 0
	}
	cluster, err := proxyBytesField(header, 1)
	if err != nil || string(cluster) != identity.ClusterID {
		return 0
	}
	raw, err := proxyBytesField(frame, 3)
	if err != nil {
		return 0
	}
	var msg raftpb.Message
	if msg.Unmarshal(raw) != nil || msg.Type != raftpb.MsgSnap || msg.To != target || msg.Snapshot == nil {
		return 0
	}
	return msg.Snapshot.Metadata.Index
}
