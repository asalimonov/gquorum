package loadgrpc

import (
	legacyproto "github.com/golang/protobuf/proto"
	"go.etcd.io/raft/v3/raftpb"
)

func sameRaftMessages(a, b []raftpb.Message) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !legacyproto.Equal(&a[i], &b[i]) {
			return false
		}
	}
	return true
}
