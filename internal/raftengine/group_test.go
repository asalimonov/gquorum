//go:build !functional

package raftengine

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRecoveryRejectsSnapshotStateWithoutIndex(t *testing.T) {
	t.Parallel()
	for _, ss := range []raftpb.Snapshot{{Data: []byte("state")}, {Metadata: raftpb.SnapshotMetadata{Term: 1}}, {Metadata: raftpb.SnapshotMetadata{ConfState: raftpb.ConfState{Voters: []uint64{1}}}}} {
		if err := validateRecovery(storage.Recovery{Snapshot: ss}); !errors.Is(err, fault.ErrUnreadable) {
			t.Fatal("orphaned snapshot state accepted", err)
		}
	}
}
