//go:build !functional

package recoverymodel

import (
	"testing"

	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3/raftpb"
)

func TestCommittedPrefixPreservesSourceAndNeverInfersCommitOrTerm(t *testing.T) {
	t.Parallel()
	source := storage.Recovery{
		Snapshot:    raftpb.Snapshot{Data: []byte("snapshot"), Metadata: raftpb.SnapshotMetadata{Index: 2, Term: 1}},
		HardState:   raftpb.HardState{Term: 1, Vote: 7, Commit: 3},
		Entries:     []raftpb.Entry{{Index: 3, Term: 1}, {Index: 4, Term: 2}},
		AppliedHint: 3,
	}
	prefix, err := CommittedPrefix(source)
	if err != nil || len(prefix.Entries) != 1 || prefix.Entries[0].Index != 3 || prefix.HardState != source.HardState {
		t.Fatal(prefix, err)
	}
	if len(source.Entries) != 2 || source.Entries[1].Term != 2 {
		t.Fatal("source evidence changed")
	}
	source.HardState.Commit = 2
	prefix, err = CommittedPrefix(source)
	if err != nil || len(prefix.Entries) != 0 {
		t.Fatal(prefix, err)
	}
	for _, invalid := range []uint64{1, 5} {
		source.HardState.Commit = invalid
		if _, err := CommittedPrefix(source); err == nil {
			t.Fatal("commit outside retained source accepted")
		}
	}
}
