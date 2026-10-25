//go:build !functional

package walbackend

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func TestStrictLogProtectsCommittedEntriesButAllowsExactRetriesAndTailReplacement(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"term", "type", "data", "exact", "tail"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			s := strictLog{collect: true, tailLimit: 4096}
			for index := uint64(1); index <= 4; index++ {
				entry := raftpb.Entry{Index: index, Term: 1, Data: []byte("original")}
				data, err := entry.Marshal()
				if err != nil {
					t.Fatal(err)
				}
				if err := s.entry(data, 4096); err != nil {
					t.Fatal(err)
				}
			}
			s.result.Recovery.HardState = raftpb.HardState{Term: 1, Commit: 3}
			entry := s.result.Recovery.Entries[1]
			switch change {
			case "term":
				entry.Term = 2
			case "type":
				entry.Type = raftpb.EntryConfChange
			case "data":
				entry.Data = []byte("conflicting")
			case "tail":
				entry.Index, entry.Term = 4, 2
			}
			data, err := entry.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			err = s.entry(data, 4096)
			if change != "exact" && change != "tail" {
				if err == nil {
					t.Fatal("CRC-valid committed rewrite accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			expected := 4
			if change == "exact" {
				expected = 3
			}
			if len(s.result.Recovery.Entries) != expected || s.result.LastLog.Index != uint64(expected) {
				t.Fatal("exact retry truncated retained suffix", s.result)
			}
			if change == "tail" && s.result.Recovery.Entries[3].Term != 2 {
				t.Fatal("legal uncommitted replacement refused")
			}
		})
	}
}
